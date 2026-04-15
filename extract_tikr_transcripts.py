#!/usr/bin/env python3
"""
Extract earnings-call transcripts from TIKR Terminal for a given ticker.

Uses the same persistent Playwright session as extract_tikr_estimates.py.
Run that script with --login first if you haven't authenticated yet.

Usage:
    # Supply any TIKR URL for the company (cid/tid are extracted)
    python extract_tikr_transcripts.py CROX --url "https://app.tikr.com/stock/estimates?cid=13639284&tid=25835213&ref=cdg2dy&tab=est"

    # Search by ticker (auto-navigates to Transcripts tab)
    python extract_tikr_transcripts.py CROX

    # Limit to N most recent accessible transcripts (default: all)
    python extract_tikr_transcripts.py CROX --limit 4

    # Earnings calls only (skip conference presentations etc.)
    python extract_tikr_transcripts.py CROX --earnings-only

    # Custom output directory
    python extract_tikr_transcripts.py CROX -o /path/to/output

    # Visible browser for debugging
    python extract_tikr_transcripts.py CROX --visible

Output:
    <output_dir>/<TICKER>/transcripts/<TICKER>_<date>_<title>.txt  — one file per transcript
    <output_dir>/<TICKER>/transcripts/<TICKER>_transcripts_index.csv — index of downloaded calls
"""

import argparse
import csv
import re
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DEFAULT_RESEARCH_DIR

DEFAULT_OUTPUT_DIR = DEFAULT_RESEARCH_DIR
BROWSER_PROFILE_DIR = Path.home() / ".tikr-playwright-session"
TIKR_BASE_URL = "https://app.tikr.com"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def cleanup_browser_locks() -> None:
    for lock_file in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
        (BROWSER_PROFILE_DIR / lock_file).unlink(missing_ok=True)


def safe_filename(s: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "_", s).strip()


def build_transcripts_index_url(base_url: str) -> str:
    """
    Given any TIKR stock URL (estimates, financials, etc.), return
    the /stock/transcripts index URL with the same cid/tid.
    """
    cid = re.search(r"cid=(\d+)", base_url)
    tid = re.search(r"tid=(\d+)", base_url)
    ref = re.search(r"ref=([^&]+)", base_url)
    if not cid or not tid:
        return base_url
    url = f"{TIKR_BASE_URL}/stock/transcripts?cid={cid.group(1)}&tid={tid.group(1)}"
    if ref:
        url += f"&ref={ref.group(1)}"
    return url


# ---------------------------------------------------------------------------
# Navigation
# ---------------------------------------------------------------------------

def dismiss_overlay(page) -> None:
    try:
        overlay = page.query_selector(".v-overlay--active")
        if overlay and overlay.is_visible():
            page.keyboard.press("Escape")
            time.sleep(0.5)
            scrim = page.query_selector(".v-overlay__scrim")
            if scrim and scrim.is_visible():
                scrim.click(force=True)
                time.sleep(0.5)
    except Exception:
        pass


def navigate_via_search(page, ticker: str, exchange: str | None = None) -> str | None:
    """
    Search for ticker on TIKR, click the first/best result, and return the
    transcripts index URL.  Returns None on failure.
    """
    page.goto(TIKR_BASE_URL, wait_until="domcontentloaded")
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except PlaywrightTimeout:
        pass

    if "/login" in page.url or "/signin" in page.url:
        print("Error: Not logged in. Run extract_tikr_estimates.py --login first.",
              file=sys.stderr)
        return None

    dismiss_overlay(page)

    search_container = None
    for sel in (".tickers--search", ".v-autocomplete"):
        try:
            search_container = page.wait_for_selector(sel, timeout=5000)
            if search_container and search_container.is_visible():
                break
            search_container = None
        except PlaywrightTimeout:
            continue

    if not search_container:
        print("Error: Could not find search input on TIKR.", file=sys.stderr)
        return None

    search_container.click(force=True)
    time.sleep(0.5)
    page.keyboard.type(ticker, delay=80)
    time.sleep(3)

    clicked = False
    item_href = None
    items = page.query_selector_all(".menuable__content__active .v-list-item")
    exchange_upper = exchange.upper() if exchange else ""
    first_visible = None

    for item in items:
        try:
            if not item.is_visible():
                continue
            href = item.get_attribute("href") or ""
            if first_visible is None:
                first_visible = item
                item_href = href
            if exchange_upper and exchange_upper in (item.inner_text() or "").upper():
                item.click()
                item_href = href
                clicked = True
                break
        except Exception:
            continue

    if not clicked and first_visible:
        try:
            first_visible.click()
            clicked = True
        except Exception:
            pass

    if not clicked:
        print(f"Error: No search results found for {ticker} on TIKR.", file=sys.stderr)
        return None

    if item_href:
        return build_transcripts_index_url(item_href)

    # No href on the clicked item — wait for SPA to load, then find cid/tid from any DOM link
    time.sleep(3)
    current_url = page.url
    if "tikr.com" in current_url and ("cid=" in current_url or "tid=" in current_url):
        return build_transcripts_index_url(current_url)

    # Scan DOM links for any href containing cid= and tid=
    found_href = page.evaluate("""() => {
        const links = document.querySelectorAll('a[href*="cid="]');
        for (const a of links) {
            const h = a.getAttribute('href') || '';
            if (h.includes('cid=') && h.includes('tid=')) return h;
        }
        return null;
    }""")
    if found_href:
        return build_transcripts_index_url(found_href)

    print(f"Error: Could not determine transcripts URL for {ticker} after search.", file=sys.stderr)
    return None


# ---------------------------------------------------------------------------
# Index page — collect transcript URLs
# ---------------------------------------------------------------------------

def wait_for_index_table(page, timeout: int = 30000) -> None:
    """Wait for the transcript index table to render at least one data row."""
    page.wait_for_function(
        "() => document.querySelectorAll('.v-data-table tbody tr').length > 0",
        timeout=timeout,
    )
    time.sleep(1)


def set_rows_per_page_max(page) -> None:
    """
    Try to change the rows-per-page selector to the largest option available
    so we get as many transcripts as possible without pagination.
    """
    try:
        # The footer select is the v-select inside v-data-footer
        footer_select = page.query_selector(".v-data-footer .v-select")
        if not footer_select:
            return
        footer_select.click()
        time.sleep(0.8)

        # Options appear in a menu — pick the last (largest) one
        options = page.query_selector_all(".menuable__content__active .v-list-item")
        if options:
            # Filter visible options; pick the last one (usually 100 or "All")
            visible = [o for o in options if o.is_visible()]
            if visible:
                visible[-1].click()
                time.sleep(2)
    except Exception:
        pass


def collect_transcript_links(page, earnings_only: bool = False) -> list[dict]:
    """
    Read all visible rows from the transcript index table and return metadata.

    Each entry: {date, title, url}
    """
    rows = page.evaluate("""() => {
        const trs = Array.from(document.querySelectorAll('.v-data-table tbody tr'));
        return trs.map(tr => {
            const dateCell  = tr.querySelector('td.event-date');
            const titleLink = tr.querySelector('td a');
            const date  = dateCell  ? dateCell.innerText.trim()      : '';
            const title = titleLink ? titleLink.innerText.trim()     : '';
            const href  = titleLink ? titleLink.getAttribute('href') : null;
            return { date, title, href };
        }).filter(r => r.href);
    }""")

    if not rows:
        return []

    result = []
    for r in rows:
        title = r["title"]
        if earnings_only and "earnings call" not in title.lower():
            continue
        result.append({
            "date":  r["date"],
            "title": title,
            "url":   TIKR_BASE_URL + r["href"],
        })
    return result


# ---------------------------------------------------------------------------
# Transcript page — extract text
# ---------------------------------------------------------------------------

def wait_for_transcript_text(page, timeout: int = 30000) -> bool:
    """Wait for the transcript text to render on the page."""
    try:
        page.wait_for_function(
            """() => {
                const el = document.querySelector(
                    '.transcript-content-wrapper, .transcript-preview-wrapper'
                );
                return el && el.innerText && el.innerText.trim().length > 200;
            }""",
            timeout=timeout,
        )
        return True
    except PlaywrightTimeout:
        return False


def extract_transcript_text(page) -> str:
    """Extract the full text from a transcript detail page."""
    return page.evaluate("""() => {
        // Prefer the dedicated wrapper; fall back to the broader preview layout
        for (const sel of [
            '.transcript-content-wrapper',
            '.transcript-preview-wrapper',
            '.transcript-preview-layout',
        ]) {
            const el = document.querySelector(sel);
            if (el && el.innerText && el.innerText.trim().length > 200) {
                return el.innerText.trim();
            }
        }
        return '';
    }""") or ""


def is_paywalled(page) -> bool:
    """
    Return True only if the transcript content is absent AND the page
    contains a paywall/upgrade prompt in the main content area (not the
    promotional banner that appears on every TIKR page).
    """
    return page.evaluate("""() => {
        // First: if transcript content is present, it's not paywalled
        const wrapper = document.querySelector(
            '.transcript-content-wrapper, .transcript-preview-wrapper'
        );
        if (wrapper && wrapper.innerText && wrapper.innerText.trim().length > 100) {
            return false;
        }
        // Second: look for paywall indicators in the main content (not the top nav banner)
        const main = document.querySelector('.v-main, main, #app');
        const text = main ? main.innerText.toLowerCase() : '';
        return text.includes('upgrade to access') || text.includes('unlock transcript')
            || text.includes('subscribe to view') || text.includes('premium feature');
    }""") or False


# ---------------------------------------------------------------------------
# Saving
# ---------------------------------------------------------------------------

def save_transcript(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    print(f"  Saved → {path}  ({len(text):,} chars)")


def save_index(rows: list[dict], path: Path, fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Index → {path}  ({len(rows)} rows)")


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

def extract_transcripts(
    ticker: str,
    output_dir: Path,
    headless: bool = True,
    direct_url: str | None = None,
    exchange: str | None = None,
    limit: int | None = None,
    earnings_only: bool = False,
) -> None:
    ticker = ticker.upper()
    transcripts_dir = output_dir / ticker / "transcripts"
    cleanup_browser_locks()

    with sync_playwright() as p:
        browser = p.chromium.launch_persistent_context(
            user_data_dir=str(BROWSER_PROFILE_DIR),
            headless=headless,
            channel="chrome",
            viewport={"width": 1400, "height": 900},
            args=["--disable-blink-features=AutomationControlled"],
            ignore_default_args=["--enable-automation"],
        )
        page = browser.new_page()

        try:
            # ── Step 1: Get the transcript index URL ──────────────────────────
            if direct_url:
                index_url = build_transcripts_index_url(direct_url)
            else:
                print(f"Searching TIKR for {ticker} ...")
                index_url = navigate_via_search(page, ticker, exchange=exchange)
                if not index_url:
                    sys.exit(1)

            # ── Step 2: Load index page and collect links ──────────────────────
            print(f"Loading transcript index: {index_url}")
            page.goto(index_url, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except PlaywrightTimeout:
                pass
            dismiss_overlay(page)

            print("Waiting for transcript table ...")
            try:
                wait_for_index_table(page)
            except PlaywrightTimeout:
                print("Error: Transcript table did not load.", file=sys.stderr)
                sys.exit(1)

            # Expand rows per page to reduce pagination
            set_rows_per_page_max(page)

            all_links = collect_transcript_links(page, earnings_only=earnings_only)
            if not all_links:
                print("Error: No transcript links found in table.", file=sys.stderr)
                sys.exit(1)

            print(f"Found {len(all_links)} transcript(s).")
            to_download = all_links[:limit] if limit else all_links
            if limit:
                print(f"Limiting to {limit} most recent.")

            # ── Step 3: Download each transcript ──────────────────────────────
            downloaded = []
            skipped = []
            for i, item in enumerate(to_download):
                date  = item["date"]
                title = item["title"]
                url   = item["url"]

                date_slug = _normalise_date(date) or safe_filename(date)
                title_slug = safe_filename(title[:70])
                fname = f"{ticker}_{date_slug}_{title_slug}.txt"
                out_path = transcripts_dir / fname

                if out_path.exists():
                    print(f"[{i+1}/{len(to_download)}] Skipping (exists): {fname}")
                    downloaded.append({"date": date, "title": title, "filename": fname})
                    continue

                print(f"\n[{i+1}/{len(to_download)}] {date} — {title[:80]}")

                page.goto(url, wait_until="domcontentloaded")
                try:
                    page.wait_for_load_state("networkidle", timeout=15000)
                except PlaywrightTimeout:
                    pass

                if not wait_for_transcript_text(page):
                    if is_paywalled(page):
                        print("  Skipped — paywalled.")
                    else:
                        print("  Warning: Transcript text did not load, skipping.",
                              file=sys.stderr)
                    skipped.append(item)
                    continue

                text = extract_transcript_text(page)
                if not text or len(text) < 100:
                    print("  Warning: Empty transcript, skipping.", file=sys.stderr)
                    skipped.append(item)
                    continue

                save_transcript(text, out_path)
                downloaded.append({"date": date, "title": title, "filename": fname})

            # ── Step 4: Write index ────────────────────────────────────────────
            print()
            if downloaded:
                save_index(
                    downloaded,
                    transcripts_dir / f"{ticker}_transcripts_index.csv",
                    fields=["date", "title", "filename"],
                )
            else:
                print("No transcripts were successfully extracted.", file=sys.stderr)

            if skipped:
                print(f"{len(skipped)} transcript(s) skipped (not loaded or paywalled).")

        finally:
            browser.close()


def _normalise_date(date_str: str) -> str | None:
    """Try to parse a human date like 'February 13, 2026' → '2026-02-13'."""
    try:
        from datetime import datetime
        for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract earnings-call transcripts from TIKR Terminal"
    )
    parser.add_argument("ticker", type=str.upper, help="Ticker symbol (e.g. CROX)")
    parser.add_argument(
        "--url",
        type=str,
        default=None,
        help="Any TIKR URL for this company (estimates, financials, etc.) — "
             "cid/tid are extracted. Example: the estimates URL from the TIKR app.",
    )
    parser.add_argument(
        "--exchange", "-e",
        type=str,
        default=None,
        help="Prefer search result from this exchange (e.g. LSE, NASDAQ).",
    )
    parser.add_argument(
        "--limit", "-n",
        type=int,
        default=None,
        help="Max number of recent accessible transcripts to download (default: all).",
    )
    parser.add_argument(
        "--earnings-only",
        action="store_true",
        help="Only download earnings calls (skip conference presentations etc.).",
    )
    parser.add_argument(
        "--output-dir", "-o",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Base output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--visible",
        action="store_true",
        help="Run browser visibly for debugging.",
    )

    args = parser.parse_args()

    if not BROWSER_PROFILE_DIR.exists():
        print("No saved TIKR session found. Login first:", file=sys.stderr)
        print("  python extract_tikr_estimates.py ANY --login", file=sys.stderr)
        sys.exit(1)

    extract_transcripts(
        ticker=args.ticker,
        output_dir=args.output_dir,
        headless=not args.visible,
        direct_url=args.url,
        exchange=args.exchange,
        limit=args.limit,
        earnings_only=args.earnings_only,
    )


if __name__ == "__main__":
    main()
