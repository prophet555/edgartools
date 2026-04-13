#!/usr/bin/env python3
"""
Download 10-K and 10-Q filings from TIKR Terminal for a given ticker.

Clicks each filing row on the TIKR filings tab, intercepts the signed S3
download URL from api.tikr.com/getDefaultDoc, and saves the file locally.

Uses the same persistent Playwright session as extract_tikr_estimates.py.
Run that script with --login first if you haven't authenticated yet.

Usage:
    # Supply any TIKR URL for the company (cid/tid are extracted)
    python extract_tikr_filings.py CROX --url "https://app.tikr.com/stock/estimates?cid=13639284&tid=25835213&ref=cdg2dy&tab=est"

    # Search by ticker
    python extract_tikr_filings.py CROX

    # Also include 10-Q filings (default: 10-K only)
    python extract_tikr_filings.py CROX --forms 10-K 10-Q

    # Custom output directory
    python extract_tikr_filings.py CROX -o /path/to/output

    # Visible browser for debugging
    python extract_tikr_filings.py CROX --visible

Output:
    <output_dir>/<TICKER>/filings/<TICKER>_<form>_<period>.{html,pdf,...}
    <output_dir>/<TICKER>/filings/<TICKER>_filings_index.csv
"""

import argparse
import csv
import re
import sys
import time
from pathlib import Path

import httpx
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DEFAULT_RESEARCH_DIR

DEFAULT_OUTPUT_DIR = DEFAULT_RESEARCH_DIR
BROWSER_PROFILE_DIR = Path.home() / ".tikr-playwright-session"
TIKR_BASE_URL = "https://app.tikr.com"

DEFAULT_FORMS = ["10-K", "10-Q"]
FOREIGN_FORMS = ["20-F", "40-F", "6-K"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def cleanup_browser_locks() -> None:
    for lock_file in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
        (BROWSER_PROFILE_DIR / lock_file).unlink(missing_ok=True)


def safe_filename(s: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "_", s).strip()


def build_filings_url(base_url: str) -> str:
    """Given any TIKR stock URL, return the /stock/filings URL."""
    cid = re.search(r"cid=(\d+)", base_url)
    tid = re.search(r"tid=(\d+)", base_url)
    ref = re.search(r"ref=([^&]+)", base_url)
    if not cid or not tid:
        return base_url
    url = f"{TIKR_BASE_URL}/stock/filings?cid={cid.group(1)}&tid={tid.group(1)}"
    if ref:
        url += f"&ref={ref.group(1)}"
    return url


def normalise_date(date_str: str) -> str | None:
    """Parse 'M/D/YY' or 'M/D/YYYY' → 'YYYY-MM-DD'."""
    try:
        from datetime import datetime
        for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    except Exception:
        pass
    return None


def ext_from_content_type(ct: str) -> str:
    ct = ct.lower().split(";")[0].strip()
    mapping = {
        "application/pdf": ".pdf",
        "text/html": ".html",
        "application/xhtml+xml": ".html",
        "text/plain": ".txt",
        "application/zip": ".zip",
        "application/octet-stream": ".bin",
    }
    return mapping.get(ct, ".bin")


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
    """Search for ticker and return the filings URL. Returns None on failure."""
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
        return build_filings_url(item_href)

    # No href on the clicked item — wait for SPA to load, then find cid/tid from any DOM link
    time.sleep(3)
    current_url = page.url
    if "tikr.com" in current_url and ("cid=" in current_url or "tid=" in current_url):
        return build_filings_url(current_url)

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
        return build_filings_url(found_href)

    print(f"Error: Could not determine filings URL for {ticker} after search.", file=sys.stderr)
    return None


# ---------------------------------------------------------------------------
# Index page
# ---------------------------------------------------------------------------

def wait_for_table(page, timeout: int = 30000) -> None:
    page.wait_for_function(
        "() => document.querySelectorAll('.v-data-table tbody tr').length > 0",
        timeout=timeout,
    )
    time.sleep(1)


def set_rows_per_page_max(page) -> None:
    """Expand rows-per-page to the largest available option."""
    try:
        footer_select = page.query_selector(".v-data-footer .v-select")
        if not footer_select:
            return
        footer_select.click()
        time.sleep(0.8)
        options = page.query_selector_all(".menuable__content__active .v-list-item")
        visible = [o for o in options if o.is_visible()]
        if visible:
            visible[-1].click()
            time.sleep(2)
    except Exception:
        pass


def collect_filing_rows(page, forms: list[str]) -> list[dict]:
    """
    Return metadata for all visible rows matching the requested form types.

    TIKR filings table columns (by position):
      0 — Document Title (contains the <a> link)
      1 — Form Name
      2 — Language
      3 — Financial Period End Date
      4 — Release Date
    """
    forms_upper = [f.upper() for f in forms]
    rows = page.evaluate("""(forms) => {
        const trs = Array.from(document.querySelectorAll('.v-data-table tbody tr'));
        return trs.map(tr => {
            const tds = Array.from(tr.querySelectorAll('td'));
            if (tds.length < 5) return null;
            return {
                title:   tds[0].innerText.trim(),
                form:    tds[1].innerText.trim(),
                period:  tds[3].innerText.trim(),
                release: tds[4].innerText.trim(),
            };
        }).filter(r => r && forms.includes(r.form.toUpperCase()));
    }""", forms_upper)
    return rows or []


# ---------------------------------------------------------------------------
# Download a single filing
# ---------------------------------------------------------------------------

def setup_route_interceptor(page) -> list:
    """
    Register a Playwright route handler for getDefaultDoc and return a
    shared list that the handler appends S3 URLs to.

    Uses page.route (network-level interception) rather than a JS fetch
    wrapper so it works regardless of whether TIKR uses fetch, XHR, or axios.
    """
    captured: list[str] = []

    def handle(route):
        resp = route.fetch()
        try:
            import json as _json
            data = _json.loads(resp.body())
            if data.get("url"):
                captured.append(data["url"])
        except Exception:
            pass
        route.fulfill(response=resp)

    page.route("**/getDefaultDoc**", handle)
    return captured


def click_row_and_get_download_url(
    page, form: str, period: str, captured: list, timeout_ms: int = 20000
) -> str | None:
    """
    Find the row matching (form, period) in the current DOM, click its link,
    and wait for the route interceptor to append a new S3 URL to `captured`.

    Re-queries the DOM at click time so stale indices from a previous render
    cycle never cause misses.  Polls with page.wait_for_timeout() to keep
    the Playwright event loop running so the route handler can fire.
    """
    before = len(captured)

    # Scroll the matching row into view and click it
    clicked = page.evaluate("""([form, period]) => {
        const trs = Array.from(document.querySelectorAll('.v-data-table tbody tr'));
        for (const tr of trs) {
            const tds = Array.from(tr.querySelectorAll('td'));
            if (tds.length < 4) continue;
            const rowForm   = tds[1].innerText.trim();
            const rowPeriod = tds[3].innerText.trim();
            if (rowForm === form && rowPeriod === period) {
                const link = tr.querySelector('a');
                if (link) {
                    tr.scrollIntoView({block: 'center'});
                    link.click();
                    return true;
                }
            }
        }
        return false;
    }""", [form, period])

    if not clicked:
        return None

    elapsed = 0
    interval = 300
    while elapsed < timeout_ms:
        page.wait_for_timeout(interval)   # yields to event loop → route handler fires
        elapsed += interval
        if len(captured) > before:
            return captured[-1]

    return None


def download_file(url: str, dest: Path) -> tuple[bool, str]:
    """
    Download a file from a signed S3 URL and save it.
    Returns (success, extension_used).
    """
    try:
        with httpx.Client(follow_redirects=True, timeout=60) as client:
            resp = client.get(url)
            resp.raise_for_status()
            ct = resp.headers.get("content-type", "application/octet-stream")
            ext = ext_from_content_type(ct)
            final_path = dest.with_suffix(ext)
            final_path.parent.mkdir(parents=True, exist_ok=True)
            final_path.write_bytes(resp.content)
            return True, ext
    except Exception as exc:
        print(f"  Download error: {exc}", file=sys.stderr)
        return False, ""


# ---------------------------------------------------------------------------
# Saving
# ---------------------------------------------------------------------------

def save_index(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["form", "period", "release", "title", "filename"]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Index → {path}  ({len(rows)} rows)")


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

def extract_filings(
    ticker: str,
    output_dir: Path,
    forms: list[str],
    headless: bool = True,
    direct_url: str | None = None,
    exchange: str | None = None,
) -> None:
    ticker = ticker.upper()
    filings_dir = output_dir / ticker / "filings"
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
            # ── Step 1: Navigate to filings page ──────────────────────────────
            if direct_url:
                filings_url = build_filings_url(direct_url)
            else:
                print(f"Searching TIKR for {ticker} ...")
                filings_url = navigate_via_search(page, ticker, exchange=exchange)
                if not filings_url:
                    sys.exit(1)

            print(f"Loading filings index: {filings_url}")
            page.goto(filings_url, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except PlaywrightTimeout:
                pass
            dismiss_overlay(page)

            print("Waiting for filings table ...")
            try:
                wait_for_table(page)
            except PlaywrightTimeout:
                print("Error: Filings table did not load.", file=sys.stderr)
                sys.exit(1)

            # Set up route interceptor (captures getDefaultDoc S3 URLs)
            captured_urls = setup_route_interceptor(page)

            # Expand rows-per-page to see as many filings as possible
            set_rows_per_page_max(page)

            # ── Step 2: Collect matching rows ──────────────────────────────────
            rows = collect_filing_rows(page, forms)
            if not rows and forms == DEFAULT_FORMS:
                # Auto-fallback: try foreign filer forms (20-F / 40-F / 6-K)
                foreign_rows = collect_filing_rows(page, FOREIGN_FORMS)
                if foreign_rows:
                    print(f"No {DEFAULT_FORMS} found — foreign filer detected, switching to {FOREIGN_FORMS}")
                    forms = FOREIGN_FORMS
                    rows = foreign_rows
            if not rows:
                # Show available form types for diagnosis
                all_forms = page.evaluate("""() => {
                    const trs = Array.from(document.querySelectorAll('.v-data-table tbody tr'));
                    const forms = new Set();
                    trs.forEach(tr => {
                        const tds = tr.querySelectorAll('td');
                        if (tds.length >= 2) forms.add(tds[1].innerText.trim());
                    });
                    return Array.from(forms).filter(f => f);
                }""")
                if all_forms:
                    print(f"No filings found for form types: {forms}", file=sys.stderr)
                    print(f"Available forms on this page: {all_forms}", file=sys.stderr)
                else:
                    print(f"No filings found for form types: {forms}", file=sys.stderr)
                sys.exit(1)

            print(f"Found {len(rows)} filing(s) matching {forms}.")

            # ── Step 3: Download each filing ───────────────────────────────────
            downloaded = []
            for i, row in enumerate(rows):
                form     = row["form"]
                period   = row["period"]
                title    = row["title"]
                rel_date = row["release"]

                period_slug = normalise_date(period) or safe_filename(period)
                base_name   = f"{ticker}_{form}_{period_slug}"
                print(f"\n[{i+1}/{len(rows)}] {form}  period={period}  released={rel_date}")
                print(f"  {title[:80]}")

                # Skip if already downloaded (any extension)
                existing = list(filings_dir.glob(f"{base_name}.*"))
                if existing:
                    print(f"  Already exists: {existing[0].name} — skipping.")
                    downloaded.append({
                        "form": form, "period": period, "release": rel_date,
                        "title": title, "filename": existing[0].name,
                    })
                    continue

                # Re-expand rows after each Vue re-render resets the table
                set_rows_per_page_max(page)
                download_url = click_row_and_get_download_url(
                    page, form, period, captured_urls
                )
                if not download_url:
                    print("  Warning: Could not get download URL, skipping.",
                          file=sys.stderr)
                    continue

                dest = filings_dir / base_name  # extension added by download_file
                ok, ext = download_file(download_url, dest)
                if ok:
                    fname = base_name + ext
                    size_kb = (filings_dir / fname).stat().st_size // 1024
                    print(f"  Saved → {fname}  ({size_kb:,} KB)")
                    downloaded.append({
                        "form": form, "period": period, "release": rel_date,
                        "title": title, "filename": fname,
                    })

            # ── Step 4: Write index ────────────────────────────────────────────
            print()
            if downloaded:
                save_index(
                    downloaded,
                    filings_dir / f"{ticker}_filings_index.csv",
                )
            else:
                print("No filings were successfully downloaded.", file=sys.stderr)

        finally:
            browser.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download 10-K/10-Q filings from TIKR Terminal"
    )
    parser.add_argument("ticker", type=str.upper, help="Ticker symbol (e.g. CROX)")
    parser.add_argument(
        "--url",
        type=str,
        default=None,
        help="Any TIKR URL for this company — cid/tid are extracted. "
             "Example: the estimates URL from the TIKR app.",
    )
    parser.add_argument(
        "--exchange", "-e",
        type=str,
        default=None,
        help="Prefer search result from this exchange (e.g. LSE, NASDAQ).",
    )
    parser.add_argument(
        "--forms",
        nargs="+",
        default=DEFAULT_FORMS,
        metavar="FORM",
        help=f"Form types to download (default: {DEFAULT_FORMS}). "
             "E.g. --forms 10-K  or  --forms 10-K 10-Q",
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

    extract_filings(
        ticker=args.ticker,
        output_dir=args.output_dir,
        forms=args.forms,
        headless=not args.visible,
        direct_url=args.url,
        exchange=args.exchange,
    )


if __name__ == "__main__":
    main()
