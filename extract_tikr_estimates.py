#!/usr/bin/env python3
"""
Extract forward analyst estimates from TIKR Terminal for a given ticker.

Uses Playwright for browser automation with a persistent login session.
First run requires --login flag to authenticate manually.

Usage:
    # First time: login to TIKR (opens visible browser)
    python extract_tikr_estimates.py TDG --login

    # Subsequent runs: headless, reuses saved session
    python extract_tikr_estimates.py TDG

    # Custom output directory
    python extract_tikr_estimates.py TDG -o /path/to/output

Requirements:
    pip install playwright
    playwright install chromium
"""

import argparse
import csv
import json
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# Add parent dir to path so config.py can be imported when run standalone
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DEFAULT_RESEARCH_DIR

# Default output base directory
DEFAULT_OUTPUT_DIR = DEFAULT_RESEARCH_DIR

# Persistent browser profile directory
BROWSER_PROFILE_DIR = Path.home() / ".tikr-playwright-session"

TIKR_BASE_URL = "https://app.tikr.com"

# Cache file mapping ticker → known-good TIKR estimates URL
TIKR_URL_CACHE_FILE = Path(__file__).resolve().parent / "tikr_url_cache.json"


def load_url_cache() -> dict:
    if TIKR_URL_CACHE_FILE.exists():
        try:
            return json.loads(TIKR_URL_CACHE_FILE.read_text())
        except Exception:
            return {}
    return {}


def save_url_cache(cache: dict) -> None:
    TIKR_URL_CACHE_FILE.write_text(json.dumps(cache, indent=2))


def wait_for_estimates_table(page, timeout: int = 30000) -> None:
    """Wait for the estimates data table to fully load."""
    page.wait_for_selector("table.fintab", timeout=timeout)
    # Wait for at least one data row with numbers
    page.wait_for_function(
        """() => {
            const cells = document.querySelectorAll('table.fintab td');
            return Array.from(cells).some(c => /\\d/.test(c.innerText));
        }""",
        timeout=timeout,
    )
    # Small extra wait for any remaining rendering
    time.sleep(1)


def navigate_to_ticker_estimates(page, ticker: str, exchange: str | None = None) -> bool:
    """Navigate to the estimates page for a given ticker.

    Strategy:
    1. Go to TIKR home, let it load and resolve any redirects/login.
    2. Click the Vuetify search container and type the ticker.
    3. Click the first dropdown result, then find and navigate to the Estimates tab.

    TIKR is a Vue SPA – selecting a search result loads content without changing
    the URL, so we detect navigation by looking for the Estimates tab link that
    appears once a stock is selected.
    """
    # Step 1: Load TIKR and wait for the app to be ready
    page.goto(TIKR_BASE_URL, wait_until="domcontentloaded")
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except PlaywrightTimeout:
        pass  # Proceed anyway, page may have long-polling connections

    # Check if we got redirected to login
    if "/login" in page.url or "/signin" in page.url:
        print("Error: Not logged in. Run with --login first.", file=sys.stderr)
        return False

    # Dismiss any blocking overlay/modal (e.g. a welcome dialog or cookie banner)
    try:
        overlay = page.query_selector('.v-overlay--active')
        if overlay and overlay.is_visible():
            # Press Escape to close the modal
            page.keyboard.press("Escape")
            time.sleep(0.5)
            # If still present, try clicking the scrim to dismiss
            scrim = page.query_selector('.v-overlay__scrim')
            if scrim and scrim.is_visible():
                scrim.click(force=True)
                time.sleep(0.5)
    except Exception:
        pass

    # Step 2: Click the search container (the inner <input> has height:0 so
    # Playwright considers it invisible – click the wrapper div instead)
    search_container = None
    container_selectors = ['.tickers--search', '.v-autocomplete']
    for sel in container_selectors:
        try:
            search_container = page.wait_for_selector(sel, timeout=5000)
            if search_container and search_container.is_visible():
                break
            search_container = None
        except PlaywrightTimeout:
            continue

    if not search_container:
        print("Error: Could not find search input on TIKR.", file=sys.stderr)
        return False

    search_container.click(force=True)
    time.sleep(0.5)

    # Type the ticker via keyboard (input is now focused)
    page.keyboard.type(ticker, delay=80)
    time.sleep(3)  # Wait for autocomplete dropdown

    # Step 3: Click the best matching dropdown result.
    # If exchange is supplied (e.g. "LSE", "NYSE"), prefer the item whose text
    # contains that string; otherwise fall back to the first visible item.
    # Also extract any href from the item so we can navigate directly.
    clicked = False
    item_href = None
    items = page.query_selector_all('.menuable__content__active .v-list-item')
    exchange_upper = exchange.upper() if exchange else ""
    first_visible = None
    first_visible_href = None

    for item in items:
        try:
            if not item.is_visible():
                continue
            href = item.get_attribute("href") or ""
            if first_visible is None:
                first_visible = item
                first_visible_href = href
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
            item_href = first_visible_href
            clicked = True
        except Exception:
            pass

    if not clicked:
        print(f"Error: No search results found for {ticker} on TIKR. "
              f"Re-run with --url <tikr-estimates-url> for this ticker.", file=sys.stderr)
        return False

    time.sleep(3)  # Wait for stock content to load

    # Step 4: Find the Estimates tab link and navigate directly to it.
    # Prefer a link whose cid matches what we clicked (from the dropdown href),
    # to avoid accidentally picking up a link from a previously loaded stock.
    cid_from_click = None
    if item_href:
        import re as _re
        m = _re.search(r"cid=(\d+)", item_href)
        if m:
            cid_from_click = m.group(1)

    estimates_href = page.evaluate("""(cid) => {
        const links = document.querySelectorAll('a[href*="estimates"], a[href*="tab=est"]');
        // If we know the cid from the clicked item, prefer a matching link
        if (cid) {
            for (const a of links) {
                if (a.offsetParent !== null && a.href.includes('cid=' + cid)) return a.href;
            }
        }
        // Fall back to any visible estimates link
        for (const a of links) {
            if (a.offsetParent !== null) return a.href;
        }
        return null;
    }""", cid_from_click)

    if estimates_href:
        page.goto(estimates_href, wait_until="domcontentloaded")
    else:
        # Fallback: try clicking an Estimates tab
        estimates_clicked = False
        for tab_sel in [
            'a:has-text("Estimates")',
            'a:has-text("Analyst Estimates")',
            'button:has-text("Estimates")',
            '[role="tab"]:has-text("Estimates")',
        ]:
            try:
                el = page.query_selector(tab_sel)
                if el and el.is_visible():
                    el.click()
                    estimates_clicked = True
                    break
            except Exception:
                continue

        if not estimates_clicked:
            print(f"Error: Could not find Estimates tab for {ticker}.", file=sys.stderr)
            return False

    # Step 5: Wait for the estimates table to render
    try:
        wait_for_estimates_table(page)
    except PlaywrightTimeout:
        print(f"Error: Estimates table did not load for {ticker}.", file=sys.stderr)
        return False

    return True


def extract_table_data(page) -> list[list[str]]:
    """Extract all rows from the estimates table via JavaScript."""
    data = page.evaluate("""() => {
        const table = document.querySelector('table.fintab');
        if (!table) return [];
        const rows = table.querySelectorAll('tr');
        const result = [];
        rows.forEach(row => {
            const cells = row.querySelectorAll('th, td');
            const rowData = Array.from(cells).map(cell => cell.innerText.trim());
            if (rowData.some(d => d !== '')) result.push(rowData);
        });
        return result;
    }""")
    return data


def filter_forward_estimates(data: list[list[str]]) -> list[list[str]]:
    """Filter columns to keep only forward estimate years (headers containing 'E')."""
    if not data:
        return data

    header = data[0]

    # Find which column indices are forward estimates or labels
    # Column 0 is always the metric label; keep it
    # Keep columns whose header contains " E" (e.g., "9/30/26 E")
    # Also keep CAGR column if present
    keep_indices = [0]  # Always keep the label column
    for i, col in enumerate(header):
        if i == 0:
            continue
        col_upper = col.strip().upper()
        if col_upper.endswith("E") or col_upper == "CAGR":
            keep_indices.append(i)

    if len(keep_indices) <= 1:
        print("Warning: No forward estimate columns detected, returning all data.",
              file=sys.stderr)
        return data

    # Filter each row
    filtered = []
    for row in data:
        filtered_row = []
        for i in keep_indices:
            if i < len(row):
                filtered_row.append(row[i])
            else:
                filtered_row.append("")
        filtered.append(filtered_row)

    return filtered


def save_csv(data: list[list[str]], output_path: Path) -> None:
    """Save extracted data as CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerows(data)
    print(f"Saved → {output_path}  ({len(data)} rows)")


def do_login(playwright) -> None:
    """Open a visible browser for manual TIKR login."""
    print("\n=== TIKR Login ===")
    print("A browser window will open. Please log in to TIKR.")
    print("The script will detect when you're logged in and save the session.\n")

    browser = playwright.chromium.launch_persistent_context(
        user_data_dir=str(BROWSER_PROFILE_DIR),
        headless=False,
        viewport={"width": 1400, "height": 900},
    )
    page = browser.new_page()
    page.goto(TIKR_BASE_URL)

    # Wait until the URL no longer contains login/signin (i.e., user is logged in)
    # or until the page shows authenticated content (search bar, dashboard, etc.)
    print("Waiting for login (up to 5 minutes) ...")
    for _ in range(300):  # 5 minutes, check every second
        time.sleep(1)
        url = page.url
        # If we're on a non-login page, user is logged in
        if "/login" not in url and "/signin" not in url and "/register" not in url:
            # Double-check: look for an element that only shows when logged in
            try:
                logged_in = page.query_selector(
                    'input[role="combobox"], input[placeholder*="earch"], '
                    '[class*="avatar"], [class*="user"], [class*="profile"]'
                )
                if logged_in:
                    time.sleep(2)  # Give cookies time to settle
                    break
            except Exception:
                pass

    browser.close()
    print("Session saved. You can now run the script without --login.")


def cleanup_browser_locks() -> None:
    """Remove stale Chromium lock files from the profile directory."""
    for lock_file in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
        lock_path = BROWSER_PROFILE_DIR / lock_file
        lock_path.unlink(missing_ok=True)


def extract_estimates(ticker: str, output_dir: Path, headless: bool = True,
                      direct_url: str | None = None,
                      exchange: str | None = None) -> None:
    """Main extraction flow."""
    ticker = ticker.upper()
    output_path = output_dir / ticker / f"{ticker}_tikr_forward_estimates.csv"
    cleanup_browser_locks()

    cache = load_url_cache()

    # Resolve URL: explicit arg > cache > search
    if not direct_url and ticker in cache:
        direct_url = cache[ticker]
        print(f"Using cached URL for {ticker}: {direct_url}")

    with sync_playwright() as p:
        browser = p.chromium.launch_persistent_context(
            user_data_dir=str(BROWSER_PROFILE_DIR),
            headless=headless,
            viewport={"width": 1400, "height": 900},
        )
        page = browser.new_page()

        try:
            if direct_url:
                print(f"Navigating directly to: {direct_url}")
                page.goto(direct_url, wait_until="domcontentloaded")
                try:
                    page.wait_for_load_state("networkidle", timeout=15000)
                except PlaywrightTimeout:
                    pass
                try:
                    wait_for_estimates_table(page)
                except PlaywrightTimeout:
                    print("Error: Estimates table did not load.", file=sys.stderr)
                    sys.exit(1)
            else:
                print(f"Navigating to TIKR estimates for {ticker} ...")
                if not navigate_to_ticker_estimates(page, ticker, exchange=exchange):
                    sys.exit(1)
                print(f"Landed on: {page.url}")

            # Save the resolved URL to cache so future runs go directly to the right page
            landed_url = page.url
            if landed_url and "tikr.com" in landed_url:
                cache[ticker] = landed_url
                save_url_cache(cache)

            print("Extracting table data ...")
            raw_data = extract_table_data(page)
            if not raw_data:
                print("Error: No data extracted from table.", file=sys.stderr)
                sys.exit(1)

            print(f"Extracted {len(raw_data)} rows, filtering to forward estimates ...")
            filtered = filter_forward_estimates(raw_data)
            save_csv(filtered, output_path)

        finally:
            browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract forward analyst estimates from TIKR Terminal"
    )
    parser.add_argument("ticker", type=str.upper, help="Ticker symbol (e.g. TDG)")
    parser.add_argument(
        "--login",
        action="store_true",
        help="Open browser for manual TIKR login (first-time setup)",
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
        help="Run browser in visible (non-headless) mode for debugging",
    )

    parser.add_argument(
        "--url",
        type=str,
        default=None,
        help="Navigate directly to this TIKR estimates URL instead of searching by ticker",
    )
    parser.add_argument(
        "--exchange", "-e",
        type=str,
        default=None,
        help="Prefer search result matching this exchange (e.g. LSE, NYSE, NASDAQ, TSX). "
             "Not needed for unambiguous US tickers.",
    )

    args = parser.parse_args()

    if args.login:
        with sync_playwright() as p:
            do_login(p)
        return

    if not BROWSER_PROFILE_DIR.exists():
        print("No saved session found. Run with --login first:", file=sys.stderr)
        print(f"  python {sys.argv[0]} {args.ticker} --login", file=sys.stderr)
        sys.exit(1)

    extract_estimates(args.ticker, args.output_dir, headless=not args.visible,
                      direct_url=args.url, exchange=args.exchange)


if __name__ == "__main__":
    main()
