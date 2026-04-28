#!/usr/bin/env python3
"""
Extract historical financials from TIKR Terminal for a given ticker.

Pulls Income Statement, Balance Sheet, and Cash Flow Statement for the
past 5 years and writes everything into a single combined CSV file.

Uses the same persistent Playwright session as extract_tikr_estimates.py.
Run that script with --login first if you haven't already.

Usage:
    # First time: login to TIKR (opens visible browser)
    python extract_tikr_estimates.py TDG --login

    # Extract financials (reuses saved session)
    python extract_tikr_financials.py TDG

    # Custom output directory
    python extract_tikr_financials.py TDG -o /path/to/output

    # Visible browser for debugging
    python extract_tikr_financials.py TDG --visible

Requirements:
    pip install playwright
    playwright install chromium
"""

import argparse
import csv
import sys
import time
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DEFAULT_RESEARCH_DIR
from extract_tikr_estimates import load_url_cache

DEFAULT_OUTPUT_DIR = DEFAULT_RESEARCH_DIR
BROWSER_PROFILE_DIR = Path.home() / ".tikr-playwright-session"
TIKR_BASE_URL = "https://app.tikr.com"

# Financial statement tabs to extract, in order
FINANCIAL_TABS = [
    ("Income Statement", "income"),
    ("Balance Sheet", "balance"),
    ("Cash Flow Statement", "cashflow"),
]


def cleanup_browser_locks() -> None:
    for lock_file in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
        (BROWSER_PROFILE_DIR / lock_file).unlink(missing_ok=True)


def wait_for_financial_table(page, timeout: int = 30000) -> None:
    """Wait for a financial data table to fully render."""
    page.wait_for_selector("table.fintab", timeout=timeout)
    page.wait_for_function(
        """() => {
            const cells = document.querySelectorAll('table.fintab td');
            return Array.from(cells).some(c => /\\d/.test(c.innerText));
        }""",
        timeout=timeout,
    )
    time.sleep(1)


def navigate_to_ticker(page, ticker: str, exchange: str | None = None) -> bool:
    """Load TIKR and select the given ticker via the search box."""
    page.goto(TIKR_BASE_URL, wait_until="domcontentloaded")
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except PlaywrightTimeout:
        pass

    if "/login" in page.url or "/signin" in page.url:
        print("Error: Not logged in. Run extract_tikr_estimates.py with --login first.",
              file=sys.stderr)
        return False

    # Dismiss any blocking overlay
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

    # Find and click the search container
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
        return False

    search_container.click(force=True)
    time.sleep(0.5)
    page.keyboard.type(ticker, delay=80)
    time.sleep(3)

    # Click the best matching dropdown result.
    # If exchange is supplied (e.g. "LSE", "NYSE"), prefer the item whose text
    # contains that string; otherwise fall back to the first visible item.
    clicked = False
    items = page.query_selector_all(".menuable__content__active .v-list-item")
    exchange_upper = exchange.upper() if exchange else ""
    first_visible = None
    for item in items:
        try:
            if not item.is_visible():
                continue
            if first_visible is None:
                first_visible = item
            if exchange_upper and exchange_upper in (item.inner_text() or "").upper():
                item.click()
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
        page.keyboard.press("Enter")

    time.sleep(3)
    return True


def navigate_to_financials_tab(page, ticker: str) -> bool:
    """Navigate to the Financials tab for the currently selected ticker."""
    # Look for a direct href to the financials section
    financials_href = page.evaluate("""() => {
        const links = document.querySelectorAll(
            'a[href*="financials"], a[href*="tab=fin"], a[href*="statements"]'
        );
        for (const a of links) {
            if (a.offsetParent !== null) return a.href;
        }
        return null;
    }""")

    if financials_href:
        page.goto(financials_href, wait_until="domcontentloaded")
        time.sleep(2)
        return True

    # Fallback: click a tab labelled "Financials"
    for tab_sel in [
        'a:has-text("Financials")',
        'button:has-text("Financials")',
        '[role="tab"]:has-text("Financials")',
    ]:
        try:
            el = page.query_selector(tab_sel)
            if el and el.is_visible():
                el.click()
                time.sleep(2)
                return True
        except Exception:
            continue

    print(f"Error: Could not find Financials tab for {ticker}.", file=sys.stderr)
    return False


def click_statement_tab(page, label: str) -> bool:
    """Click one of the sub-tabs: Income Statement / Balance Sheet / Cash Flow."""
    for sel in [
        f'a:has-text("{label}")',
        f'button:has-text("{label}")',
        f'[role="tab"]:has-text("{label}")',
        f'.v-tab:has-text("{label}")',
        f'span:has-text("{label}")',
    ]:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                el.click()
                time.sleep(2)
                return True
        except Exception:
            continue

    # Broader search: any clickable element containing the label text
    try:
        el = page.locator(f"text={label}").first
        if el.is_visible():
            el.click()
            time.sleep(2)
            return True
    except Exception:
        pass

    return False


def set_annual_view(page) -> None:
    """Ensure the table is showing Annual (not quarterly) data."""
    for sel in [
        'button:has-text("Annual")',
        '[role="tab"]:has-text("Annual")',
        '.v-tab:has-text("Annual")',
        'span:has-text("Annual")',
        'a:has-text("Annual")',
    ]:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                el.click()
                time.sleep(1.5)
                return
        except Exception:
            continue


def extract_table_data(page) -> list[list[str]]:
    """Extract all rows from the visible fintab table."""
    return page.evaluate("""() => {
        const table = document.querySelector('table.fintab');
        if (!table) return [];
        const rows = table.querySelectorAll('tr');
        const result = [];
        rows.forEach(row => {
            const cells = row.querySelectorAll('th, td');
            const rowData = Array.from(cells).map(c => c.innerText.trim());
            if (rowData.some(d => d !== '')) result.push(rowData);
        });
        return result;
    }""")


def keep_recent_years(data: list[list[str]], years: int = 5) -> list[list[str]]:
    """
    Trim columns to the label column plus the most recent `years` historical
    columns (i.e. columns whose header does NOT end with 'E').

    If fewer than `years` historical columns exist, all are kept.
    """
    if not data:
        return data

    header = data[0]

    # Identify historical (non-estimate, non-CAGR) column indices
    historical_indices = []
    for i, col in enumerate(header):
        if i == 0:
            continue
        col_stripped = col.strip().upper()
        if col_stripped.endswith("E") or col_stripped in ("CAGR", ""):
            continue
        historical_indices.append(i)

    # Keep only the most recent `years`
    keep_hist = historical_indices[-years:] if len(historical_indices) > years else historical_indices
    keep_indices = [0] + keep_hist

    filtered = []
    for row in data:
        filtered_row = [row[i] if i < len(row) else "" for i in keep_indices]
        filtered.append(filtered_row)

    return filtered


def combine_statements(
    statements: dict[str, list[list[str]]]
) -> list[list[str]]:
    """
    Combine Income Statement, Balance Sheet, and Cash Flow data into one
    flat CSV block.  Each section is preceded by a blank row and a header
    row identifying the statement type.
    """
    combined: list[list[str]] = []

    for label, data in statements.items():
        if not data:
            continue
        # Section divider
        combined.append([])
        combined.append([f"=== {label} ==="])
        combined.extend(data)

    # Remove the leading blank row
    if combined and combined[0] == []:
        combined = combined[1:]

    return combined


def save_csv(data: list[list[str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerows(data)
    print(f"Saved → {output_path}  ({len(data)} rows)")


def extract_financials(ticker: str, output_dir: Path, headless: bool = True,
                       direct_url: str | None = None,
                       exchange: str | None = None) -> None:
    ticker = ticker.upper()
    output_path = output_dir / ticker / f"{ticker}_tikr_financials.csv"
    cleanup_browser_locks()

    if not direct_url:
        cache = load_url_cache()
        if ticker in cache:
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
                # Convert any TIKR URL to the financials page using cid/tid params
                parsed = urlparse(direct_url)
                qs = parse_qs(parsed.query)
                cid = qs.get("cid", [None])[0]
                tid = qs.get("tid", [None])[0]
                if cid and tid:
                    financials_url = f"{TIKR_BASE_URL}/stock/financials?{urlencode({'cid': cid, 'tid': tid})}"
                else:
                    financials_url = direct_url
                print(f"Navigating to TIKR financials for {ticker} ...")
                page.goto(financials_url, wait_until="domcontentloaded")
                try:
                    page.wait_for_load_state("networkidle", timeout=15000)
                except PlaywrightTimeout:
                    pass
                time.sleep(3)
            else:
                print(
                    f"Error: No URL for {ticker}. Pass --url <tikr-url> or add an entry to tikr_url_cache.json.",
                    file=sys.stderr,
                )
                sys.exit(1)

            statements: dict[str, list[list[str]]] = {}

            for tab_label, tab_key in FINANCIAL_TABS:
                print(f"  Extracting {tab_label} ...")

                if not click_statement_tab(page, tab_label):
                    print(f"  Warning: Could not click '{tab_label}' tab, skipping.",
                          file=sys.stderr)
                    statements[tab_label] = []
                    continue

                set_annual_view(page)

                try:
                    wait_for_financial_table(page)
                except PlaywrightTimeout:
                    print(f"  Warning: Table did not load for {tab_label}, skipping.",
                          file=sys.stderr)
                    statements[tab_label] = []
                    continue

                raw = extract_table_data(page)
                if not raw:
                    print(f"  Warning: No data for {tab_label}.", file=sys.stderr)
                    statements[tab_label] = []
                    continue

                trimmed = keep_recent_years(raw, years=5)
                statements[tab_label] = trimmed
                print(f"    → {len(trimmed)} rows, {len(trimmed[0]) - 1} year columns")

            combined = combine_statements(statements)
            if not combined:
                print("Error: No financial data extracted.", file=sys.stderr)
                sys.exit(1)

            save_csv(combined, output_path)

        finally:
            browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract 5-year historical financials from TIKR Terminal"
    )
    parser.add_argument("ticker", type=str.upper, help="Ticker symbol (e.g. TDG)")
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
        "--years",
        type=int,
        default=5,
        help="Number of historical years to keep (default: 5)",
    )

    parser.add_argument(
        "--url",
        type=str,
        default=None,
        help="Navigate directly to this TIKR financials URL instead of searching by ticker",
    )
    parser.add_argument(
        "--exchange", "-e",
        type=str,
        default=None,
        help="Prefer search result matching this exchange (e.g. LSE, NYSE, NASDAQ, TSX). "
             "Not needed for unambiguous US tickers.",
    )

    args = parser.parse_args()

    if not BROWSER_PROFILE_DIR.exists():
        print("No saved session found. Run this first:", file=sys.stderr)
        print(f"  python extract_tikr_estimates.py {args.ticker} --login", file=sys.stderr)
        sys.exit(1)

    extract_financials(args.ticker, args.output_dir, headless=not args.visible,
                       direct_url=args.url, exchange=args.exchange)


if __name__ == "__main__":
    main()
