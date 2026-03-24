#!/usr/bin/env python3
"""
Search TIKR for a ticker and return the list of matching results.

Prints JSON to stdout: list of {"text": "...", "exchange": "..."} objects.
Exit 0 on success, 1 on error.

Usage:
    python search_tikr.py VOD
    python search_tikr.py VOD --visible
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

BROWSER_PROFILE_DIR = Path.home() / ".tikr-playwright-session"
TIKR_BASE_URL = "https://app.tikr.com"


def cleanup_browser_locks() -> None:
    for lock_file in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
        (BROWSER_PROFILE_DIR / lock_file).unlink(missing_ok=True)


def search_ticker(ticker: str, headless: bool = True) -> list[dict]:
    """
    Open TIKR, type the ticker, and return all dropdown results as a list of
    {"text": <full item text>, "exchange": <exchange code or "">} dicts.
    """
    cleanup_browser_locks()

    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch_persistent_context(
            user_data_dir=str(BROWSER_PROFILE_DIR),
            headless=headless,
            viewport={"width": 1400, "height": 900},
        )
        page = browser.new_page()
        try:
            page.goto(TIKR_BASE_URL, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except PlaywrightTimeout:
                pass

            if "/login" in page.url or "/signin" in page.url:
                print("Error: Not logged in. Run: python extract_tikr_estimates.py ANY --login",
                      file=sys.stderr)
                sys.exit(1)

            # Dismiss overlay if present
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

            # Find search box
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
                sys.exit(1)

            search_container.click(force=True)
            time.sleep(0.5)
            page.keyboard.type(ticker, delay=80)
            time.sleep(3)

            # Collect all visible dropdown items
            items = page.query_selector_all(".menuable__content__active .v-list-item")
            for item in items:
                try:
                    if not item.is_visible():
                        continue
                    text = (item.inner_text() or "").strip()
                    if not text:
                        continue
                    # Try to extract exchange code — typically the last ALL-CAPS word
                    # e.g. "Vodafone Group PLC  VOD  LSE"
                    parts = re.split(r"\s{2,}|\n", text)
                    exchange = ""
                    for part in reversed(parts):
                        part = part.strip()
                        if re.match(r"^[A-Z]{2,6}$", part) and part != ticker.upper():
                            exchange = part
                            break
                    results.append({"text": text, "exchange": exchange})
                except Exception:
                    continue

        finally:
            browser.close()

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Search TIKR for ticker matches")
    parser.add_argument("ticker", type=str.upper)
    parser.add_argument("--visible", action="store_true")
    args = parser.parse_args()

    if not BROWSER_PROFILE_DIR.exists():
        print("No saved session. Run: python extract_tikr_estimates.py ANY --login",
              file=sys.stderr)
        sys.exit(1)

    results = search_ticker(args.ticker, headless=not args.visible)
    print(json.dumps(results, ensure_ascii=False))


if __name__ == "__main__":
    main()
