#!/usr/bin/env python3
"""
Extract financial statements from SEC EDGAR using edgartools.

- Fetches the last 4 10-K filings (income statement, balance sheet, cash flow)
- Fetches the latest 3 10-Q filings (income statement, balance sheet, cash flow)
- Saves all files into a folder named after the ticker

Usage:
    python extract_sec.py AAPL
    python extract_sec.py TSLA --output-dir ./financials

Requirements:
    pip install edgartools pandas
"""

import os
import sys
import time
import argparse
import subprocess
from pathlib import Path

import pandas as pd
from edgar import Company, set_identity, get_identity
from config import DEFAULT_RESEARCH_DIR


def ensure_identity() -> None:
    """Set SEC EDGAR identity – required by law (User-Agent header)."""
    identity = os.environ.get("EDGAR_IDENTITY", "").strip()

    if identity:
        print(f"Using identity from environment: {identity}")
    else:
        print("\nSEC requires a User-Agent with real name + email.")
        print("Example: 'Alex Smith alex.smith.research@gmail.com'\n")
        identity = input("Enter your identity (name + email): ").strip()

        if not identity or "@" not in identity:
            print("Error: Identity must contain a valid email.", file=sys.stderr)
            sys.exit(1)

    set_identity(identity)

    if not get_identity():
        print("Failed to set identity – exiting.", file=sys.stderr)
        sys.exit(1)


def save_statement(name: str, stmt, path: Path) -> None:
    """Convert a Statement to DataFrame and save as CSV."""
    if stmt is None:
        print(f"  No data for {name}")
        return
    try:
        df = stmt.to_dataframe()
    except Exception as e:
        print(f"  Could not convert {name} to dataframe: {e}")
        return
    if df is not None and not df.empty:
        df.to_csv(path, encoding="utf-8-sig")
        print(f"  Saved → {path}  ({len(df):,} rows)")
    else:
        print(f"  No data for {name}")


def get_financials_with_retry(filing, retries: int = 3, delay: float = 5.0):
    """Fetch filing financials, retrying on network timeout."""
    for attempt in range(1, retries + 1):
        try:
            obj = filing.obj()
            return obj.financials
        except Exception as e:
            if "Timeout" in type(e).__name__ or "Timeout" in str(e):
                if attempt < retries:
                    print(f"  Timeout on attempt {attempt}/{retries}, retrying in {delay}s...")
                    time.sleep(delay)
                    delay *= 2
                else:
                    raise
            else:
                raise


def save_filing_financials(financials, label: str, ticker: str, output_dir: Path) -> None:
    """Save income statement, balance sheet, and cash flow from a Financials object."""
    print(f"\n[{label}]")
    save_statement(
        "income_statement",
        financials.income_statement(),
        output_dir / f"{ticker}_{label}_income_statement.csv",
    )
    save_statement(
        "balance_sheet",
        financials.balance_sheet(),
        output_dir / f"{ticker}_{label}_balance_sheet.csv",
    )
    save_statement(
        "cash_flow_statement",
        financials.cashflow_statement(),
        output_dir / f"{ticker}_{label}_cash_flow_statement.csv",
    )


def extract_and_save_financials(ticker: str, base_output_dir: Path) -> None:
    print(f"\nFetching financials for {ticker} ...")

    try:
        company = Company(ticker)
        ticker_str = company.tickers[0] if company.tickers else ticker
        print(f"Company: {company.name} ({ticker_str}) – CIK: {company.cik}")

        # Output folder named after the ticker
        ticker_dir = base_output_dir / ticker_str
        raw_dir = ticker_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        print(f"Output folder: {ticker_dir.resolve()}")

        # ── 1. 10-K filings – last 4 years ───────────────────────────────────
        tenk_filings_all = company.get_filings(form="10-K")
        tenk_df = tenk_filings_all.to_pandas()

        if tenk_df.empty:
            print("No 10-K filings found.", file=sys.stderr)
            sys.exit(1)

        tenk_df["reportDate"] = pd.to_datetime(tenk_df["reportDate"])
        tenk_latest = tenk_df.sort_values("reportDate", ascending=False).head(4)

        print(f"\nFound {len(tenk_latest)} 10-K filing(s) (up to 4 years):")
        for idx, row in tenk_latest.sort_values("reportDate").iterrows():
            period = row["reportDate"].strftime("%Y-%m-%d")
            print(f"  10-K period: {period}  (filed: {row.get('filing_date', 'unknown')})")
            filing = tenk_filings_all.get_filing_at(idx)
            label = f"10-K_{period}"
            try:
                save_filing_financials(get_financials_with_retry(filing), label, ticker_str, raw_dir)
            except Exception as e:
                print(f"  Skipping {label}: {e.__class__.__name__}: {e}", file=sys.stderr)

        # ── 2. 10-Q filings – latest 3 ───────────────────────────────────────
        tenq_filings_all = company.get_filings(form="10-Q")
        tenq_df = tenq_filings_all.to_pandas()

        if tenq_df.empty:
            print("\nNo 10-Q filings found – done.")
        else:
            tenq_df["reportDate"] = pd.to_datetime(tenq_df["reportDate"])
            tenq_latest = tenq_df.sort_values("reportDate", ascending=False).head(3)

            print(f"\nFound {len(tenq_latest)} 10-Q filing(s) (latest 3):")
            for idx, row in tenq_latest.sort_values("reportDate").iterrows():
                period = row["reportDate"].strftime("%Y-%m-%d")
                print(f"  10-Q period: {period}  (filed: {row.get('filing_date', 'unknown')})")
                filing = tenq_filings_all.get_filing_at(idx)
                label = f"10-Q_{period}"
                try:
                    save_filing_financials(get_financials_with_retry(filing), label, ticker_str, raw_dir)
                except Exception as e:
                    print(f"  Skipping {label}: {e.__class__.__name__}: {e}", file=sys.stderr)

        print(f"\nDone. All files saved to: {ticker_dir.resolve()}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error processing {ticker}: {e.__class__.__name__}: {e}", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract SEC financial statements")
    parser.add_argument("ticker", type=str.upper, help="Company ticker symbol (e.g. AAPL)")
    parser.add_argument(
        "--output-dir", "-o",
        type=Path,
        default=DEFAULT_RESEARCH_DIR,
        help="Base folder; a subfolder named after the ticker will be created inside",
    )

    args = parser.parse_args()
    ensure_identity()
    extract_and_save_financials(args.ticker, args.output_dir)

    # Run combine_financials.py on the output directory
    script_dir = Path(__file__).resolve().parent
    combine_script = script_dir / "sec_financials" / "combine_financials.py"
    output_dir = args.output_dir.resolve()
    if combine_script.exists():
        print(f"\nRunning combine_financials.py for {args.ticker} ...")
        result = subprocess.run(
            [sys.executable, str(combine_script), args.ticker],
            cwd=str(output_dir),
        )
        if result.returncode != 0:
            print("Warning: combine_financials.py exited with errors.", file=sys.stderr)
    else:
        print(f"\nNote: {combine_script} not found – skipping combine step.")

    # Run extract_tikr_estimates.py for forward analyst estimates
    tikr_script = script_dir / "extract_tikr_estimates.py"
    if tikr_script.exists():
        print(f"\n{'='*60}")
        print(f"Running TIKR forward estimates extraction for {args.ticker} ...")
        print(f"{'='*60}")
        result = subprocess.run(
            [sys.executable, str(tikr_script), args.ticker],
            cwd=str(script_dir),
        )
        if result.returncode != 0:
            print("Warning: extract_tikr_estimates.py exited with errors.", file=sys.stderr)
            print("Hint: Run 'python extract_tikr_estimates.py --login' first if you haven't logged in yet.")
    else:
        print(f"\nNote: {tikr_script} not found – skipping TIKR estimates step.")


if __name__ == "__main__":
    main()
