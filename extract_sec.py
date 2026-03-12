#!/usr/bin/env python3
"""
Extract financial statements from SEC EDGAR using edgartools.

- Fetches the latest 10-K (income statement, balance sheet, cash flow)
- If 10-Qs exist after the 10-K period end, also fetches those 3 statements
  for each 10-Q in the current fiscal year

Usage:
    python extract_sec.py AAPL
    python extract_sec.py TSLA --output-dir ./financials

Requirements:
    pip install edgartools pandas
"""

import os
import sys
import argparse
from pathlib import Path

import pandas as pd
from edgar import Company, set_identity, get_identity


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
    df = stmt.to_dataframe()
    if df is not None and not df.empty:
        df.to_csv(path, encoding="utf-8-sig")
        print(f"  Saved → {path}  ({len(df):,} rows)")
    else:
        print(f"  No data for {name}")


def save_filing_financials(financials, label: str, ticker: str, output_dir: Path) -> None:
    """Save all 3 statements from a Financials object."""
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


def extract_and_save_financials(ticker: str, output_dir: Path) -> None:
    print(f"\nFetching financials for {ticker} ...")

    try:
        company = Company(ticker)
        ticker_str = company.tickers[0] if company.tickers else ticker
        print(f"Company: {company.name} ({ticker_str}) – CIK: {company.cik}")

        # ── 1. Latest 10-K ────────────────────────────────────────────────────
        tenk_filing = company.get_filings(form="10-K").latest(1)
        if tenk_filing is None:
            print("No 10-K filings found.", file=sys.stderr)
            sys.exit(1)

        tenk_period = tenk_filing.period_of_report   # e.g. 2025-05-31
        tenk_label  = f"10-K_{tenk_period}"
        print(f"\nLatest 10-K period: {tenk_period} (filed: {tenk_filing.filing_date})")

        output_dir.mkdir(parents=True, exist_ok=True)

        tenk_obj = tenk_filing.obj()
        save_filing_financials(tenk_obj.financials, tenk_label, ticker, output_dir)

        # ── 2. 10-Qs filed after the 10-K period end ─────────────────────────
        tenq_filings_all = company.get_filings(form="10-Q")
        tenq_df = tenq_filings_all.to_pandas()

        # Keep only 10-Qs whose reportDate is after the 10-K period end
        tenq_df["reportDate"] = pd.to_datetime(tenq_df["reportDate"])
        tenq_after = tenq_df[tenq_df["reportDate"] > pd.Timestamp(tenk_period)]

        if tenq_after.empty:
            print("\nNo 10-Qs found after the latest 10-K period – done.")
            return

        print(f"\nFound {len(tenq_after)} 10-Q(s) filed after {tenk_period}:")

        # Build accession → row-index map for fast lookup
        accession_to_idx = {
            row["accession_number"]: idx
            for idx, row in tenq_df.iterrows()
        }

        # Process in chronological order (oldest first)
        for _, row in tenq_after.sort_values("reportDate").iterrows():
            period = row["reportDate"].strftime("%Y-%m-%d")
            accession = row["accession_number"]
            print(f"  10-Q period: {period}")

            df_idx = accession_to_idx.get(accession)
            if df_idx is None:
                print(f"    Could not find index for {accession} – skipping.")
                continue

            tenq_filing = tenq_filings_all.get_filing_at(df_idx)
            tenq_label = f"10-Q_{period}"
            tenq_obj = tenq_filing.obj()
            save_filing_financials(tenq_obj.financials, tenq_label, ticker, output_dir)

        print("\nDone. Check the output folder for CSV files.")

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
        default=Path("./sec_financials"),
        help="Folder to save CSV files (default: ./sec_financials)",
    )

    args = parser.parse_args()
    ensure_identity()
    extract_and_save_financials(args.ticker, args.output_dir)


if __name__ == "__main__":
    main()
