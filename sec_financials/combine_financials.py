"""
Combine SEC financial CSVs into consolidated files for financial modeling.

Usage:
    python combine_financials.py <TICKER>

    Example:
        python combine_financials.py MELI
        python combine_financials.py ZS

The script looks for CSV files in a child folder named after the ticker:
    <script_dir>/<TICKER>/<TICKER>_10-K_*_<statement>.csv
    <script_dir>/<TICKER>/<TICKER>_10-Q_*_<statement>.csv

Produces 6 output files written to the same child folder:
    <TICKER>_10K_income_statement_combined.csv
    <TICKER>_10K_balance_sheet_combined.csv
    <TICKER>_10K_cash_flow_statement_combined.csv
    <TICKER>_10Q_income_statement_combined.csv
    <TICKER>_10Q_balance_sheet_combined.csv
    <TICKER>_10Q_cash_flow_statement_combined.csv

Each output has: concept, label, and one column per period date with values.
Only consolidated (non-dimensional, non-breakdown, non-abstract) line items are kept.
"""

import sys
import pandas as pd
import glob
import os
import re

FILING_TYPES = ["10-K", "10-Q"]
STATEMENT_TYPES = {
    "income_statement": "income_statement",
    "balance_sheet": "balance_sheet",
    "cash_flow_statement": "cash_flow_statement",
}


def get_date_columns(df):
    """Return columns that look like date periods (YYYY-MM-DD)."""
    return [c for c in df.columns if re.match(r"^\d{4}-\d{2}-\d{2}$", c)]


def load_and_filter(filepath):
    """Load a CSV and filter to consolidated line items only."""
    df = pd.read_csv(filepath, index_col=0)

    # Filter: non-abstract, non-dimensional, non-breakdown rows
    mask = (
        (df["abstract"].astype(str) == "False")
        & (df["dimension"].astype(str) == "False")
        & (df["is_breakdown"].astype(str) == "False")
    )
    df = df[mask].copy()

    date_cols = get_date_columns(df)
    keep_cols = ["concept", "label"] + date_cols
    df = df[keep_cols].copy()

    # Convert date columns to numeric
    for col in date_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def combine_files(file_list):
    """Combine multiple filing CSVs into one wide dataframe.

    Uses the row order from the most recent filing as the canonical order,
    then appends any concepts found only in older filings at the end.
    Uses concept+label as a composite key to handle duplicate concepts.
    """
    if not file_list:
        return None

    # Sort files by the latest date column (most recent filing last)
    file_dates = []
    for f in file_list:
        df_tmp = pd.read_csv(f, index_col=0, nrows=0)
        dates = get_date_columns(df_tmp)
        max_date = max(dates) if dates else ""
        file_dates.append((max_date, f))
    file_dates.sort()

    # Load all files, collect date data keyed by (concept, label)
    # date_data[date_col] = {(concept, label): value}
    date_data = {}
    for _, f in file_dates:
        df = load_and_filter(f)
        date_cols = get_date_columns(df)
        for _, row in df.iterrows():
            key = (row["concept"], row["label"])
            for col in date_cols:
                if col not in date_data:
                    date_data[col] = {}
                val = row[col]
                if pd.notna(val):
                    date_data[col][key] = val

    # Use the most recent filing's row order as canonical
    most_recent_file = file_dates[-1][1]
    canonical_df = load_and_filter(most_recent_file)[["concept", "label"]]
    canonical_keys = list(zip(canonical_df["concept"], canonical_df["label"]))

    # Collect all keys across all periods
    all_keys = set()
    for col_data in date_data.values():
        all_keys.update(col_data.keys())

    # Find keys not in canonical order (from older filings only)
    canonical_set = set(canonical_keys)
    extra_keys = [k for k in all_keys if k not in canonical_set]

    if extra_keys:
        extra_df = pd.DataFrame(extra_keys, columns=["concept", "label"])
        canonical_df = pd.concat([canonical_df, extra_df], ignore_index=True)

    # Sort date columns chronologically
    all_dates = sorted(date_data.keys())

    # Build result
    result = canonical_df.copy()
    for date_col in all_dates:
        col_data = date_data[date_col]
        result[date_col] = [
            col_data.get((row["concept"], row["label"]), None)
            for _, row in result.iterrows()
        ]

    return result


def main():
    if len(sys.argv) < 2:
        print("Usage: python combine_financials.py <TICKER>")
        print("  Example: python combine_financials.py MELI")
        sys.exit(1)

    ticker = sys.argv[1].upper()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    ticker_dir = os.path.join(script_dir, f"{ticker}_SEC")

    if not os.path.isdir(ticker_dir):
        print(f"Error: folder not found: {ticker_dir}")
        sys.exit(1)

    print(f"Processing ticker: {ticker}")
    print(f"Input/output folder: {ticker_dir}")

    # Track all source files so we can move them into raw/ afterwards
    all_source_files = []

    for filing_type in FILING_TYPES:
        for stmt_key, stmt_name in STATEMENT_TYPES.items():
            pattern = os.path.join(
                ticker_dir, f"{ticker}_{filing_type}_*_{stmt_name}.csv"
            )
            files = sorted(glob.glob(pattern))

            if not files:
                print(f"\n  No files found for {filing_type} {stmt_name} — skipping")
                continue

            all_source_files.extend(files)

            print(f"\n  {filing_type} {stmt_name}:")
            for f in files:
                print(f"    {os.path.basename(f)}")

            combined = combine_files(files)
            if combined is not None:
                out_name = f"{ticker}_{filing_type.replace('-', '')}_{stmt_key}_combined.csv"
                out_path = os.path.join(ticker_dir, out_name)
                combined.to_csv(out_path, index=False)
                print(f"    -> {out_name} ({len(combined)} rows, {len(get_date_columns(combined))} periods)")

    # Move individual source CSVs into a raw/ subfolder
    if all_source_files:
        raw_dir = os.path.join(ticker_dir, "raw")
        os.makedirs(raw_dir, exist_ok=True)
        for f in all_source_files:
            dest = os.path.join(raw_dir, os.path.basename(f))
            os.rename(f, dest)
        print(f"\n  Moved {len(all_source_files)} source file(s) into {raw_dir}")


if __name__ == "__main__":
    main()
