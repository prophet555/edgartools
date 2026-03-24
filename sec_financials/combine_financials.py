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
    # Each filter column is only applied if it exists in the CSV
    mask = pd.Series(True, index=df.index)
    for col in ["abstract", "dimension", "is_breakdown"]:
        if col in df.columns:
            mask = mask & (df[col].astype(str) == "False")
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

    Strategy: most-recent filing is the authoritative base.
    - Canonical rows come exclusively from the most recent filing (no orphan rows
      from older filings appended at the bottom).
    - Prior filings contribute only date columns not already covered by any more-
      recent filing ("unique" periods).
    - For each historical period, values are matched to canonical rows by:
        1. Exact (concept, label) match
        2. Label-only fallback (case-insensitive) when the label is unique within
           that filing — handles concept renames between filings.
    - The most recent filing's own date columns are preserved exactly as-is.
    """
    if not file_list:
        return None

    # Sort files by the latest date column (oldest filing first, most recent last)
    file_dates = []
    for f in file_list:
        df_tmp = pd.read_csv(f, index_col=0, nrows=0)
        dates = get_date_columns(df_tmp)
        max_date = max(dates) if dates else ""
        file_dates.append((max_date, f))
    file_dates.sort()

    most_recent_file = file_dates[-1][1]

    # Canonical rows and date columns come from the most recent filing
    canonical_df = load_and_filter(most_recent_file)[["concept", "label"]]
    recent_df = load_and_filter(most_recent_file)
    recent_dates = get_date_columns(recent_df)
    claimed_dates = set(recent_dates)

    # Build per-filing lookup tables for their unique historical periods only.
    # Walk from most-recent to oldest so "claimed" accumulates correctly.
    # historical_lookups: list of (unique_dates, exact_lookup, label_lookup)
    #   exact_lookup[(concept, label)][date] = value
    #   label_lookup[normalized_label][date]  = value  (only for unique labels)
    historical_lookups = []

    for _, f in reversed(file_dates[:-1]):  # skip most recent
        df = load_and_filter(f)
        date_cols = get_date_columns(df)
        unique_dates = [d for d in date_cols if d not in claimed_dates]
        if not unique_dates:
            continue
        claimed_dates.update(unique_dates)

        exact_lookup = {}   # (concept, label) -> {date: value}
        label_counts = {}   # normalized_label -> count of rows with that label
        label_lookup = {}   # normalized_label -> {date: value}

        for _, row in df.iterrows():
            key = (row["concept"], row["label"])
            norm = row["label"].strip().lower()
            label_counts[norm] = label_counts.get(norm, 0) + 1
            for date in unique_dates:
                val = row[date]
                if pd.notna(val):
                    exact_lookup.setdefault(key, {})[date] = val
                    label_lookup.setdefault(norm, {})[date] = val

        # Remove ambiguous labels (appeared more than once in this filing)
        label_lookup = {
            norm: dates for norm, dates in label_lookup.items()
            if label_counts[norm] == 1
        }

        historical_lookups.append((unique_dates, exact_lookup, label_lookup))

    # Collect all historical unique dates in chronological order
    all_historical_dates = sorted(
        date for unique_dates, _, _ in historical_lookups for date in unique_dates
    )

    # Build output: canonical rows + most-recent columns (as-is) + historical columns
    result = canonical_df.reset_index(drop=True).copy()

    # Most recent filing's date columns — copied directly, no modification
    for date in recent_dates:
        result[date] = recent_df[date].values

    # Historical columns — filled via exact then label fallback
    for date in all_historical_dates:
        values = []
        for _, row in result.iterrows():
            key = (row["concept"], row["label"])
            norm = row["label"].strip().lower()
            val = None
            for unique_dates, exact_lookup, label_lookup in historical_lookups:
                if date not in unique_dates:
                    continue
                # Exact match
                val = exact_lookup.get(key, {}).get(date)
                if val is None:
                    # Label fallback
                    val = label_lookup.get(norm, {}).get(date)
                break
            values.append(val)
        result[date] = values

    # Reorder columns: concept, label, then all dates chronologically
    all_dates = sorted(recent_dates + all_historical_dates)
    return result[["concept", "label"] + all_dates]


def main():
    if len(sys.argv) < 2:
        print("Usage: python combine_financials.py <TICKER>")
        print("  Example: python combine_financials.py MELI")
        sys.exit(1)

    ticker = sys.argv[1].upper()

    # When called from extract_sec.py, cwd is the base research dir;
    # look for <TICKER>/raw/ for source files, write to <TICKER>/combined/
    cwd = os.getcwd()
    ticker_dir = os.path.join(cwd, ticker)
    raw_dir = os.path.join(ticker_dir, "raw")

    if not os.path.isdir(raw_dir):
        print(f"Error: raw folder not found: {raw_dir}")
        sys.exit(1)

    combined_dir = os.path.join(ticker_dir, "combined")
    os.makedirs(combined_dir, exist_ok=True)

    print(f"Processing ticker: {ticker}")
    print(f"Input folder:  {raw_dir}")
    print(f"Output folder: {combined_dir}")

    for filing_type in FILING_TYPES:
        for stmt_key, stmt_name in STATEMENT_TYPES.items():
            pattern = os.path.join(
                raw_dir, f"{ticker}_{filing_type}_*_{stmt_name}.csv"
            )
            files = sorted(glob.glob(pattern))

            if not files:
                print(f"\n  No files found for {filing_type} {stmt_name} — skipping")
                continue

            print(f"\n  {filing_type} {stmt_name}:")
            for f in files:
                print(f"    {os.path.basename(f)}")

            combined = combine_files(files)
            if combined is not None:
                out_name = f"{ticker}_{filing_type.replace('-', '')}_{stmt_key}_combined.csv"
                out_path = os.path.join(combined_dir, out_name)
                combined.to_csv(out_path, index=False)
                print(f"    -> {out_name} ({len(combined)} rows, {len(get_date_columns(combined))} periods)")


if __name__ == "__main__":
    main()
