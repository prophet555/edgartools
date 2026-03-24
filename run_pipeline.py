#!/usr/bin/env python3
"""
Equities research pipeline — routes to the right data source automatically.

By default, only gathers data (financials + estimates). Pass --build-model to
also build the Excel model after gathering.

US tickers (no --exchange):
    extract_sec.py TICKER
    └── EDGAR 10-K/10-Q actuals + TIKR forward estimates (already chained inside)

Foreign tickers (--exchange provided):
    extract_tikr_financials.py TICKER --exchange EXCHANGE
    extract_tikr_estimates.py  TICKER --exchange EXCHANGE

Usage:
    python run_pipeline.py GS                            # US — gather data only
    python run_pipeline.py GS --build-model              # US — gather data + build model
    python run_pipeline.py MC --exchange ENXT            # Foreign — gather data only
    python run_pipeline.py DGE --exchange LSE --build-model
"""

import argparse
import subprocess
import sys
from pathlib import Path

VENV_PYTHON = Path(__file__).resolve().parent / ".venv" / "bin" / "python"
SCRIPTS_DIR  = Path(__file__).resolve().parent


def run(cmd: list[str], label: str) -> bool:
    print(f"\n── {label} ──")
    result = subprocess.run(cmd, cwd=SCRIPTS_DIR)
    if result.returncode != 0:
        print(f"✗ {label} failed (exit {result.returncode})", file=sys.stderr)
        return False
    print(f"✓ {label} done")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Equities research pipeline")
    parser.add_argument("ticker", type=str.upper, help="Ticker symbol (e.g. GS, MC, DGE)")
    parser.add_argument(
        "--exchange", "-e",
        default=None,
        help="Exchange for foreign/ambiguous tickers (e.g. ENXT, LSE, TSX). "
             "Omit for US-listed tickers — SEC EDGAR will be used.",
    )
    parser.add_argument(
        "--visible", action="store_true",
        help="Run browser visibly (TIKR steps only, for debugging)",
    )
    parser.add_argument(
        "--build-model", action="store_true",
        help="Also build the Excel model after gathering data (default: data only)",
    )
    args = parser.parse_args()

    py = str(VENV_PYTHON)
    visible = ["--visible"] if args.visible else []

    build_model_cmd = [py, str(SCRIPTS_DIR / "build_model.py"), args.ticker]

    if args.exchange:
        # ── Foreign ticker: TIKR for both actuals and estimates ────────────
        print(f"Pipeline: {args.ticker} [{args.exchange}] — source: TIKR")
        exch = ["--exchange", args.exchange]
        steps = [
            (
                [py, str(SCRIPTS_DIR / "extract_tikr_financials.py"), args.ticker] + exch + visible,
                f"TIKR Financials — {args.ticker}",
            ),
            (
                [py, str(SCRIPTS_DIR / "extract_tikr_estimates.py"), args.ticker] + exch + visible,
                f"TIKR Estimates — {args.ticker}",
            ),
        ]
    else:
        # ── US ticker: SEC EDGAR actuals + TIKR estimates (chained inside extract_sec.py) ──
        print(f"Pipeline: {args.ticker} — source: SEC EDGAR + TIKR")
        steps = [
            (
                [py, str(SCRIPTS_DIR / "extract_sec.py"), args.ticker],
                f"SEC + TIKR — {args.ticker}",
            ),
        ]

    if args.build_model:
        steps.append((build_model_cmd, f"Build Model — {args.ticker}"))

    ok = all(run(cmd, label) for cmd, label in steps)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
