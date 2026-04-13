#!/usr/bin/env python3
"""
Equities research pipeline — routes to the right data source automatically.

US tickers (no --exchange):
    1. extract_sec.py          — EDGAR 10-K/10-Q actuals + TIKR estimates (chained inside)
    2. extract_tikr_financials.py — TIKR 5-year historical financials
    3. extract_tikr_filings.py — download 10-K/10-Q PDFs from TIKR

Foreign tickers (--exchange provided):
    1. extract_tikr_financials.py — TIKR historical financials
    2. extract_tikr_estimates.py  — TIKR forward estimates
    3. extract_tikr_filings.py    — download filings PDFs from TIKR

Usage:
    python run_pipeline.py GS                            # US ticker
    python run_pipeline.py MC --exchange ENXT            # Foreign ticker
    python run_pipeline.py DGE --exchange LSE --visible  # Foreign, visible browser
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

VENV_PYTHON = Path(__file__).resolve().parent / ".venv" / "bin" / "python"
SCRIPTS_DIR  = Path(__file__).resolve().parent
TIKR_PROFILE_DIR = Path.home() / ".tikr-playwright-session"

TIKR_SCRIPTS = {
    "extract_tikr_financials.py",
    "extract_tikr_estimates.py",
    "extract_tikr_filings.py",
    "extract_tikr_transcripts.py",
}


def kill_tikr_chrome() -> None:
    """Kill any Chrome processes that are holding the TIKR persistent profile."""
    subprocess.run(
        ["pkill", "-f", str(TIKR_PROFILE_DIR)],
        stderr=subprocess.DEVNULL,
    )
    time.sleep(2)


# Steps that are optional — pipeline continues even if they fail
OPTIONAL_SCRIPTS = {"extract_tikr_financials.py"}


def run(cmd: list[str], label: str, delay: int = 0) -> bool:
    if delay:
        print(f"\nWaiting {delay}s before {label} ...")
        time.sleep(delay)
    # Kill any lingering Chrome holding the TIKR profile before each TIKR step
    script_name = Path(cmd[1]).name if len(cmd) > 1 else ""
    if script_name in TIKR_SCRIPTS:
        kill_tikr_chrome()
    print(f"\n── {label} ──")
    result = subprocess.run(cmd, cwd=SCRIPTS_DIR)
    if result.returncode != 0:
        if script_name in OPTIONAL_SCRIPTS:
            print(f"⚠ {label} failed (exit {result.returncode}) — skipping (optional)", file=sys.stderr)
            return True
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
        "--url",
        default=None,
        help="TIKR URL for this company (any tab) — passed to TIKR scripts so they "
             "skip the unreliable search and navigate directly via cid/tid.",
    )
    parser.add_argument(
        "--filings-delay",
        type=int,
        default=120,
        metavar="SECONDS",
        help="Seconds to wait before running the filings download step (default: 120).",
    )
    args = parser.parse_args()

    py = str(VENV_PYTHON)
    visible = ["--visible"] if args.visible else []
    url_arg = ["--url", args.url] if args.url else []

    if args.exchange:
        # ── Foreign ticker: TIKR for both actuals and estimates ────────────
        print(f"Pipeline: {args.ticker} [{args.exchange}] — source: TIKR")
        exch = ["--exchange", args.exchange]
        steps = [
            (
                [py, str(SCRIPTS_DIR / "extract_tikr_financials.py"), args.ticker] + exch + url_arg + visible,
                f"TIKR Financials — {args.ticker}",
                0,
            ),
            (
                [py, str(SCRIPTS_DIR / "extract_tikr_estimates.py"), args.ticker] + exch + url_arg + visible,
                f"TIKR Estimates — {args.ticker}",
                0,
            ),
            (
                [py, str(SCRIPTS_DIR / "build_model.py"), args.ticker],
                f"Build Model — {args.ticker}",
                0,
            ),
            (
                [py, str(SCRIPTS_DIR / "extract_tikr_filings.py"), args.ticker] + exch + url_arg + visible,
                f"TIKR Filings — {args.ticker}",
                args.filings_delay,
            ),
            (
                [py, str(SCRIPTS_DIR / "extract_tikr_transcripts.py"), args.ticker] + exch + url_arg + visible,
                f"TIKR Transcripts — {args.ticker}",
                60,
            ),
        ]
    else:
        # ── US ticker: SEC EDGAR actuals + TIKR financials + model + filings ──
        print(f"Pipeline: {args.ticker} — source: SEC EDGAR + TIKR")
        steps = [
            (
                [py, str(SCRIPTS_DIR / "extract_sec.py"), args.ticker] + url_arg,
                f"SEC + TIKR — {args.ticker}",
                0,
            ),
            (
                [py, str(SCRIPTS_DIR / "extract_tikr_financials.py"), args.ticker] + url_arg + visible,
                f"TIKR Financials — {args.ticker}",
                0,
            ),
            (
                [py, str(SCRIPTS_DIR / "build_model.py"), args.ticker],
                f"Build Model — {args.ticker}",
                0,
            ),
            (
                [py, str(SCRIPTS_DIR / "extract_tikr_filings.py"), args.ticker] + url_arg + visible,
                f"TIKR Filings — {args.ticker}",
                args.filings_delay,
            ),
            (
                [py, str(SCRIPTS_DIR / "extract_tikr_transcripts.py"), args.ticker] + url_arg + visible,
                f"TIKR Transcripts — {args.ticker}",
                60,
            ),
        ]

    ok = all(run(cmd, label, delay) for cmd, label, delay in steps)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
