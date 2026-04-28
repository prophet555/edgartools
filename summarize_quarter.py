#!/usr/bin/env python3
"""
Quarterly earnings summary for a ticker.

Pulls the latest earnings-call transcript via TIKR, then tries to pull the
matching 10-Q filing for the same quarter (falls back to transcript-only if
the 10-Q is not yet available). Files are saved to the standard OneDrive
research directory by the underlying extractors.

The script then invokes the `claude` CLI in non-interactive mode (-p) with
file paths to the transcript / 10-Q / prior transcripts; Claude reads the
documents locally and writes a markdown summary covering guidance vs reality,
KPI trends, and what changed — preceded by a headline KPI table.

Usage:
    python summarize_quarter.py CME
    python summarize_quarter.py CME --url "https://app.tikr.com/stock/...&tab=est"
    python summarize_quarter.py CME --skip-fetch  # use already-downloaded files
    python summarize_quarter.py CME --out /path/to/CME_Q126.md

Auth: uses your existing `claude` CLI OAuth login — no ANTHROPIC_API_KEY
needed. If `ANTHROPIC_API_KEY` is set in the environment, the CLI will use
that instead.
"""

import argparse
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DEFAULT_RESEARCH_DIR

SCRIPT_DIR = Path(__file__).resolve().parent
PYTHON = str(SCRIPT_DIR / ".venv" / "bin" / "python")
TRANSCRIPTS_SCRIPT = str(SCRIPT_DIR / "extract_tikr_transcripts.py")
FILINGS_SCRIPT = str(SCRIPT_DIR / "extract_tikr_filings.py")

DEFAULT_MODEL = "claude-opus-4-7"
PRIOR_TRANSCRIPTS_FOR_CONTEXT = 3  # how many prior earnings transcripts to include

EARNINGS_FILENAME_RE = re.compile(
    r"^(?P<ticker>[A-Z0-9.\-]+)_(?P<date>\d{4}-\d{2}-\d{2})_.*Q(?P<q>[1-4])\s+(?P<year>\d{4}).*Earnings Call",
    re.IGNORECASE,
)

PROMPT = """Analyze the most recent quarterly earnings using the newly uploaded documents.
Compare against prior guidance and historical results.

Begin your response with a **Headline Table** — a markdown table of the most relevant KPIs for this ticker (revenue, EPS, key volume/customer/segment metrics, guidance points). Show the current quarter, the prior-year same-quarter, the YoY % change, and a one-word trend tag (↑ / ↓ / flat). Pick the KPIs that matter most for this specific business — do not use a generic template.

After the table, produce the following sections:

## Section 1: Guidance vs Reality
- What management previously guided -> cite prior filing
- What actually happened -> cite new report
- Beat / Met / Missed - quantify the delta
- Management's explanation -> exact quote
- Has this explanation been used before? Check prior transcripts.
- Judgment: is the explanation supported by data or hand-waving?

## Section 2: KPI Trends (Year-over-Year)
For each key KPI:
- Current quarter value (cite source)
- Same quarter last year (cite source)
- Absolute and percentage change
- What the trend signals
- Whether it supports or contradicts management's narrative

## Section 3: What Actually Changed?
- What materially improved vs last year?
- What materially deteriorated?
- What is new (strategy shifts, pricing changes, restructuring)?
- What did NOT change despite management emphasis?

## Summary
- Execution vs expectations: Improving / Stable / Deteriorating
- Management credibility: Strong / Mixed / Weak
- Business momentum: Accelerating / Steady / Decelerating

Cite documents as [Q1 2026 transcript], [10-Q FY26Q1], [Q1 2025 transcript], etc. Quote exactly when attributing statements to management."""


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def parse_period_from_transcript_name(filename: str) -> tuple[int, int, str] | None:
    """Return (year, quarter, iso_date) for an earnings call transcript filename."""
    m = EARNINGS_FILENAME_RE.search(filename)
    if not m:
        return None
    return int(m.group("year")), int(m.group("q")), m.group("date")


def find_latest_earnings_transcript(transcripts_dir: Path) -> Path | None:
    candidates = []
    for f in transcripts_dir.glob("*.txt"):
        parsed = parse_period_from_transcript_name(f.name)
        if parsed:
            candidates.append((parsed[2], parsed[0], parsed[1], f))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][3]


def find_prior_earnings_transcripts(transcripts_dir: Path, latest: Path, n: int) -> list[Path]:
    candidates = []
    for f in transcripts_dir.glob("*.txt"):
        if f == latest:
            continue
        parsed = parse_period_from_transcript_name(f.name)
        if parsed:
            candidates.append((parsed[2], f))
    candidates.sort(reverse=True)
    return [f for _, f in candidates[:n]]


def expected_10q_period(call_year: int, call_quarter: int, fiscal_year_end_month: int = 12) -> str:
    """For calendar-year filers, Q1 call (April) reports period ending Mar 31 of that year."""
    quarter_end_month = {1: 3, 2: 6, 3: 9, 4: 12}[call_quarter]
    last_day = {3: 31, 6: 30, 9: 30, 12: 31}[quarter_end_month]
    return f"{call_year}-{quarter_end_month:02d}-{last_day:02d}"


def find_matching_10q(filings_dir: Path, call_year: int, call_quarter: int) -> Path | None:
    target_period = expected_10q_period(call_year, call_quarter)
    # Try exact match first
    for ext in ("pdf", "html", "htm", "txt"):
        p = filings_dir / f"*_10-Q_{target_period}.{ext}"
        matches = list(filings_dir.glob(p.name))
        if matches:
            return matches[0]
    # Fallback: any 10-Q whose period date is within ~120 days of the call year/quarter
    target_dt = datetime.fromisoformat(target_period)
    best = None
    best_delta = None
    for f in filings_dir.glob("*_10-Q_*"):
        m = re.search(r"_10-Q_(\d{4}-\d{2}-\d{2})", f.name)
        if not m:
            continue
        try:
            dt = datetime.fromisoformat(m.group(1))
        except ValueError:
            continue
        delta = abs((dt - target_dt).days)
        if delta <= 45 and (best_delta is None or delta < best_delta):
            best = f
            best_delta = delta
    return best


# ---------------------------------------------------------------------------
# Extractor invocation
# ---------------------------------------------------------------------------

def run_extractor(args: list[str]) -> None:
    print(f"\n→ {' '.join(args)}", flush=True)
    result = subprocess.run(args, check=False)
    if result.returncode != 0:
        print(f"  Warning: extractor exited with code {result.returncode}", file=sys.stderr)


def fetch_latest(ticker: str, url: str | None, fetch_filings: bool) -> None:
    transcript_args = [PYTHON, TRANSCRIPTS_SCRIPT, ticker, "--limit", "1", "--earnings-only"]
    if url:
        transcript_args.extend(["--url", url])
    run_extractor(transcript_args)

    if fetch_filings:
        filings_args = [PYTHON, FILINGS_SCRIPT, ticker, "--forms", "10-Q", "--limit", "1"]
        if url:
            filings_args.extend(["--url", url])
        run_extractor(filings_args)


# ---------------------------------------------------------------------------
# Claude call
# ---------------------------------------------------------------------------

def build_prompt(
    ticker: str,
    latest_transcript: Path,
    latest_10q: Path | None,
    prior_transcripts: list[Path],
) -> str:
    lines = [
        f"You are analyzing the latest quarterly earnings for {ticker}.",
        "",
        "Read the following documents (use your Read tool — they are absolute paths):",
        "",
        f"LATEST EARNINGS CALL TRANSCRIPT: {latest_transcript}",
    ]
    if latest_10q:
        lines.append(f"MATCHING 10-Q FILING: {latest_10q}")
    else:
        lines.append("No matching 10-Q yet — analysis will rely on the transcript only.")
    if prior_transcripts:
        lines.append("")
        lines.append("PRIOR EARNINGS CALL TRANSCRIPTS (for the 'has this explanation been used before' check):")
        for p in prior_transcripts:
            lines.append(f"  - {p}")
    lines.append("")
    lines.append("After reading them, output the analysis described below as raw markdown — do not preface, do not wrap in code fences, do not add any commentary about what you are about to do. Output only the markdown.")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append(PROMPT)
    return "\n".join(lines)


def call_claude_cli(prompt: str, model: str, doc_paths: list[Path]) -> str:
    if not shutil.which("claude"):
        print("Error: `claude` CLI not on PATH. Install Claude Code or set ANTHROPIC_API_KEY and use the SDK.", file=sys.stderr)
        sys.exit(1)
    add_dirs: list[str] = []
    seen = set()
    for p in doc_paths:
        d = str(p.parent)
        if d not in seen:
            seen.add(d)
            add_dirs.extend(["--add-dir", d])
    cmd = [
        "claude", "-p", prompt,
        "--model", model,
        "--allowedTools", "Read",
        *add_dirs,
    ]
    print(f"\nInvoking: claude -p ... --model {model}", flush=True)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"claude CLI failed (exit {result.returncode}):\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

RESEARCH_OBSIDIAN_BASE = Path("/Users/mh/Documents/agcm2/03_eq/0_research")


def find_obsidian_earnings_dir(ticker: str) -> Path:
    """
    Resolve the Earnings folder for a ticker inside the obsidian research vault.
    Search order:
      1. <base>/<theme>/<TICKER>/Earnings  (any themed subfolder)
      2. <base>/<theme>/<TICKER>           (themed subfolder, no Earnings yet — create it)
      3. <base>/<TICKER>/Earnings          (fallback flat layout)
    """
    if RESEARCH_OBSIDIAN_BASE.exists():
        for theme_dir in RESEARCH_OBSIDIAN_BASE.iterdir():
            if not theme_dir.is_dir():
                continue
            ticker_dir = theme_dir / ticker
            if ticker_dir.is_dir():
                earnings = ticker_dir / "Earnings"
                earnings.mkdir(parents=True, exist_ok=True)
                return earnings
    return RESEARCH_OBSIDIAN_BASE / ticker / "Earnings"


def default_output_path(ticker: str, transcript: Path) -> Path:
    parsed = parse_period_from_transcript_name(transcript.name)
    if parsed:
        year, quarter, _ = parsed
        slug = f"Q{quarter}{str(year)[-2:]}"
    else:
        slug = datetime.now().strftime("%Y%m%d")
    earnings_dir = find_obsidian_earnings_dir(ticker)
    return earnings_dir / f"{slug} {ticker} earnings.md"


def main() -> None:
    ap = argparse.ArgumentParser(description="Summarize the latest quarter for a ticker via TIKR + Claude")
    ap.add_argument("ticker", type=str.upper)
    ap.add_argument("--url", default=None, help="TIKR URL for the company (any tab)")
    ap.add_argument("--out", type=Path, default=None, help="Markdown output path")
    ap.add_argument("--skip-fetch", action="store_true", help="Skip TIKR downloads; use existing local files")
    ap.add_argument("--transcripts-only", action="store_true", help="Skip 10-Q lookup")
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--prior", type=int, default=PRIOR_TRANSCRIPTS_FOR_CONTEXT,
                    help="Number of prior earnings transcripts to include for context")
    args = ap.parse_args()

    ticker = args.ticker
    base = DEFAULT_RESEARCH_DIR / ticker
    transcripts_dir = base / "transcripts"
    filings_dir = base / "filings"

    if not args.skip_fetch:
        fetch_latest(ticker, args.url, fetch_filings=not args.transcripts_only)

    latest_transcript = find_latest_earnings_transcript(transcripts_dir)
    if not latest_transcript:
        print(f"Error: no earnings transcript found in {transcripts_dir}", file=sys.stderr)
        sys.exit(1)
    print(f"\nLatest transcript: {latest_transcript.name}")

    parsed = parse_period_from_transcript_name(latest_transcript.name)
    latest_10q = None
    if parsed and not args.transcripts_only and filings_dir.exists():
        year, quarter, _ = parsed
        latest_10q = find_matching_10q(filings_dir, year, quarter)
        if latest_10q:
            print(f"Matching 10-Q:     {latest_10q.name}")
        else:
            print("No matching 10-Q for this quarter (transcript only).")

    prior = find_prior_earnings_transcripts(transcripts_dir, latest_transcript, args.prior)
    if prior:
        print(f"Prior transcripts: {[p.name for p in prior]}")

    out_path = args.out or default_output_path(ticker, latest_transcript)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    prompt = build_prompt(ticker, latest_transcript, latest_10q, prior)
    doc_paths = [latest_transcript] + ([latest_10q] if latest_10q else []) + prior
    summary = call_claude_cli(prompt, args.model, doc_paths)

    out_path.write_text(summary, encoding="utf-8")
    print(f"\nSummary saved → {out_path}")


if __name__ == "__main__":
    main()
