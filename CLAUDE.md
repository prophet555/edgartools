# Project Notes

- Run all scripts in venv: `/Users/mh/Dev/edgartools/.venv/bin/python`

## Telegram Commands

### `/getdata TICKER [EXCHANGE]`

Gathers data only (SEC financials + TIKR estimates). Does NOT build the Excel model.

**Exchange provided** → run pipeline immediately, no questions asked:
```
/Users/mh/Dev/edgartools/.venv/bin/python /Users/mh/Dev/edgartools/run_pipeline.py TICKER --exchange EXCHANGE
```
Reply with a brief status: what ran, where files were saved, any errors.

**No exchange provided** → two-step flow:

1. Run the TIKR search to find matching listings:
   ```
   /Users/mh/Dev/edgartools/.venv/bin/python /Users/mh/Dev/edgartools/search_tikr.py TICKER
   ```
   This prints a JSON list of `{"text": "...", "exchange": "..."}` objects.

2. Decide based on the results:
   - **0 or 1 result, or only US exchanges (NYSE/NASDAQ/AMEX)** → run the US pipeline directly (no `--exchange` flag needed):
     ```
     /Users/mh/Dev/edgartools/.venv/bin/python /Users/mh/Dev/edgartools/run_pipeline.py TICKER
     ```
   - **Multiple non-US results** → reply listing the options, e.g.:
     ```
     Found multiple listings for VOD — which one?
     • /getdata VOD LSE  — Vodafone Group PLC (London)
     • /getdata VOD NASDAQ  — Vodafone Group PLC (US ADR)
     ```
     Then wait for the user to reply with their choice before running the pipeline.

**Examples:**
- `/getdata GS` → single US result → gather data immediately
- `/getdata VOD` → multiple results → ask user to pick
- `/getdata DGE LSE` → exchange given → gather data immediately

---

### `/build TICKER`

Builds the Excel model from already-gathered data. Run after `/getdata` when ready.

**Use the Opus model** for this command — spawn an Agent with `model: "opus"` to run:
```
/Users/mh/Dev/edgartools/.venv/bin/python /Users/mh/Dev/edgartools/build_model.py TICKER
```
Reply with a brief status: actuals/estimates periods included, file path.
