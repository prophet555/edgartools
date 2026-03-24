---
name: financial-model
description: >
  Build a full 3-statement financial model (IS, BS, CFS) with an Assumptions tab and DCF valuation
  from uploaded TIKR-format CSV files (financials + forward estimates), or use combined csv files for financials.
  Use this skill whenever the
  user asks to "build a model", "make a financial model", "create a DCF", "build a valuation model",
  or references building projections from CSV financial data. Also trigger when the user uploads
  financials and estimates CSVs and wants them turned into an Excel model, or asks to "model" a
  company. This skill produces a professional .xlsx workbook with 5 tabs: IS, BS, CFS, Assumptions,
  DCF — with full cross-sheet linkage, sensitivity tables, and investment-bank-grade formatting.
---

# Financial Model Builder

Build a 5-tab Excel financial model from uploaded TIKR-format CSV data. The model uses actual
historical financials as blue hardcodes and projects forward years using assumption-driven formulas.
Every estimate cell traces back to the Assumptions tab — no hardcoded percentages or dollar amounts
live inside IS/BS/CFS formulas.

This skill depends on the `xlsx` skill for Excel creation mechanics (openpyxl, recalc, error
checking). Read the xlsx SKILL.md first for formatting and formula construction fundamentals, then
follow the instructions below for the model-specific structure.

---

## Input Files

The user provides two CSVs (typically from TIKR.com):

1. **Financials CSV** — contains three sections separated by `=== Section Name ===` headers:
   - `=== Income Statement ===` — historical IS with line items, growth rates, margins, supplementary data
   - `=== Balance Sheet ===` — historical BS with assets, liabilities, equity, supplementary data
   - `=== Cash Flow Statement ===` — historical CFS with operating/investing/financing activities

2. **Forward Estimates CSV** — consensus analyst estimates with columns per future year:
   - Revenue, EBITDA, EBIT, Net Income, EPS, D&A, Interest Expense, CFO, CapEx, etc.
   - Some rows may be blank (no consensus available) — skip those

### Parsing Rules

- Detect actual year columns from the financials CSV header row (format: `MM/DD/YY` or `MM/DD/YYYY`)
- Detect estimate year columns from the estimates CSV header row (format: `MM/DD/YY E` or `MM/DD/YYYY E`)
- Drop the `LTM` column if present — it duplicates the most recent actual year
- Drop the `CAGR` column if present in estimates
- Numbers use comma separators and parentheses for negatives: `"(1,234.00)"` = -1234
- Percentage rows contain `%` in values — parse as decimals (e.g., `36.2%` → 0.362)
- Section headers like `Supplementary Data:` or `Price Factors:` are metadata separators, not data rows
- Line item labels are in column A (or the first column); use them exactly as-is for row labels in Excel
  (do not rename or genericize them — preserve the company's actual filing terminology)

---

## Sheet Structure

Create 5 tabs in this order:

| # | Tab | Tab Color |
|---|-----|-----------|
| 1 | IS | #2F5496 (blue) |
| 2 | BS | #375623 (green) |
| 3 | CFS | #843C0C (brown) |
| 4 | Assumptions | #7030A0 (purple) |
| 5 | DCF | #C00000 (red) |

Value columns: 90pt (approximately 13 in openpyxl units) each.

---

## Formatting — Apply Identically on All Tabs

**Row 1**: Blank spacer, height ~7 (0.5 equivalent).

**Column A**: Blank spacer, width 1.

**Row 2 — Title bar** (starts in column B, merged across all data columns):
- Background `#1F3864`, white bold text, font size 13
- Text: `[Company] ([Ticker]) — [Statement Name] ($000s)`

**Row 3 — Column headers**:
- Actual year columns: background `#2F5496`, white bold text, centered
- Estimate year columns: background `#375623`, white bold text, centered

**Section headers** (e.g., REVENUE, ASSETS, OPERATING ACTIVITIES):
- Background `#D6E4F7`, bold text color `#1F3864`, spanning full row

**Row labels — Column B**:
- Width: 35
- All labels: BLACK text (`#000000`) on WHITE (`#FFFFFF`) — apply explicitly, do not rely on inherited
- Total/subtotal rows: bold black
- Sub-line items: 2-space indent, normal weight
- Margin/growth/ratio helper rows: italic grey (`#666666`)

**Font color coding for data cells**:
- Blue (`#0000FF`) — hardcoded inputs (actuals and user-editable assumptions)
- Black (`#000000`) — all calculated formulas
- Green (`#008000`) — cross-sheet links pulling from another tab
- Grey italic (`#888888`) — helper/ratio rows (margins, growth %)

**Number formats**:
- Dollar values: `#,##0`
- Negatives: `#,##0;(#,##0);-`
- Percentages: `0.0%`
- Per-share values: `$#,##0.00`
- Multiples/turns: `0.0x`

**Total row borders**: Medium-weight top border in the tab's accent color on every subtotal/total row.

**Green highlight rows** (key totals):
- Total Assets, Total Liabilities & Equity: background `#E2EFDA`

---

## Build Order

Build Assumptions first (it has no dependencies). Then IS, BS, CFS, and DCF in that order.
The reason: IS needs Assumptions for projections; BS needs IS (retained earnings) and Assumptions;
CFS needs IS (net income, D&A, SBC) and BS (working capital changes); DCF needs all three.

---

## Assumptions Tab

Columns: Assumption label (B) | one column per estimate year (C onward) | Notes (last column)

Three sections with `#D6E4F7` section headers:

### INCOME STATEMENT DRIVERS
One row per key driver. Derive initial values from the estimates CSV where consensus exists, and from
the last actual year's margins/ratios where consensus is missing. Typical rows:
- Revenue growth %
- Gross margin %
- SG&A as % of revenue
- D&A ($ amount — from estimates CSV if available, else trended)
- Other operating expenses as % of revenue
- Interest expense ($ — from estimates CSV)
- Tax rate %
- Other income items as % of revenue or fixed $
- Share count (from estimates CSV EPS ÷ Net Income, or trended from actuals)
- SBC ($ — estimate or trend)
- Any company-specific drivers apparent from the filing line items

### BALANCE SHEET DRIVERS
One row per working capital or balance sheet item that will be projected:
- DSO (days sales outstanding = AR / Revenue × 365)
- DPO (days payable outstanding = AP / COGS × 365)
- Inventory days (if applicable = Inventory / COGS × 365)
- Each other current asset/liability as % of revenue or as fixed $
- PP&E: CapEx from estimates, depreciation from IS drivers, net PP&E rolls forward
- Intangibles/goodwill: amortization schedule or held flat
- Long-term debt: user assumption (hold flat or scheduled repayment)
- Other non-current items as % of revenue or held flat

### CASH FLOW DRIVERS
One row per item not already derived from IS/BS changes:
- CapEx ($ — from estimates CSV)
- Stock issuances/repurchases ($)
- Dividends ($)
- Debt issuance/repayment ($)
- Other one-off cash items ($)

### Rules
- All values are blue hardcodes entered by the user
- Every estimate cell on IS, BS, and CFS must trace back to a cell on this tab
- Never put a hardcoded % or $ directly inside an IS/BS/CFS formula
- Notes column: cite the source and date for every assumption (e.g., "TIKR consensus, Mar 2026" or
  "FY2025 actual rate held flat")
- Add a MODEL NOTES section at the bottom explaining color coding and units

---

## Income Statement Tab

Columns: [N] actual years (blue hardcodes) | [N] estimate years (black formulas)

Use as many actual and estimate years as are available in the uploaded files.

### Sections and Rows
Adapt line item names to match the company's actual filings. Use the uploaded IS labels exactly.

**REVENUE**: One row per revenue line reported. Total Revenue row (bold). YoY Growth % helper (italic grey).

**COST OF REVENUE / COGS**: One row per CoR line. Total Cost row (bold).

**Gross Profit** (bold total). Gross Margin % helper (italic grey).

**OPERATING EXPENSES**: One row per OpEx line. Total Operating Expenses (bold).

**Operating Income / EBIT** (bold). EBIT Margin % helper (italic grey).

**OTHER INCOME / EXPENSE**: Interest income, interest expense, equity investments, FX, other — as reported.

**Pre-Tax Income** (bold). Tax Provision / Benefit.

**Net Income / Net Loss** (bold). Minority interest / NCI lines as reported. Net Income Attributable to Company (bold).

**Weighted Average Shares**. **EPS** (Basic & Diluted) — format `$#,##0.00`.

**SUPPLEMENTAL METRICS**: SBC, D&A, EBITDA, EBITDA Margin % — pulled from IS data or Assumptions.

### Linkage Rules
- Actuals: hardcoded blue values matching the filing exactly
- Estimates: every line references the Assumptions tab — no hardcoded percentages
- All cross-sheet links (e.g., to BS for interest): green font

---

## Balance Sheet Tab

Columns: [N] actual years (blue hardcodes) | [N] estimate years (black formulas)

Use exact line item names from the uploaded BS.

**ASSETS**: Current Assets (one row per line, Total Current Assets bold). Non-Current Assets (one row per line, Total Non-Current Assets bold). **TOTAL ASSETS** (bold, green background `#E2EFDA`).

**LIABILITIES & STOCKHOLDERS' EQUITY**: Current Liabilities (one row per line, Total Current Liabilities bold). Non-Current Liabilities (one row per line, Total Liabilities bold). Equity section (paid-in capital, retained earnings, treasury stock, AOCI, NCI as reported, Total Stockholders' Equity bold). **TOTAL LIABILITIES & EQUITY** (bold, green background `#E2EFDA`).

**Balance Check** row = Total Assets − Total L&E (italic grey, should be 0 for actuals).

### Linkage Rules
- Cash for all estimate years: green cross-sheet link to CFS Ending Cash — never hardcode projected cash
- Retained earnings rolls forward: prior year + IS Net Income − Dividends
- Paid-in capital rolls forward: prior year + SBC + stock issuances from CFS
- All other estimate cells reference Assumptions tab drivers

---

## Cash Flow Statement Tab

Columns: [N] actual years (blue hardcodes) | [N] estimate years (black formulas)

Use exact line item names from the uploaded CFS.

**OPERATING ACTIVITIES**: Net income (green link from IS). Non-cash add-backs (D&A, SBC — link to IS). Working capital changes (derived from period-over-period BS changes). Other operating items. **Net Cash from Operating Activities** (bold).

**INVESTING ACTIVITIES**: CapEx, acquisitions, investments, disposals. **Net Cash from Investing Activities** (bold).

**FINANCING ACTIVITIES**: Debt issuance/repayment, equity issuances, dividends, buybacks. **Net Cash from Financing Activities** (bold).

Effect of FX on Cash (if reported). **Net Change in Cash** (bold). Beginning Cash. **Ending Cash** (bold) — this cell is what BS Cash links to.

**SUPPLEMENTAL**: Free Cash Flow = CFO + CapEx (italic grey). FCF Margin % = FCF / Revenue (italic grey).

### Linkage Rules
- Ending Cash feeds BS Cash for each estimate year
- All estimate rows reference either IS, BS changes, or Assumptions
- Never hardcode a projected operating or capex item directly in this tab

---

## DCF Tab

### Section 1 — DCF ASSUMPTIONS (navy header `#2F5496`)
Two-column table: Assumption label | Value | Notes. Blue hardcoded inputs:
- WACC, Terminal Growth Rate, projection period (years)
- Cash & equivalents, short-term investments, total debt
- Net cash (formula: cash + investments − debt)
- Diluted shares outstanding
- Reference NTM multiple from comps (for cross-check)
- Current share price

### Section 2 — FREE CASH FLOW PROJECTIONS (navy header)
Columns: Base year (actual) | Year 1..N estimate | Terminal column. All rows are green cross-sheet links:
- Revenue, Revenue Growth % (grey), EBIT, EBIT Margin % (grey)
- Tax on EBIT (use tax rate from Assumptions; zero if EBIT negative), NOPAT
- (+) D&A, (+) SBC, (−) CapEx, (+/−) Change in Working Capital
- **Unlevered Free Cash Flow** (bold total), uFCF Margin % (grey)

### Section 3 — DCF CALCULATION (navy header)
- Period numbers (grey italic)
- Discount factor row
- FCF row (linked from Section 2)
- Terminal Value (Gordon Growth: final year FCF × (1 + TGR) / (WACC − TGR))
- PV of each FCF and PV of Terminal Value (bold)
- Sum of PV of FCFs
- PV of Terminal Value

### Section 4 — VALUATION BRIDGE (navy header)
- Sum of PV FCFs
- (+) PV of Terminal Value
- **Enterprise Value** — bold, green background `#E2EFDA` (show in $000s and $M)
- % from Terminal Value (grey italic)
- (+) Net Cash
- **Equity Value** — bold, green background
- (/) Diluted Shares
- **Intrinsic Value Per Share** — RED background `#C00000`, white bold text, `$#,##0.00`

Grey italic cross-checks:
- TV as % of EV, Implied EV/NTM Revenue, Implied EV/NTM EBITDA
- Current Share Price
- Upside/Downside to Current Price

### Section 5 — SENSITIVITY ANALYSIS (navy header)
Start in column C.

**Table 1**: Implied Share Price — WACC (5 rows) × Terminal Growth Rate (6 cols)
**Table 2**: Implied Share Price — WACC (5 rows) × FCF Exit Multiple (6 cols)

For both tables:
- Headers: navy background `#2F5496`, white bold text
- Base WACC row header: RED background `#C00000`, white text
- Base case cell (intersection of base WACC and base assumption): YELLOW background `#FFFF00`, black bold
- All other data cells: white background, `$#,##0.00`, centered
- Note row below each table (grey italic) identifying the base case

### Section 6 — EV / NTM REVENUE MULTIPLES (dark green header `#375623`)
- Input rows: NTM Revenue (green link to IS), Net Cash, Diluted Shares
- Peer context note (grey italic)
- Grid: multiples as column headers (choose range appropriate to the sector, odd number of columns so base case is centered)
- Three output rows:
  - Implied EV ($M) — light blue background `#D9E1F2`
  - Equity Value ($M) — white background
  - Implied Share Price — dark green background `#375623`, white bold text
- Base case multiple column: YELLOW background `#FFFF00`, black bold
- Note row below (grey italic) identifying NTM period and net cash treatment

---

## Critical Linkage Rules — Verify Before Delivering

1. Every estimate formula on IS, BS, CFS must reference Assumptions. No hardcoded %s or $s in projection formulas.
2. Revenue growth on IS flows from the Assumptions revenue growth row. If revenue has sub-lines, each defaults to total revenue growth so one input updates all.
3. BS Cash for all estimate years = CFS Ending Cash (green cross-sheet link). Never hardcode projected cash.
4. IS Net Income feeds BS retained earnings roll-forward each year.
5. IS SBC + CFS stock issuances feed BS paid-in capital roll-forward.
6. DCF FCF build pulls Revenue, EBIT, D&A, SBC, CapEx via green links. No hardcoded values in DCF FCF section.
7. EV/NTM Revenue table NTM Revenue cell is a green link to IS.
8. Balance sheet must balance for all actual years (Balance Check = 0).
9. No formula errors (#REF!, #VALUE!, #NAME?, circular references) anywhere.
10. Column B labels are BLACK text on WHITE background on every tab — apply explicitly.

---

## Data & Citation Standards

- Actuals entered as blue hardcodes must match the uploaded filing exactly
- Add a cell comment to every hardcoded actual referencing the source: `"Source: TIKR.com, [Fiscal Year], [Line item]"`
- Estimate assumption inputs are blue with a Notes column entry
- Cross-sheet links are green — no comment needed

---

## Post-Build Checklist

After building all 5 tabs:

1. Run `recalc.py` on the output file to populate all formula values
2. Check for formula errors in the recalc output — fix any `#REF!`, `#DIV/0!`, `#VALUE!`, `#NAME?`
3. Verify Balance Check = 0 for all actual years
4. Verify Ending Cash on CFS matches Cash on BS for all estimate years
5. Verify DCF Intrinsic Value per Share is a reasonable number (not negative, not absurdly high)
6. Spot-check 2-3 IS estimate cells to confirm they reference Assumptions, not hardcodes
7. Confirm all tab colors are set correctly
8. Confirm all section headers have the `#D6E4F7` background
