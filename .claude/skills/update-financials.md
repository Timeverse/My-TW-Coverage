---
name: update-financials
description: Update financial tables (annual 3yr + quarterly 4Q) for ticker reports using yfinance data
user_invocable: true
---

# Update Financials

Refresh the `## 財務概況` section in ticker reports with the latest financial data from yfinance. Also updates market cap and enterprise value in metadata.

**All enrichment content (業務簡介, 供應鏈, 客戶供應商) is preserved — only financials are replaced.**

## Usage

The user can specify scope in their message:

- **All tickers**: `/update-financials` (no arguments — updates all 1,733 reports)
- **Single ticker**: `/update-financials 2330`
- **Multiple tickers**: `/update-financials 2330 2317 3034`
- **By batch**: `/update-financials --batch 101`
- **By sector**: `/update-financials --sector Semiconductors`
- **Dry run**: add `--dry-run` to preview without writing

## Instructions

1. Parse the user's arguments from their message.
2. Run the update script:

```bash
cd "f:\My TW Coverage" && python scripts/update_financials.py [ARGS]
```

3. Report results: how many updated, skipped, failed.
4. If updating ALL tickers, warn the user this will take a while (~15-30 min for 1,733 tickers due to yfinance rate limits) and ask for confirmation before proceeding.
5. After completion, ask if the user wants to commit the changes.

## What Gets Updated

| Field | Source | Location |
|---|---|---|
| **市值** (Market Cap) | `stock.info['marketCap']` | Metadata block |
| **企業價值** (Enterprise Value) | `stock.info['enterpriseValue']` | Metadata block |
| **年度財務 (3yr)** | `stock.income_stmt` + `stock.cashflow` | `### 年度關鍵財務數據` table |
| **季度財務 (4Q)** | `stock.quarterly_income_stmt` + `stock.quarterly_cashflow` | `### 季度關鍵財務數據` table |

All monetary values in **百萬台幣** (Million NTD). Margins in **%**.

## Metrics Tracked

Revenue, Gross Profit, Gross Margin %, Selling & Marketing Exp, General & Admin Exp, Operating Income, Operating Margin %, Net Income, Net Margin %, Op Cash Flow, Investing Cash Flow, Financing Cash Flow, CAPEX.
