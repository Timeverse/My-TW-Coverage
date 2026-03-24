"""
update_financials.py — Refresh financial tables in ticker reports.

Fetches latest annual (3yr) and quarterly (4Q) data from yfinance,
then replaces ONLY the ## 財務概況 section in each report file.
All enrichment content (業務簡介, 供應鏈, 客戶供應商) is preserved.

Usage:
  python scripts/update_financials.py                  # Update ALL tickers
  python scripts/update_financials.py 2330             # Update single ticker
  python scripts/update_financials.py 2330 2317 3034   # Update specific tickers
  python scripts/update_financials.py --batch 101      # Update all tickers in a batch
  python scripts/update_financials.py --sector Semiconductors  # Update entire sector folder

Units: 百萬台幣 (Million NTD). Margins in %.
"""

import os
import re
import sys
import glob
import time

import pandas as pd
import yfinance as yf

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(PROJECT_ROOT, "Pilot_Reports")
TASK_FILE = os.path.join(PROJECT_ROOT, "task.md")

# Financial metrics to extract
METRICS_KEYS = {
    "revenue": ["Total Revenue"],
    "gross_profit": ["Gross Profit"],
    "selling_exp": ["Selling And Marketing Expense"],
    "admin_exp": ["General And Administrative Expense"],
    "operating_income": ["Operating Income"],
    "net_income": ["Net Income", "Net Income Common Stockholders"],
    "ocf": ["Operating Cash Flow", "Total Cash From Operating Activities"],
    "icf": ["Investing Cash Flow", "Total Cashflows From Investing Activities"],
    "fcf": ["Financing Cash Flow", "Total Cash From Financing Activities"],
    "capex": ["Capital Expenditure", "Capital Expenditures"],
}


def get_series(df, keys):
    """Safely extract a row from a DataFrame by trying multiple key names."""
    for key in keys:
        if key in df.index:
            return df.loc[key]
    return pd.Series(dtype=float)


def calc_margin(numerator, denominator):
    """Calculate margin percentage, handling empty/zero cases."""
    if denominator.empty or numerator.empty:
        return pd.Series(dtype=float)
    result = (numerator / denominator) * 100
    result = result.replace([float("inf"), float("-inf")], float("nan"))
    return result


def extract_metrics(income_stmt, cashflow):
    """Extract key financial metrics from income statement and cashflow DataFrames."""
    if income_stmt.empty and cashflow.empty:
        return pd.DataFrame()

    revenue = get_series(income_stmt, METRICS_KEYS["revenue"])
    gross_profit = get_series(income_stmt, METRICS_KEYS["gross_profit"])
    selling_exp = get_series(income_stmt, METRICS_KEYS["selling_exp"])
    admin_exp = get_series(income_stmt, METRICS_KEYS["admin_exp"])
    operating_income = get_series(income_stmt, METRICS_KEYS["operating_income"])
    net_income = get_series(income_stmt, METRICS_KEYS["net_income"])

    ocf = get_series(cashflow, METRICS_KEYS["ocf"])
    icf = get_series(cashflow, METRICS_KEYS["icf"])
    fcf = get_series(cashflow, METRICS_KEYS["fcf"])
    capex = get_series(cashflow, METRICS_KEYS["capex"])

    data = {
        "Revenue": revenue,
        "Gross Profit": gross_profit,
        "Gross Margin (%)": calc_margin(gross_profit, revenue),
        "Selling & Marketing Exp": selling_exp,
        "General & Admin Exp": admin_exp,
        "Operating Income": operating_income,
        "Operating Margin (%)": calc_margin(operating_income, revenue),
        "Net Income": net_income,
        "Net Margin (%)": calc_margin(net_income, revenue),
        "Op Cash Flow": ocf,
        "Investing Cash Flow": icf,
        "Financing Cash Flow": fcf,
        "CAPEX": capex,
    }
    return pd.DataFrame(data).T


def fetch_financials(ticker):
    """Fetch financial data for a ticker. Tries .TW then .TWO suffix."""
    for suffix in [".TW", ".TWO"]:
        try:
            stock = yf.Ticker(f"{ticker}{suffix}")

            # Test if data is available
            income = stock.income_stmt
            if income is None or income.empty:
                continue

            # Annual data (latest 3 years)
            df_annual = extract_metrics(stock.income_stmt, stock.cashflow)
            if not df_annual.empty:
                non_pct_rows = [r for r in df_annual.index if "%" not in r]
                df_annual.loc[non_pct_rows] = df_annual.loc[non_pct_rows] / 1_000_000
                df_annual = df_annual.iloc[:, :3]

            # Quarterly data (latest 4 quarters)
            df_quarterly = extract_metrics(
                stock.quarterly_income_stmt, stock.quarterly_cashflow
            )
            if not df_quarterly.empty:
                non_pct_rows = [r for r in df_quarterly.index if "%" not in r]
                df_quarterly.loc[non_pct_rows] = (
                    df_quarterly.loc[non_pct_rows] / 1_000_000
                )
                df_quarterly = df_quarterly.iloc[:, :4]

            # Also update market cap and enterprise value
            info = stock.info
            market_cap = (
                f"{info['marketCap'] / 1_000_000:,.0f}"
                if info.get("marketCap")
                else None
            )
            enterprise_value = (
                f"{info['enterpriseValue'] / 1_000_000:,.0f}"
                if info.get("enterpriseValue")
                else None
            )

            return {
                "annual": df_annual,
                "quarterly": df_quarterly,
                "market_cap": market_cap,
                "enterprise_value": enterprise_value,
                "suffix": suffix,
            }

        except Exception:
            continue

    return None


def build_financial_section(data):
    """Build the ## 財務概況 markdown section from fetched data."""
    section = "## 財務概況 (單位: 百萬台幣, 只有 Margin 為 %)\n"
    section += "### 年度關鍵財務數據 (近 3 年)\n"

    if data["annual"] is not None and not data["annual"].empty:
        section += data["annual"].to_markdown(floatfmt=".2f") + "\n\n"
    else:
        section += "無可用數據。\n\n"

    section += "### 季度關鍵財務數據 (近 4 季)\n"
    if data["quarterly"] is not None and not data["quarterly"].empty:
        section += data["quarterly"].to_markdown(floatfmt=".2f") + "\n"
    else:
        section += "無可用數據。\n"

    return section


def update_metadata(content, data):
    """Update market cap and enterprise value in metadata block if available."""
    if data.get("market_cap"):
        content = re.sub(
            r"(\*\*市值:\*\*) .+?百萬台幣",
            rf"\1 {data['market_cap']} 百萬台幣",
            content,
        )
    if data.get("enterprise_value"):
        content = re.sub(
            r"(\*\*企業價值:\*\*) .+?百萬台幣",
            rf"\1 {data['enterprise_value']} 百萬台幣",
            content,
        )
    return content


def update_file(filepath, ticker, dry_run=False):
    """Update a single ticker report file with fresh financial data."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # Fetch fresh data
    data = fetch_financials(ticker)
    if data is None:
        print(f"  {ticker}: SKIP (no data from yfinance)")
        return False

    # Build new financial section
    new_fin_section = build_financial_section(data)

    # Replace the financial section (everything from ## 財務概況 to end of file)
    fin_pattern = r"## 財務概況.*"
    if re.search(fin_pattern, content, re.DOTALL):
        new_content = re.sub(fin_pattern, new_fin_section, content, flags=re.DOTALL)
    else:
        # No financial section exists — append it
        new_content = content.rstrip() + "\n\n" + new_fin_section

    # Update metadata (market cap, enterprise value)
    new_content = update_metadata(new_content, data)

    if dry_run:
        print(f"  {ticker}: WOULD UPDATE ({data['suffix']})")
        return True

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(new_content)

    print(f"  {ticker}: UPDATED ({data['suffix']})")
    return True


def find_ticker_files(tickers=None, sector=None):
    """Find report files matching given tickers or sector."""
    files = {}
    for fp in glob.glob(os.path.join(REPORTS_DIR, "**", "*.md"), recursive=True):
        fn = os.path.basename(fp)
        m = re.match(r"^(\d{4})_", fn)
        if not m:
            continue
        t = m.group(1)

        if sector:
            folder = os.path.basename(os.path.dirname(fp))
            if folder.lower() != sector.lower():
                continue

        if tickers is None or t in tickers:
            files[t] = fp

    return files


def get_batch_tickers(batch_num):
    """Get ticker list for a batch from task.md."""
    with open(TASK_FILE, "r", encoding="utf-8") as f:
        content = f.read()
    pattern = re.compile(
        r"Batch\s+" + str(batch_num) + r"\*\*.*?:\s*(.*)$",
        re.IGNORECASE | re.MULTILINE,
    )
    match = pattern.search(content)
    if match:
        raw = match.group(1).strip().rstrip(".")
        return [re.search(r"(\d{4})", t).group(1) for t in raw.split(",") if re.search(r"\d{4}", t)]
    return []


def main():
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    args = sys.argv[1:]
    dry_run = "--dry-run" in args
    if dry_run:
        args.remove("--dry-run")

    tickers = None
    sector = None

    if not args:
        # Update ALL tickers
        print("Updating ALL ticker financials...")
        files = find_ticker_files()
    elif args[0] == "--batch":
        batch_num = args[1]
        tickers = get_batch_tickers(batch_num)
        print(f"Updating {len(tickers)} tickers in Batch {batch_num}...")
        files = find_ticker_files(tickers)
    elif args[0] == "--sector":
        sector = " ".join(args[1:])
        print(f"Updating all tickers in sector: {sector}...")
        files = find_ticker_files(sector=sector)
    else:
        # Specific tickers
        tickers = [t.strip() for t in args if re.match(r"^\d{4}$", t.strip())]
        print(f"Updating {len(tickers)} tickers: {', '.join(tickers)}")
        files = find_ticker_files(tickers)

    if not files:
        print("No matching files found.")
        return

    print(f"Found {len(files)} files to update.\n")

    updated = 0
    failed = 0
    skipped = 0

    for ticker in sorted(files.keys()):
        try:
            result = update_file(files[ticker], ticker, dry_run=dry_run)
            if result:
                updated += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"  {ticker}: ERROR ({e})")
            failed += 1

        # Rate limit to avoid yfinance throttling
        time.sleep(0.5)

    print(f"\nDone. Updated: {updated} | Skipped: {skipped} | Failed: {failed}")


if __name__ == "__main__":
    main()
