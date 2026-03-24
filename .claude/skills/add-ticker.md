---
name: add-ticker
description: Add a new ticker to the coverage database with financials and enrichment
user_invocable: true
---

# Add Ticker

Add a new Taiwan-listed company to the coverage database. Generates the .md report file with financials, then researches and enriches it.

## Usage

- `/add-ticker 2330 台積電` — auto-detect sector from yfinance
- `/add-ticker 2330 台積電 --sector Semiconductors` — specify sector

## Instructions

### Step 1: Generate the base file

```bash
cd "f:\My TW Coverage" && python scripts/add_ticker.py <ticker> <name> [--sector <sector>]
```

This creates the .md file with metadata + financials from yfinance and placeholder enrichment sections.

### Step 2: Research and enrich

After generating, research the company:
1. Web search: `[Ticker] 法說會`, `[Ticker] 年報 主要客戶`, `[Company] supplier customer`
2. **VERIFY**: the company name from filename matches your research (CLAUDE.md Golden Rule #2)
3. Write enrichment data as JSON:

```json
{
  "XXXX": {
    "desc": "Traditional Chinese description with [[wikilinks]]...",
    "supply_chain": "**上游:**\n- ...\n**中游:**\n- ...\n**下游:**\n- ...",
    "cust": "### 主要客戶\n- ...\n\n### 主要供應商\n- ..."
  }
}
```

4. Save to a temp file and apply:
```bash
python scripts/update_enrichment.py --data /tmp/enrich.json <ticker>
```

### Step 3: Audit

```bash
python scripts/audit_batch.py --all -v 2>&1 | grep <ticker>
```

Or just read the file and verify it meets Golden Rules (8+ wikilinks, no generics, no English).

### Step 4: Add to task.md if applicable

If the ticker belongs to an existing batch, add it. Otherwise note it as a standalone addition.
