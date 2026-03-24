# My TW Coverage — Project Rules

## Purpose
Equity research coverage of 1,735+ Taiwan-listed companies (TWSE + OTC). Each ticker report maps business overview, supply chain position, and customer/supplier relationships. **Wikilinks `[[...]]` are the core mechanism** — they create a searchable cross-reference graph across all tickers (e.g., find every company serving `[[Apple]]` or participating in `[[矽光子]]` supply chain).

---

## Golden Rules

### 1. Wikilinks Must Be Specific Proper Nouns (MOST IMPORTANT RULE)
Every `[[wikilink]]` MUST resolve to a **specific, searchable proper noun**: a company name, product name, named technology, named material, or named process. Generic category words must NEVER be wikilinked.

**Three categories of wikilinks** — all equally important:

**A. Companies:** `[[台積電]]`, `[[Apple]]`, `[[Bosch]]`
**B. Named Technologies & Products:** `[[CoWoS]]`, `[[HBM]]`, `[[矽光子]]`, `[[VCSEL]]`, `[[MLCC]]`, `[[ABF 載板]]`, `[[EUV]]`
**C. Named Materials & Chemicals:** `[[光阻液]]`, `[[研磨液]]`, `[[超純水]]`, `[[氦氣]]`, `[[氖氣]]`, `[[鈦酸鋇]]`, `[[聚醯亞胺]]`

The purpose: a user searching `[[CoWoS]]` should find every company involved in CoWoS packaging. Searching `[[光阻液]]` should find every supplier and consumer of photoresist. **If a technology, product, or material is specific enough to be a meaningful search term, it MUST be wikilinked.**

**Examples of technology/material wikilinks to ALWAYS include when mentioned:**

| Category | Must-wikilink examples |
|---|---|
| Packaging | `[[CoWoS]]`, `[[InFO]]`, `[[FOPLP]]`, `[[CPO]]`, `[[HBM]]`, `[[2.5D 封裝]]`, `[[3D 封裝]]` |
| Photonics | `[[矽光子]]`, `[[EML]]`, `[[VCSEL]]`, `[[光收發模組]]`, `[[CPO]]` |
| Processes | `[[EUV]]`, `[[蝕刻]]`, `[[CVD]]`, `[[PVD]]`, `[[CMP]]`, `[[微影]]`, `[[磊晶]]` |
| Materials | `[[光阻液]]`, `[[研磨液]]`, `[[超純水]]`, `[[ABF]]`, `[[BT 樹脂]]`, `[[銅箔]]`, `[[玻纖布]]` |
| Substrates | `[[ABF 載板]]`, `[[BT 載板]]`, `[[矽晶圓]]`, `[[碳化矽]]`, `[[氮化鎵]]`, `[[磷化銦]]` |
| Components | `[[MLCC]]`, `[[MOSFET]]`, `[[IGBT]]`, `[[導線架]]`, `[[探針卡]]` |
| Applications | `[[AI 伺服器]]`, `[[電動車]]`, `[[5G]]`, `[[低軌衛星]]`, `[[資料中心]]` |

**Generic terms are allowed and encouraged as plain-text context/labels** before the specific names — they help the reader understand what the proper nouns refer to. Just don't wrap them in `[[...]]`.

| WRONG | RIGHT |
|---|---|
| `[[國際農藥原廠]]` | 國際農藥原廠如 [[Syngenta]]、[[Bayer CropScience]] |
| `[[跨國農化公司]]` | 跨國農化公司如 [[BASF]]、[[Corteva]] |
| `[[電信營運商]]` | 電信營運商如 [[中華電信]]、[[台灣大哥大]]、[[遠傳電信]] |
| `[[北美大型車廠]]` | 北美車廠如 [[Tesla]]、[[Ford]]、[[GM]] |

**Banned in wikilinks** — generic words like: 大廠, 供應商, 客戶, 廠商, 原廠, 經銷商, 製造商, 業者, 企業, 公司 (when used as category labels, not part of a proper noun)

**When specific names are genuinely unobtainable** (NDA-protected, private companies): use the generic descriptive phrase as plain text without wikilink brackets.

### 2. Ticker-Company Identity Must Match (CRITICAL — DATA INTEGRITY)
Before writing ANY enrichment content for a ticker, you MUST verify that the **company name in the filename** matches the company you researched. A ticker number can map to a completely different company than what you assume.

**Mandatory verification step:**
1. Read the actual filename: `XXXX_公司名.md`
2. Confirm your research is about **that exact company name**, not a similarly-named or better-known company with a different ticker
3. If web search returns a different company for the ticker number, **trust the filename** — the filename is ground truth

| WRONG (assumed) | RIGHT (from filename) |
|---|---|
| 6735 = 英特磊 (actually 4971) | 6735 = **美達科技** |
| 6787 = 萬潤科技 (actually 6187) | 6787 = **晶瑞光** |
| 6826 = 家登精密 (actually 3680) | 6826 = **和淞** |
| 6877 = 揚明光學 (actually 6877 was renamed) | 6877 = **鏵友益** |

**Never inject content about Company A into Company B's file.** This silently corrupts the database and is the single most damaging error possible.

### 3. Research Quality Never Degrades
The 100th ticker must receive the same deep research as the 1st. Search strategies:
- `[Ticker] 法說會` (investor conference)
- `[Ticker] 年報 主要客戶` (annual report customers)
- `[Ticker] 供應商 供應鏈` (supply chain)
- `[Company Name] supplier customer`
- Company IR pages and MOPS filings

**Never guess. Never fill generically to save time. If unsure, research more.**

### 4. Minimum 8 Proper-Noun Wikilinks Per File
Each enriched report must contain at least 8 wikilinks, and every one must pass the "specific proper noun" rule above.

### 5. Financial Tables Are Sacred
Never modify, delete, or regenerate the `## 財務概況` section or any financial tables.

### 6. All Content in Traditional Chinese
Business descriptions must be professional Traditional Chinese. Remove all original English text completely.

### 7. No Placeholders in Final Output
These strings must never appear in a completed report:
- `*(待 AI 補充)*` / `*(待 [[AI]] 補充)*`
- `(待更新)`
- `(基於嚴格實名制，因未查獲確切客戶全名而予省略)`

### 8. Metadata Must Be Complete
Every report must have these fields populated (no placeholders):
```
**板塊:** [Sector in English]
**產業:** [Industry in English]
**市值:** [number] 百萬台幣
**企業價值:** [number] 百萬台幣
```
Use `N/A 百萬台幣` for Enterprise Value only when fundamentally unavailable (e.g., banks).

### 9. Supply Chain Must Be Segmented
Break down by business segment or category — never cram into single lines:
```markdown
**上游 (原料與設備):**
- **晶圓代工:** [[台積電]], [[聯電]]
- **封裝基板:** 基板廠如 [[欣興]], [[南亞電路板]]

**中游:**
- **IC 封測:** **公司名** (封裝與測試服務)

**下游 (終端應用):**
- **AI 伺服器:** [[NVIDIA]], [[Supermicro]]
- **消費電子:** [[Apple]], [[Samsung]]
```

### 10. Customers & Suppliers Must Use Specific Names
Break down by business segment with generic context labels followed by specific wikilinked names. Same rules as above apply.

---

## File Structure

### Report Format
```
Pilot_Reports/{Industry}/{Ticker}_{ChineseName}.md
```
- Filename: `XXXX_中文名.md` (4-digit ticker + Chinese company name)
- 99 industry sector folders

### Report Sections (in order)
1. `# {Ticker} - [[{Company Name}]]` — Title with wikilinked company name
2. `## 業務簡介` — Metadata block + Traditional Chinese business description
3. `## 供應鏈位置` — Segmented upstream/midstream/downstream
4. `## 主要客戶及供應商` — Specific names by segment
5. `## 財務概況` — Financial tables (DO NOT TOUCH)

---

## Units & Formatting
- All monetary values: **百萬台幣** (Million NTD)
- Margins: percentage with `%`
- Wikilinks: `[[entity]]` — no bold inside brackets
- UTF-8 encoding for all file operations

---

## Batch Enrichment

### Source of Truth
- **Batch definitions & progress**: `task.md`
- **Batch status**: `[x]` = completed, `[ ]` = pending

### Tools
| Tool | Command | Purpose |
|---|---|---|
| Audit | `python scripts/audit_batch.py <batch> -v` | Single batch quality check |
| Audit All | `python scripts/audit_batch.py --all -v` | Regression check on all completed batches |
| Enrich | Edit DATA in `scripts/fix_batch.py`, then run | Apply enrichment to files |
| Financials | `python scripts/update_financials.py [args]` | Refresh financial tables (annual 3yr + quarterly 4Q) |

### Financial Update (`/update-financials`)
Refreshes `## 財務概況` section + metadata (市值, 企業價值) from yfinance. Preserves all enrichment content.
```
python scripts/update_financials.py                     # ALL tickers
python scripts/update_financials.py 2330                # Single ticker
python scripts/update_financials.py 2330 2317 3034      # Multiple tickers
python scripts/update_financials.py --batch 101         # All tickers in a batch
python scripts/update_financials.py --sector Semiconductors  # Entire sector
```

### Process
```
1. Audit       → python .agent/workflows/audit_batch.py <batch> -v
2. Research    → web search per ticker (法說會, 年報, MOPS, IR pages, company website)
3. Verify Name → CRITICAL: confirm filename company matches researched company
4. Prepare     → inject research into fix_batch.py DATA dict (CLEAN dict first!)
5. Execute     → python .agent/workflows/fix_batch.py
6. Verify      → re-audit, confirm all CLEAN
7. Mark done   → update task.md with [x]
8. Commit      → git checkpoint after each batch
```

### Research Queries (per ticker)
- `[Ticker] 法說會` — investor conference transcripts
- `[Ticker] 年報 主要客戶` — annual report customer disclosures
- `[Ticker] 供應商 供應鏈` — supply chain information
- `[Company Name] supplier customer` — English-language sources
- Company IR pages, MOPS filings, industry reports

### Execution Rules
- **CLEAN the DATA dict before each new batch** — old entries cause regressions
- Process in groups of 10 tickers per research cycle
- All data injection via the DATA dict in `fix_batch.py` — no temporary scripts
- Mark `task.md` immediately after batch completion
- Git commit after each batch is verified CLEAN
