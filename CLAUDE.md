# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a research project for analyzing cryptocurrency-related mentions in SEC filings. The project downloads filings from the SEC EDGAR database, scans them for crypto keywords, and generates analysis reports and snippets for qualitative research.

## Environment Setup

**Python Version:** Requires Python 3.10+

**Virtual Environment:**
```bash
python3 -m venv venv
source venv/bin/activate  # On macOS/Linux
pip install -r requirements.txt
```

**Dependencies:** All dependencies are in `requirements.txt`. Key packages include:
- `requests` - HTTP client with connection pooling
- `pandas` + `openpyxl` - Excel I/O
- `beautifulsoup4` + `lxml` - HTML parsing
- `PyPDF2` - PDF extraction (optional)

## Core Architecture

### Two-Stage Pipeline

The project follows a two-stage workflow:

1. **Data Collection** (`scan.py`) - Downloads SEC filings to local staging, then flushes to OneDrive
2. **Analysis Scripts** - Process downloaded filings to generate reports and snippets

### Data Flow

```
Input (data/Publicly_Trade_Companies_SEC.xlsx)
  → scan.py downloads to ~/edgar_tmp/stage/{CIK}/
  → Files moved to OneDrive folder
  → Analysis scripts (in scripts/) read from OneDrive
  → Output .xlsx files to data/
```

### Repository Structure

```
SEC-Crypto-Analysis/
├── scan.py                  # Main EDGAR scanner
├── VMscan.py                # VM variant scanner
├── download_full_10ks.py    # Full 10-K downloader
├── pull_from_vm.sh          # VM data transfer script
├── requirements.txt
├── progress.txt             # Scan progress tracking
├── README.md / CLAUDE.md / TODO.md / CurrentNotes.md
│
├── scripts/                 # Analysis scripts
│   ├── analyze_filings.py
│   ├── generate_snippets.py
│   ├── generate_10k_snippets_with_sections.py
│   ├── temporal_analysis.py
│   ├── sic2_temporal_analysis.py
│   ├── sic_hit_percentages.py
│   ├── sic_keyword_percentages.py
│   ├── extract_sic2.py
│   ├── extract_balance_sheets.py
│   ├── parse_10k_sections.py
│   ├── company_keyword_breakdown.py
│   ├── generate_analysis.py
│   ├── mastercard_case_study.py
│   └── scan_mastercard.py
│
├── data/                    # Excel input/output files
│   ├── Publicly_Trade_Companies_SEC.xlsx  (input)
│   ├── crypto_keyword_hits.xlsx
│   ├── crypto_snippets_150.xlsx
│   ├── crypto_analysis.xlsx
│   ├── ... (other Excel/PNG outputs)
│   └── test_output/         # Parsed JSON test files
│
└── venv/                    # Python virtual environment
```

### Key Directories

- **Staging:** `~/edgar_tmp/stage/` - Local temporary storage during scanning
- **OneDrive:** `~/Library/CloudStorage/OneDrive-UniversityofTulsa/NSF-BSF Precautions - crypto10k/` - Final storage location
- **Progress Tracking:** `progress.txt` (in repo root) - Records completed CIKs for resuming
- **Data:** `data/` - All Excel input/output files

### CIK Folder Structure

Each company gets a folder named by 10-digit zero-padded CIK containing:
- `SIC.txt` - Company's SIC code
- `{CIK}_{FORM}_{YYYY-MM-DD}_{DOCNAME}.htm(l)` - HTML filings with keyword hits
- `{CIK}_{FORM}_{YYYY-MM-DD}_{ACCESSION}.txt` - Master text file (fallback)
- `COMPLETE` - Marker file indicating CIK processing is finished

## Commands

### Main Scanning Workflow

**Initial scan (downloads to staging):**
```bash
python scan.py --start-from-cik <10-digit-CIK>  # Start from specific CIK
python scan.py --resume-from-last                # Resume from last completed
```

**Resume after interruption:**
- Press `Ctrl-C` to pause anytime - progress is auto-saved to `~/edgar_tmp/progress.txt`
- Rerun the same command to resume from where it left off

**Clear temporary files:**
```bash
# Clear staging directory only
python -c "import shutil, pathlib; shutil.rmtree(pathlib.Path.home() / 'edgar_tmp/stage', ignore_errors=True)"

# Clear staging AND progress file (full reset)
python -c "import shutil, pathlib; shutil.rmtree(pathlib.Path.home() / 'edgar_tmp', ignore_errors=True)"
```

### Analysis Scripts

All analysis scripts are in the `scripts/` folder. Run from repo root:

**Generate keyword hit analysis:**
```bash
python scripts/analyze_filings.py  # Creates data/crypto_keyword_hits.xlsx
```

**Temporal analysis:**
```bash
python scripts/temporal_analysis.py        # Overall trends
python scripts/sic2_temporal_analysis.py   # By industry (SIC code)
```

**Generate snippets for qualitative coding:**
```bash
python scripts/generate_snippets.py              # Sample 150 companies
python scripts/mastercard_case_study.py          # Single company deep dive
```

**Statistical analysis:**
```bash
python scripts/company_keyword_breakdown.py      # Keyword frequency per company
python scripts/sic_hit_percentages.py           # Hit rates by SIC code
python scripts/sic_keyword_percentages.py       # Keyword distribution by SIC
```

## scan.py Configuration

### Performance Tuning

The scanner uses aggressive concurrency and connection pooling:
- `CIK_CONCURRENCY = 10` - Parallel companies being processed
- `DOC_CONCURRENCY = 20` - Parallel documents per filing
- `MAX_RPS = 9.8` - Requests per second (respects SEC 10 req/sec limit)
- Connection pooling: 128 connections, 512 max pool size

### HTTP Strategy

- Thread-local `requests.Session` per worker with connection pooling
- Smart retry with `Retry-After` header support
- Rate limiting using `RateLimiter` class
- Stream processing with early termination when keywords found
- HEAD requests to check file size before downloading large files

### Keyword Matching

Keywords are matched case-insensitively with word boundaries:
- bitcoin, blockchain, ethereum, cryptocurrency
- digital asset, distributed ledger, non-fungible token, crypto-asset
- Hyphens and spaces within phrases are accepted (e.g., "digital-asset" or "digital asset")

Regular expression in `scan.py` and analysis scripts:
```python
KEYWORDS = re.compile(
    r"\b(bitcoin|blockchain|ethereum|cryptocurrency|"
    r"digital[- ]asset|distributed[- ]ledger|non[- ]fungible[- ]token|crypto[- ]asset)\b",
    re.I
)
```

### SEC Filings Scanned

Forms: `10-K`, `10-Q`, `8-K`, `20-F`, `40-F`, `6-K`

Default lookback: 5 years (configurable via `YEARS_BACK` in `scan.py`)

## Analysis Script Patterns

### Common Data Loading Pattern

Most analysis scripts follow this pattern:
1. Load company names from `data/Publicly_Trade_Companies_SEC.xlsx`
2. Read filings from OneDrive folder by walking CIK directories
3. Parse filename format: `{CIK}_{FORM}_{DATE}_{ACCESSION}.ext`
4. Extract keywords using same regex as `scan.py`
5. Output to Excel in `data/` folder with openpyxl engine

Scripts use `SCRIPT_DIR` and `DATA_DIR` constants for portable paths:
```python
SCRIPT_DIR = pathlib.Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data"
```

### File Naming Convention

All downloaded files follow strict naming:
- HTML docs: `{CIK}_{FORM}_{YYYY-MM-DD}_{DOCNAME}.htm(l)`
- Master text: `{CIK}_{FORM}_{YYYY-MM-DD}_{ACCESSION}.txt`

The `parse_filename()` function in analysis scripts handles this format.

## Modifying the Scanner

### Changing Keywords

Update the `KEYWORDS` regex in `scan.py`. Also update in all analysis scripts to maintain consistency.

### Adding New Form Types

Add to the `FORMS` set in `scan.py`:
```python
FORMS = {"10-K", "10-Q", "8-K", "20-F", "40-F", "6-K"}
```

### Adjusting Concurrency

Edit these constants in `scan.py`:
- Lower values = more stable, slower
- Higher values = faster but risks rate limiting
- Current values are optimized for SEC's 10 req/sec limit

## Important Implementation Details

### Thread Safety

- `_TLS = threading.local()` provides per-thread session objects
- `RATE_LIMITER` uses threading.Lock for global rate limiting
- `STOP_EVENT` coordinates graceful shutdown across threads

### Progress Persistence

- Each completed CIK is appended to `progress.txt` (in repo root)
- `COMPLETE` marker file in each CIK folder indicates finished processing
- `.STAGING` file indicates in-progress (removed when complete)

### Memory Optimization

- Stream processing with chunk size 256KB
- Early termination when keywords found (minimal context retained)
- In-memory PDF processing (no temp files)
- Limited PDF extraction to first 10 pages only
- File size cap: 20MB per file (`MAX_SAVE_MB_PER_FILE`)

### Error Handling

- Backoff strategy: [15, 30, 60] seconds for retries
- Respects HTTP `Retry-After` header for 429 responses
- Graceful degradation (skip on repeated failures)
- Ctrl-C triggers flush to OneDrive before exit

## SEC API Endpoints

The scanner uses these SEC.gov endpoints:
- Submissions metadata: `https://data.sec.gov/submissions/CIK{cik}.json`
- Filing index: `https://www.sec.gov/Archives/edgar/data/{cik_nolead}/{folder}/index.json`
- Documents: `https://www.sec.gov/Archives/edgar/data/{cik_nolead}/{folder}/{name}`
- Master text: `https://www.sec.gov/Archives/edgar/data/{cik_nolead}/{acc_dash}.txt`

**User-Agent Required:** All requests must include a User-Agent with contact info per SEC policy. Update `UA` constant in `scan.py` with actual contact information before running at scale.
