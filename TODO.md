# SEC Crypto Analysis - TODO List

## ✅ Completed
- [x] Remove "tokenization" keyword from all scripts
- [x] Basic temporal analysis (overall trends 2020-2025)
- [x] SIC2 temporal analysis (industry-specific trends)
- [x] Label 150 company snippets with categories
- [x] Mastercard case study with labeled snippets
- [x] Company keyword breakdown analysis
- [x] SIC hit percentage analysis

## 🔄 In Progress
- [ ] **Download full 10-K filings** (script: `download_full_10ks.py`)
  - 1,723 10-K files to download
  - Estimated size: ~3.5GB
  - Progress tracked in `~/edgar_tmp/full_10k_progress.txt`
  - Supports pause/resume with Ctrl-C

## 📋 Next Steps - Parsing & Structure

### High Priority
- [ ] **Parse 10-K sections** (Risk Factors, MD&A, Financial Statements)
  - Identify section boundaries using Item markers
  - Build section map for each filing
  - Test on sample files first

- [ ] **Extract Balance Sheet data from Item 8**
  - Parse HTML tables
  - Extract: Total Assets, Total Liabilities, Stockholders' Equity
  - Handle multi-year comparative data
  - Output structured Excel file

- [ ] **Tag snippets with section information**
  - Match saved snippets to source sections
  - Add "Section" column to keyword hits Excel
  - Distinguish Risk Factors vs Business vs Other mentions

### Analysis After Parsing
- [ ] **Temporal analysis by label category**
  - Use 150 labeled snippets
  - Track how "crypto as risk" vs "crypto as business" changed over time
  - Break down by SIC2 sector

- [ ] **Company size analysis**
  - Use extracted Total Assets to categorize companies
  - Analyze adoption patterns by size (Small/Medium/Large/Mega)
  - Compare keyword usage across size categories

- [ ] **Risk vs Opportunity framing**
  - Analyze Item 1A (Risk Factors) mentions vs other sections
  - Quantify how often crypto appears as risk vs opportunity
  - Temporal trends in risk framing

## 📊 Advanced Analysis (Future)

- [ ] **Financial characteristics analysis**
  - Combine balance sheet data with crypto mentions
  - Do crypto-mentioning companies have different financial profiles?
  - Longitudinal analysis: track company financials over time

- [ ] **Company demographics**
  - Pull metadata: location, incorporation date, industry
  - Geographic patterns in crypto adoption
  - Company age vs crypto mention likelihood

- [ ] **Additional case studies (5-10 companies)**
  - Select representative companies across sectors
  - Deep dive into their crypto narrative evolution
  - Compare across SIC codes and company sizes

- [ ] **NLP sentiment analysis**
  - Automated sentiment scoring of snippets
  - Positive vs negative framing
  - Track sentiment changes over time

- [ ] **Keyword co-occurrence analysis**
  - Which keywords appear together?
  - Network analysis of keyword relationships
  - Context patterns (e.g., "bitcoin" + "risk" vs "blockchain" + "opportunity")

## 🔧 Technical Improvements

- [ ] **Set up cloud server for large-scale downloads**
  - Oracle Cloud Free Tier (recommended - $0/month)
  - Or wait for university server access
  - Enables re-downloading all forms (not just 10-K)

- [ ] **Leverage XBRL semantic tags**
  - More robust financial data extraction
  - Standardized accounting terms
  - Better handling of edge cases

- [ ] **Parallel processing for parsing**
  - Speed up section parser with ThreadPoolExecutor
  - Process multiple files simultaneously
  - Similar to scan.py's approach

## 📝 Notes

- Progress files stored in `~/edgar_tmp/`
- OneDrive sync location: `~/Library/CloudStorage/OneDrive-UniversityofTulsa/NSF-BSF Precautions - crypto10k/`
- Full 10-Ks will be saved to: `OneDrive/NSF-BSF Precautions - crypto10k/full_10ks/{CIK}/`
- All scripts should use the venv: `source venv/bin/activate`
