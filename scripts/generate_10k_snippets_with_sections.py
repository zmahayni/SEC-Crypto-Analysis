#!/usr/bin/env python3
"""
Generate snippets from FULL 10-K filings with section labels for classification.
Samples ONE snippet per company to maximize company diversity.
"""

import re
import pathlib
import pandas as pd
import random
import sys
from typing import Optional, List, Tuple
from bs4 import BeautifulSoup

# =============================================================================
# CONFIG
# =============================================================================

SCRIPT_DIR = pathlib.Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data"

HOME = pathlib.Path.home()
CLOUD_FOLDER = (
    HOME
    / "Library/CloudStorage/OneDrive-SharedLibraries-UniversityofTulsa/NSF-BSF Precautions - crypto10k"
)
FULL_10K_FOLDER = HOME / "Desktop" / "full_10ks"
OUTPUT_FILE = DATA_DIR / "10k_snippets_for_classification.xlsx"

# Sampling config
TARGET_SAMPLE_SIZE = 200
RANDOM_SEED = 42

# Keywords (same as scan.py)
KEYWORDS = re.compile(
    r"\b("
    r"bitcoin|blockchain|ethereum|cryptocurrency|"
    r"digital[- ]asset|distributed[- ]ledger|non[- ]fungible[- ]token|crypto[- ]asset"
    r")\b",
    re.I,
)

# Snippet parameters
MIN_PARAGRAPH_LENGTH = 80
MIN_SNIPPET_LENGTH = 200
MAX_SNIPPET_LENGTH = 800
MAX_FILE_SIZE_MB = 50  # Skip files larger than this (BeautifulSoup is too slow)

# Noise patterns to skip
NOISE_PATTERNS = [
    re.compile(r"^(table of contents|item \d+[a-z]?\.?)", re.I),
    re.compile(r"^\s*page \d+", re.I),
    re.compile(r"^\s*\d+\s*$"),  # Just numbers
    re.compile(r"^[\s\-_=]+$"),  # Just whitespace/separators
]

# Standard 10-K sections
STANDARD_ITEMS = {
    "1": "Business",
    "1A": "Risk Factors",
    "1B": "Unresolved Staff Comments",
    "2": "Properties",
    "3": "Legal Proceedings",
    "4": "Mine Safety Disclosures",
    "5": "Market for Common Equity",
    "6": "Reserved",
    "7": "MD&A",
    "7A": "Market Risk Disclosures",
    "8": "Financial Statements",
    "9": "Accountant Disagreements",
    "9A": "Controls and Procedures",
    "9B": "Other Information",
    "9C": "Foreign Jurisdictions",
    "10": "Directors and Officers",
    "11": "Executive Compensation",
    "12": "Security Ownership",
    "13": "Related Transactions",
    "14": "Accounting Fees",
    "15": "Exhibits",
}

# =============================================================================
# SECTION PARSING
# =============================================================================


def find_section_positions(text: str) -> List[Tuple[str, int, str]]:
    """
    Find all Item section markers and their positions in the text.
    Returns list of (item_number, char_position, title) tuples.
    """
    # Pattern to match "ITEM X" or "ITEM XA" headers
    item_pattern = re.compile(
        r'\bITEM\s+(\d+[A-Z]?)[\.\s\-:]+([A-Z][^\n]{5,80})',
        re.IGNORECASE
    )

    sections = []
    seen_items = set()

    for match in item_pattern.finditer(text):
        item_num = match.group(1).upper()
        title = match.group(2).strip()
        position = match.start()

        # Only include standard 10-K items, skip duplicates
        if item_num in STANDARD_ITEMS and item_num not in seen_items:
            # Skip table of contents entries (usually very short titles)
            if len(title) > 5:
                sections.append((item_num, position, title))
                seen_items.add(item_num)

    # Sort by position
    sections.sort(key=lambda x: x[1])
    return sections


def get_section_for_position(position: int, sections: List[Tuple[str, int, str]]) -> str:
    """
    Given a character position and list of section boundaries,
    return which section the position falls into (with readable name).
    """
    if not sections:
        return "Unknown"

    current_item = None

    for item_num, section_pos, title in sections:
        if position < section_pos:
            break
        current_item = item_num

    if current_item is None:
        return "Before Item 1"

    # Return readable name from mapping
    readable_name = STANDARD_ITEMS.get(current_item, "Unknown")
    return f"Item {current_item}: {readable_name}"


# =============================================================================
# TEXT EXTRACTION
# =============================================================================


def strip_html_tags(text: str) -> str:
    """
    Remove HTML tags and decode entities using BeautifulSoup with lxml parser.
    Preserves paragraph boundaries and removes table junk.
    """
    try:
        # Parse HTML with BeautifulSoup using lxml parser
        print(f"        DEBUG: BeautifulSoup parsing {len(text)} chars...")
        sys.stdout.flush()
        soup = BeautifulSoup(text, "lxml")
        print(f"        DEBUG: BeautifulSoup parsing complete")
        sys.stdout.flush()

        # Remove unwanted tags entirely
        print(f"        DEBUG: Removing unwanted tags...")
        sys.stdout.flush()
        for tag in soup(["script", "style", "noscript", "svg", "head", "table"]):
            tag.decompose()

        # Convert <br> tags to newlines
        print(f"        DEBUG: Converting br tags...")
        sys.stdout.flush()
        for br in soup.find_all("br"):
            br.replace_with("\n")

        # Get text with newline separators to preserve paragraph structure
        print(f"        DEBUG: Extracting text...")
        sys.stdout.flush()
        text = soup.get_text(separator="\n")
        print(f"        DEBUG: Text extraction complete")
        sys.stdout.flush()

        # Normalize whitespace while keeping paragraphs
        text = re.sub(r"\n{3,}", "\n\n", text)

        # Collapse spaces and tabs on each line
        lines = text.split("\n")
        lines = [re.sub(r"[ \t]+", " ", line.strip()) for line in lines]
        text = "\n".join(lines)

        text = text.strip()

    except Exception:
        # Fallback to basic regex if BeautifulSoup fails
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text)
        text = text.strip()

    # Additional cleaning: decode HTML entities
    text = text.replace("&nbsp;", " ")
    text = text.replace("&amp;", "&")
    text = text.replace("&lt;", "<")
    text = text.replace("&gt;", ">")
    text = text.replace("&quot;", '"')
    text = text.replace("&#39;", "'")
    text = text.replace("&#59;", ";")
    text = text.replace("&apos;", "'")

    # Decode numeric entities
    text = re.sub(r"&#(\d+);", lambda m: chr(int(m.group(1))), text)
    text = re.sub(r"&#x([0-9a-fA-F]+);", lambda m: chr(int(m.group(1), 16)), text)

    # Final whitespace cleanup
    text = re.sub(r"\s+", " ", text)
    text = text.strip()

    return text


def read_file_text(filepath: pathlib.Path) -> str:
    """Read file content with error handling and HTML stripping."""
    try:
        # Check file size first
        file_size_mb = filepath.stat().st_size / (1024 * 1024)
        if file_size_mb > MAX_FILE_SIZE_MB:
            print(f"      DEBUG: File too large ({file_size_mb:.1f} MB > {MAX_FILE_SIZE_MB} MB) - SKIPPING")
            sys.stdout.flush()
            return ""

        print(f"      DEBUG: Reading raw file ({file_size_mb:.1f} MB)...")
        sys.stdout.flush()
        text = filepath.read_text(encoding="utf-8", errors="ignore")
        print(f"      DEBUG: Raw file size: {len(text)} chars")
        sys.stdout.flush()
    except Exception:
        try:
            text = filepath.read_text(encoding="latin-1", errors="ignore")
        except Exception:
            return ""

    # Full 10-K .txt files contain HTML/SGML - strip it
    if filepath.suffix.lower() in [".txt", ".htm", ".html"]:
        print(f"      DEBUG: Stripping HTML (this can be slow for large files)...")
        sys.stdout.flush()
        text = strip_html_tags(text)
        print(f"      DEBUG: After HTML strip: {len(text)} chars")
        sys.stdout.flush()

    return text


# =============================================================================
# SNIPPET EXTRACTION (paragraph-based approach)
# =============================================================================


def split_into_sentences(text: str) -> list:
    """Simple sentence splitter."""
    sentences = re.split(r"(?<=[.!?])\s+", text)
    return [s.strip() for s in sentences if s.strip()]


def is_noisy_paragraph(text: str) -> bool:
    """
    Check if paragraph looks like noise/boilerplate/table data.
    Returns True if paragraph should be skipped.
    """
    # Too short
    if len(text.strip()) < MIN_PARAGRAPH_LENGTH:
        return True

    # Check against noise patterns
    for pattern in NOISE_PATTERNS:
        if pattern.search(text):
            return True

    # Calculate letter ratio
    alpha_count = sum(c.isalpha() for c in text)
    total_count = len(text)
    if total_count > 0 and alpha_count / total_count < 0.25:
        return True

    # Check for table separators
    pipe_count = text.count("|")
    if pipe_count > 3:
        return True

    # Check for columnar data patterns
    digit_sequences = re.findall(r"\d+", text)
    if len(digit_sequences) > 10 and len(text) < 500:
        return True

    return False


def extract_snippet_with_section(text: str, sections: List[Tuple[str, int, str]]) -> Optional[dict]:
    """
    Extract a clean snippet from text using paragraph-first approach.
    Returns dict with snippet and section info, or None if no valid snippet found.
    """
    # Split into paragraphs
    paragraphs = text.split("\n\n")
    paragraphs = [p.strip() for p in paragraphs if p.strip()]

    if not paragraphs:
        return None

    # Track cumulative position for section detection
    cumulative_pos = 0

    # Find first clean paragraph with keyword
    for para in paragraphs:
        if not KEYWORDS.search(para) or is_noisy_paragraph(para):
            cumulative_pos += len(para) + 2  # +2 for \n\n
            continue

        # Found a good paragraph - extract snippet
        match = KEYWORDS.search(para)
        if not match:
            cumulative_pos += len(para) + 2
            continue

        keyword = match.group(0).lower()

        # Get section based on position in original text
        keyword_pos = cumulative_pos + match.start()
        section = get_section_for_position(keyword_pos, sections)

        # Split paragraph into sentences
        sentences = split_into_sentences(para)
        if not sentences:
            cumulative_pos += len(para) + 2
            continue

        # Find anchor sentence (contains keyword)
        anchor_idx = None
        for i, sentence in enumerate(sentences):
            if KEYWORDS.search(sentence):
                anchor_idx = i
                break

        if anchor_idx is None:
            cumulative_pos += len(para) + 2
            continue

        # Build snippet: anchor + 1 before + 1 after
        start_idx = max(0, anchor_idx - 1)
        end_idx = min(len(sentences), anchor_idx + 2)

        snippet_sentences = sentences[start_idx:end_idx]
        snippet = " ".join(snippet_sentences)

        # Truncate if too long
        if len(snippet) > MAX_SNIPPET_LENGTH:
            snippet = snippet[:MAX_SNIPPET_LENGTH] + "..."

        # Final verification: keyword must be in snippet
        if not KEYWORDS.search(snippet):
            cumulative_pos += len(para) + 2
            continue

        return {
            "section": section,
            "keyword": keyword,
            "snippet": snippet.strip(),
        }

    return None


def parse_filename(filename: str) -> Optional[dict]:
    """Parse filename format: {CIK}_{FORM_TYPE}_{FILING_DATE}_{ACCESSION}.ext"""
    try:
        name = filename.replace(".txt", "").replace(".html", "").replace(".htm", "")
        parts = name.split("_")
        if len(parts) < 3:
            return None

        return {
            "cik": parts[0],
            "form_type": parts[1],
            "filing_date": parts[2],
        }
    except Exception:
        return None


# =============================================================================
# COMPANY INFO
# =============================================================================


def load_company_names() -> dict:
    """Load CIK to company name mapping from Excel."""
    try:
        df = pd.read_excel(DATA_DIR / "Publicly_Trade_Companies_SEC.xlsx", engine="openpyxl")

        cik_col = None
        for col in df.columns:
            if "cik" in col.lower():
                cik_col = col
                break

        name_col = None
        for col in df.columns:
            if any(x in col.lower() for x in ["name", "company"]):
                name_col = col
                break

        if not cik_col or not name_col:
            return {}

        cik_to_name = {}
        for _, row in df.iterrows():
            try:
                cik_raw = str(row[cik_col]).replace(".0", "")
                cik_raw = "".join(c for c in cik_raw if c.isdigit())

                if not cik_raw:
                    continue

                company_name = str(row[name_col])
                if company_name and company_name != "nan":
                    cik_to_name[cik_raw] = company_name
                    cik_to_name[cik_raw.zfill(10)] = company_name
                    cik_to_name[cik_raw.lstrip("0") or "0"] = company_name
            except Exception:
                continue

        print(f"Loaded {len(set(cik_to_name.values()))} company names")
        return cik_to_name
    except Exception as e:
        print(f"Warning: Could not load company names: {e}")
        return {}


def get_sic_from_folder(cik: str) -> str:
    """Get SIC code from main OneDrive folder (not full_10ks)."""
    try:
        cik_padded = cik.zfill(10)
        sic_file = CLOUD_FOLDER / cik_padded / "SIC.txt"
        if sic_file.exists():
            return sic_file.read_text(encoding="utf-8").strip()
    except Exception:
        pass
    return ""


# =============================================================================
# PROCESSING
# =============================================================================


def find_snippet_for_company(cik_folder: pathlib.Path) -> Optional[dict]:
    """
    Find one clean snippet from a company's full 10-K filings.
    Returns dict with snippet info or None if no valid snippet found.
    """
    # Get all .txt files in CIK folder (full 10-K filings)
    files = list(cik_folder.glob("*.txt"))
    if not files:
        return None

    # Sort by date descending (most recent first)
    files.sort(key=lambda f: f.name, reverse=True)

    # Try each file until we find a clean snippet
    for filepath in files:
        print(f"  DEBUG: Processing file {filepath.name}...")
        sys.stdout.flush()
        file_info = parse_filename(filepath.name)
        if not file_info:
            continue

        # Read file
        print(f"    DEBUG: Reading file...")
        text = read_file_text(filepath)
        print(f"    DEBUG: Read {len(text) if text else 0} chars")
        if not text or len(text) < 1000:
            print(f"    DEBUG: Skipping (too short or empty)")
            continue

        # Find section positions
        print(f"    DEBUG: Finding section positions...")
        sections = find_section_positions(text)
        print(f"    DEBUG: Found {len(sections)} sections")

        # Check if file has any keyword matches at all
        keyword_matches = KEYWORDS.findall(text)
        if not keyword_matches:
            print(f"    DEBUG: No keywords found in file - skipping")
            continue
        print(f"    DEBUG: Found {len(keyword_matches)} keyword matches: {set(m.lower() for m in keyword_matches[:5])}")

        # Extract snippet with section
        print(f"    DEBUG: Extracting snippet...")
        result = extract_snippet_with_section(text, sections)
        print(f"    DEBUG: Snippet result: {'Found!' if result else 'None (noisy paragraphs?)'}")
        if result:
            return {
                "cik": file_info["cik"],
                "form_type": file_info["form_type"],
                "filing_date": file_info["filing_date"],
                "section": result["section"],
                "keyword": result["keyword"],
                "snippet": result["snippet"],
                "source_file": filepath.name,
            }

    return None


# =============================================================================
# MAIN
# =============================================================================


def main():
    random.seed(RANDOM_SEED)

    print("=" * 60)
    print("Generating 10-K Snippets with Section Labels for Classification")
    print("=" * 60)
    print(f"Target sample size: {TARGET_SAMPLE_SIZE} companies (one snippet each)")

    # Load company names
    print("\nLoading company names...")
    cik_to_name = load_company_names()

    # Check for full_10ks folder
    if not FULL_10K_FOLDER.exists():
        print(f"\nERROR: Full 10-K folder not found: {FULL_10K_FOLDER}")
        print("Please run download_full_10ks.py first, then sync the folder locally.")
        return

    # Find CIK folders
    print(f"\nScanning: {FULL_10K_FOLDER}")
    cik_folders = [f for f in FULL_10K_FOLDER.iterdir() if f.is_dir()]
    print(f"Found {len(cik_folders)} CIK folders")

    if len(cik_folders) == 0:
        print("No CIK folders found. Ensure full 10-Ks have been downloaded.")
        return

    # Shuffle for random sampling (with seed for reproducibility)
    random.shuffle(cik_folders)

    # Process folders until we reach target sample size
    snippets = []
    processed = 0

    for cik_folder in cik_folders:
        if len(snippets) >= TARGET_SAMPLE_SIZE:
            break

        processed += 1
        print(f"\nDEBUG: [{processed}/{len(cik_folders)}] Processing folder: {cik_folder.name}")
        sys.stdout.flush()

        # Find snippet for this company
        snippet_data = find_snippet_for_company(cik_folder)
        if snippet_data:
            cik = snippet_data["cik"]

            # Get company name and SIC
            snippet_data["company_name"] = cik_to_name.get(
                cik, cik_to_name.get(cik.zfill(10), "")
            )
            sic = get_sic_from_folder(cik)
            snippet_data["sic"] = sic
            snippet_data["sic2"] = sic[:2] if len(sic) >= 2 else ""

            snippets.append(snippet_data)
            print(f"DEBUG: Total snippets so far: {len(snippets)}")

    print(f"\nTotal folders processed: {processed}")
    print(f"Total snippets extracted: {len(snippets)}")

    if len(snippets) < TARGET_SAMPLE_SIZE:
        print(
            f"Warning: Only found {len(snippets)} companies with valid snippets "
            f"(target was {TARGET_SAMPLE_SIZE})"
        )

    if not snippets:
        print("No snippets found!")
        return

    # Create DataFrame
    df = pd.DataFrame(snippets)

    # Add empty Classification column for manual coding
    df["classification"] = ""

    # Reorder columns
    columns = [
        "company_name",
        "cik",
        "sic",
        "sic2",
        "form_type",
        "filing_date",
        "section",
        "keyword",
        "snippet",
        "classification",
    ]
    df = df[[c for c in columns if c in df.columns]]

    # Rename columns for readability
    df.columns = [
        "Company Name",
        "CIK",
        "SIC",
        "SIC2",
        "Filing Type",
        "Filing Date",
        "Section",
        "Keyword",
        "Snippet",
        "Classification",
    ]

    # Sort by company name
    df = df.sort_values("Company Name")

    # Save to Excel
    print(f"\nSaving to {OUTPUT_FILE}...")
    df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")

    # Summary statistics
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total snippets: {len(df)}")
    print(f"Unique companies: {df['CIK'].nunique()}")

    print("\nSnippets by Section:")
    section_counts = df["Section"].value_counts()
    for section, count in section_counts.items():
        pct = (count / len(df)) * 100
        print(f"  {section}: {count} ({pct:.1f}%)")

    print("\nSnippets by Keyword:")
    keyword_counts = df["Keyword"].value_counts()
    for keyword, count in keyword_counts.head(10).items():
        print(f"  {keyword}: {count}")

    print(f"\n✓ Saved to: {OUTPUT_FILE}")
    print("\nColumns:")
    print("  - Company Name, CIK, SIC, SIC2: Company identifiers")
    print("  - Filing Type, Filing Date: Filing metadata")
    print("  - Section: Which 10-K Item the snippet comes from (e.g., Item 1A)")
    print("  - Keyword, Snippet: The matched keyword and surrounding context")
    print("  - Classification: Empty column for your manual coding")


if __name__ == "__main__":
    main()
