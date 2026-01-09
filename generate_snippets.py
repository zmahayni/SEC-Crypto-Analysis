#!/usr/bin/env python3
"""
Generate clean snippets for qualitative labeling from downloaded SEC filings.
Sample EXACTLY 150 distinct companies with crypto keyword hits.
One snippet per company.
"""

import re
import pathlib
import pandas as pd
import random
from typing import Optional
from bs4 import BeautifulSoup

# =============================================================================
# CONFIG
# =============================================================================

HOME = pathlib.Path.home()
CLOUD_FOLDER = (
    HOME
    / "Library/CloudStorage/OneDrive-UniversityofTulsa/NSF-BSF Precautions - crypto10k"
)
OUTPUT_FILE = pathlib.Path("crypto_snippets_150.xlsx")

# Same keywords as in scan.py (without tokenization)
KEYWORDS = re.compile(
    r"\b("
    r"bitcoin|blockchain|ethereum|cryptocurrency|"
    r"digital[- ]asset|distributed[- ]ledger|non[- ]fungible[- ]token|crypto[- ]asset"
    r")\b",
    re.I,
)

# Snippet parameters
TARGET_SAMPLE_SIZE = 150
MIN_PARAGRAPH_LENGTH = 80
MIN_SNIPPET_LENGTH = 200
MAX_SNIPPET_LENGTH = 800

# Noise patterns to skip
NOISE_PATTERNS = [
    re.compile(r"^(table of contents|item \d+[a-z]?\.?)", re.I),
    re.compile(r"^\s*page \d+", re.I),
    re.compile(r"^\s*\d+\s*$"),  # Just numbers
    re.compile(r"^[\s\-_=]+$"),  # Just whitespace/separators
]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def load_company_names() -> dict:
    """Load CIK to company name mapping from Excel."""
    try:
        df = pd.read_excel("Publicly_Trade_Companies_SEC.xlsx", engine="openpyxl")

        # Find the CIK column (try different possible names)
        cik_col = None
        for col in df.columns:
            if "cik" in col.lower():
                cik_col = col
                break

        if cik_col is None:
            print("Warning: Could not find CIK column in Excel")
            return {}

        # Find the company name column
        name_col = None
        for col in df.columns:
            if any(x in col.lower() for x in ["name", "company"]):
                name_col = col
                break

        if name_col is None:
            print("Warning: Could not find company name column in Excel")
            return {}

        # Build mapping with both padded and stripped CIK formats
        cik_to_name = {}
        for _, row in df.iterrows():
            try:
                cik_raw = str(row[cik_col]).replace(".0", "")  # Remove .0 if present
                cik_raw = "".join(c for c in cik_raw if c.isdigit())  # Keep only digits

                if not cik_raw:
                    continue

                company_name = str(row[name_col])
                if company_name and company_name != "nan":
                    # Store under multiple formats
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


def get_sic2_from_folder(cik: str) -> str:
    """Get SIC2 code from OneDrive folder."""
    try:
        cik_padded = cik.zfill(10)
        sic_file = CLOUD_FOLDER / cik_padded / "SIC.txt"
        if sic_file.exists():
            sic_full = sic_file.read_text(encoding="utf-8").strip()
            return sic_full[:2] if len(sic_full) >= 2 else ""
    except Exception:
        pass
    return ""


def parse_filename(filename: str) -> Optional[dict]:
    """
    Parse filename format: {CIK}_{FORM_TYPE}_{FILING_DATE}_{ACCESSION}.ext
    Returns dict with cik, form_type, filing_date, or None if invalid.
    """
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


def strip_html_tags(text: str) -> str:
    """
    Remove HTML tags and decode entities using BeautifulSoup with lxml parser.
    Preserves paragraph boundaries and removes table junk.
    """
    try:
        # Parse HTML with BeautifulSoup using lxml parser
        soup = BeautifulSoup(text, "lxml")

        # Remove unwanted tags entirely
        for tag in soup(["script", "style", "noscript", "svg", "head", "table"]):
            tag.decompose()

        # Convert <br> tags to newlines
        for br in soup.find_all("br"):
            br.replace_with("\n")

        # Get text with newline separators to preserve paragraph structure
        text = soup.get_text(separator="\n")

        # Normalize whitespace while keeping paragraphs
        # Collapse 3+ newlines to exactly 2 newlines (paragraph break)
        text = re.sub(r"\n{3,}", "\n\n", text)

        # Collapse spaces and tabs on each line
        lines = text.split("\n")
        lines = [re.sub(r"[ \t]+", " ", line.strip()) for line in lines]
        text = "\n".join(lines)

        # Remove empty lines at start/end
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

    # Remove font/style attributes that might have leaked through
    text = re.sub(r"font-[a-z\-]+:[^;]+;?", " ", text)
    text = re.sub(r"color:#[0-9a-fA-F]+;?", " ", text)
    text = re.sub(r'style="[^"]*"', " ", text)

    # Final whitespace cleanup
    text = re.sub(r"\s+", " ", text)
    text = text.strip()

    return text


def read_file_text(filepath: pathlib.Path) -> str:
    """Read file content with error handling and HTML stripping."""
    try:
        text = filepath.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        try:
            text = filepath.read_text(encoding="latin-1", errors="ignore")
        except Exception:
            return ""

    # Strip HTML if it's an HTML file
    if filepath.suffix.lower() in [".htm", ".html"]:
        text = strip_html_tags(text)

    return text


def split_into_sentences(text: str) -> list:
    """
    Simple sentence splitter.
    Splits on period, exclamation, question mark followed by space/newline.
    """
    # Basic sentence boundary detection
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

    # Check against noise patterns (headings, TOC, page numbers)
    for pattern in NOISE_PATTERNS:
        if pattern.search(text):
            return True

    # Calculate letter ratio
    alpha_count = sum(c.isalpha() for c in text)
    total_count = len(text)
    if total_count > 0 and alpha_count / total_count < 0.25:
        return True

    # Check for table separators (pipes, excessive tabs)
    pipe_count = text.count("|")
    if pipe_count > 3:  # Likely a table
        return True

    # Check for columnar data patterns (lots of numbers with spaces)
    # Count digit sequences separated by whitespace
    digit_sequences = re.findall(r"\d+", text)
    if len(digit_sequences) > 10 and len(text) < 500:
        return True

    return False


def extract_snippet(text: str, keyword_match: str) -> Optional[str]:
    """
    Extract a clean snippet around a keyword match using paragraph-first approach.

    Strategy:
    1. Split text into paragraphs (by double newline)
    2. Find first paragraph that contains keyword AND is not noisy
    3. Within that paragraph, split into sentences
    4. Extract: anchor sentence (with keyword) + 1 before + 1 after
    5. If too short, append next paragraph if it's also clean
    """
    # Split into paragraphs
    paragraphs = text.split("\n\n")
    paragraphs = [p.strip() for p in paragraphs if p.strip()]

    if not paragraphs:
        return None

    # Find first clean paragraph with keyword
    target_para_idx = None
    for i, para in enumerate(paragraphs):
        if KEYWORDS.search(para) and not is_noisy_paragraph(para):
            target_para_idx = i
            break

    if target_para_idx is None:
        return None

    target_para = paragraphs[target_para_idx]

    # Split paragraph into sentences
    sentences = split_into_sentences(target_para)
    if not sentences:
        return None

    # Find anchor sentence (contains keyword)
    anchor_idx = None
    for i, sentence in enumerate(sentences):
        if KEYWORDS.search(sentence):
            anchor_idx = i
            break

    if anchor_idx is None:
        return None

    # Build snippet: anchor + 1 before + 1 after (within same paragraph)
    start_idx = max(0, anchor_idx - 1)
    end_idx = min(len(sentences), anchor_idx + 2)

    snippet_sentences = sentences[start_idx:end_idx]
    snippet = " ".join(snippet_sentences)

    # If too short, try to append next paragraph
    if len(snippet) < MIN_SNIPPET_LENGTH and target_para_idx + 1 < len(paragraphs):
        next_para = paragraphs[target_para_idx + 1]
        if not is_noisy_paragraph(next_para):
            snippet = snippet + " " + next_para

    # Truncate if too long
    if len(snippet) > MAX_SNIPPET_LENGTH:
        snippet = snippet[:MAX_SNIPPET_LENGTH]

    # Final verification: keyword must be in snippet
    if not KEYWORDS.search(snippet):
        return None

    return snippet.strip()


def find_snippet_for_company(cik_folder: pathlib.Path) -> Optional[dict]:
    """
    Find one clean snippet from a company's filings.
    Returns dict with snippet info or None if no valid snippet found.
    """
    # Get all text files in CIK folder
    files = []
    for ext in [".txt", ".htm", ".html"]:
        files.extend(cik_folder.glob(f"*{ext}"))

    if not files:
        return None

    # Prefer .txt files (master documents), then .htm/.html
    files.sort(key=lambda f: (f.suffix != ".txt", f.name))

    # Try each file until we find a clean snippet
    for filepath in files:
        # Parse filename
        file_info = parse_filename(filepath.name)
        if not file_info:
            continue

        # Read file
        text = read_file_text(filepath)
        if not text:
            continue

        # Check for keyword match
        match = KEYWORDS.search(text)
        if not match:
            continue

        # Extract snippet
        snippet = extract_snippet(text, match.group(0))
        if snippet:
            return {
                "cik": file_info["cik"],
                "form_type": file_info["form_type"],
                "filing_date": file_info["filing_date"],
                "keyword": match.group(0).lower(),
                "snippet": snippet,
                "source_file": filepath.name,
            }

    return None


# =============================================================================
# MAIN
# =============================================================================


def main():
    print("Generating clean snippets for qualitative labeling...")
    print(f"Target sample size: {TARGET_SAMPLE_SIZE} companies")

    # Load company names
    print("\nLoading company names...")
    cik_to_name = load_company_names()

    # Walk OneDrive folder and find companies with hits
    print(f"\nScanning folder: {CLOUD_FOLDER}")

    cik_folders = [f for f in CLOUD_FOLDER.iterdir() if f.is_dir()]
    print(f"Found {len(cik_folders)} CIK folders")

    # Shuffle to randomize sampling
    random.shuffle(cik_folders)

    snippets = []
    processed = 0

    for cik_folder in cik_folders:
        if len(snippets) >= TARGET_SAMPLE_SIZE:
            break

        processed += 1
        if processed % 100 == 0:
            print(f"Processed {processed} folders, found {len(snippets)} snippets...")

        # Find snippet for this company
        snippet_data = find_snippet_for_company(cik_folder)
        if snippet_data:
            cik = snippet_data["cik"]

            # Get company name and SIC2
            snippet_data["company_name"] = cik_to_name.get(
                cik, cik_to_name.get(cik.zfill(10), "")
            )
            snippet_data["sic2"] = get_sic2_from_folder(cik)

            snippets.append(snippet_data)

    print(f"\nTotal folders processed: {processed}")
    print(f"Total snippets extracted: {len(snippets)}")

    if len(snippets) < TARGET_SAMPLE_SIZE:
        print(
            f"Warning: Only found {len(snippets)} companies with valid snippets (target was {TARGET_SAMPLE_SIZE})"
        )

    # Create DataFrame
    df = pd.DataFrame(snippets)

    # Add empty Classification column
    df["classification"] = ""

    # Reorder columns (include company_name)
    df = df[
        [
            "company_name",
            "cik",
            "sic2",
            "form_type",
            "filing_date",
            "keyword",
            "snippet",
            "classification",
        ]
    ]
    df.columns = [
        "Company Name",
        "CIK",
        "SIC2",
        "Filing Type",
        "Filing Date",
        "Keyword",
        "Snippet",
        "Classification",
    ]

    # Save to Excel
    df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")

    print("\n✓ Saved to:", OUTPUT_FILE)
    print("Total snippets:", len(df))
    print("\nKeyword distribution:")
    print(df["Keyword"].value_counts())
    print("\nSample snippet:")
    if len(df) > 0:
        print("Company:", df.iloc[0]["Company Name"])
        print("CIK:", df.iloc[0]["CIK"])
        print("SIC2:", df.iloc[0]["SIC2"])
        print("Keyword:", df.iloc[0]["Keyword"])
        print("Snippet:", df.iloc[0]["Snippet"][:200], "...")


if __name__ == "__main__":
    random.seed(42)  # For reproducibility
    main()
