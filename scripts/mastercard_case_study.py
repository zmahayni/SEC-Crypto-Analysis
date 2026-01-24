#!/usr/bin/env python3
"""
Mastercard Case Study: Extract all crypto-related snippets from saved filings (2020-2025).
Creates a longitudinal qualitative record for timeline visualization and manual labeling.
"""

import re
import pathlib
import pandas as pd
from bs4 import BeautifulSoup
from typing import Optional, List

# =============================================================================
# CONFIG
# =============================================================================

SCRIPT_DIR = pathlib.Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data"

HOME = pathlib.Path.home()
CLOUD_FOLDER = (
    HOME
    / "Library/CloudStorage/OneDrive-UniversityofTulsa/NSF-BSF Precautions - crypto10k"
)

MASTERCARD_CIK = "0001141391"
COMPANY_NAME = "Mastercard"
OUTPUT_FILE = DATA_DIR / "mastercard_crypto_snippets.xlsx"

# Same keywords as in scan.py
KEYWORDS = re.compile(
    r"\b("
    r"bitcoin|blockchain|ethereum|cryptocurrency|"
    r"digital[- ]asset|distributed[- ]ledger|non[- ]fungible[- ]token|crypto[- ]asset"
    r")\b",
    re.I,
)

MIN_PARAGRAPH_LENGTH = 80

# Noise patterns
NOISE_PATTERNS = [
    re.compile(r"^(table of contents|item \d+[a-z]?\.?)", re.I),
    re.compile(r"^\s*page \d+", re.I),
    re.compile(r"^\s*\d+\s*$"),
    re.compile(r"^[\s\-_=]+$"),
]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def strip_html_tags(text: str) -> str:
    """Remove HTML tags and decode entities using BeautifulSoup."""
    try:
        soup = BeautifulSoup(text, "lxml")

        # Remove unwanted tags
        for tag in soup(["script", "style", "noscript", "svg", "head", "table"]):
            tag.decompose()

        # Convert <br> to newlines
        for br in soup.find_all("br"):
            br.replace_with("\n")

        # Get text
        text = soup.get_text(separator="\n")

        # Normalize whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        lines = text.split("\n")
        lines = [re.sub(r"[ \t]+", " ", line.strip()) for line in lines]
        text = "\n".join(lines)

        text = text.strip()

    except Exception:
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
    """Read file content with HTML stripping."""
    try:
        text = filepath.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        try:
            text = filepath.read_text(encoding="latin-1", errors="ignore")
        except Exception:
            return ""

    # Strip HTML if needed
    if filepath.suffix.lower() in [".htm", ".html"]:
        text = strip_html_tags(text)

    return text


def split_into_sentences(text: str) -> List[str]:
    """Split text into sentences."""
    sentences = re.split(r"(?<=[.!?])\s+", text)
    return [s.strip() for s in sentences if s.strip()]


def is_noisy_paragraph(text: str) -> bool:
    """Check if paragraph is noise/boilerplate."""
    if len(text.strip()) < MIN_PARAGRAPH_LENGTH:
        return True

    for pattern in NOISE_PATTERNS:
        if pattern.search(text):
            return True

    alpha_count = sum(c.isalpha() for c in text)
    total_count = len(text)
    if total_count > 0 and alpha_count / total_count < 0.25:
        return True

    if text.count("|") > 3:
        return True

    digit_sequences = re.findall(r"\d+", text)
    if len(digit_sequences) > 10 and len(text) < 500:
        return True

    return False


def extract_all_snippets(text: str) -> List[tuple]:
    """
    Extract ALL snippets containing keywords from text.
    Returns list of (keyword, snippet) tuples.
    """
    snippets = []

    # Split into paragraphs
    paragraphs = text.split("\n\n")
    paragraphs = [p.strip() for p in paragraphs if p.strip()]

    for para_idx, para in enumerate(paragraphs):
        # Skip noisy paragraphs
        if is_noisy_paragraph(para):
            continue

        # Check if paragraph contains keyword
        if not KEYWORDS.search(para):
            continue

        # Split into sentences
        sentences = split_into_sentences(para)
        if not sentences:
            continue

        # Find all sentences with keywords
        for sent_idx, sentence in enumerate(sentences):
            match = KEYWORDS.search(sentence)
            if match:
                # Extract snippet: anchor + 1 before + 1 after
                start_idx = max(0, sent_idx - 1)
                end_idx = min(len(sentences), sent_idx + 2)

                snippet_sentences = sentences[start_idx:end_idx]
                snippet = " ".join(snippet_sentences)

                # Verify keyword is in snippet
                if KEYWORDS.search(snippet):
                    keyword = match.group(0).lower()
                    snippets.append((keyword, snippet.strip()))

    return snippets


def parse_filename(filename: str) -> Optional[dict]:
    """Parse filing filename to extract metadata."""
    try:
        name = filename.replace(".txt", "").replace(".html", "").replace(".htm", "")
        parts = name.split("_")
        if len(parts) < 3:
            return None

        return {
            "form_type": parts[1],
            "filing_date": parts[2],
        }
    except Exception:
        return None


# =============================================================================
# MAIN
# =============================================================================


def main():
    print(f"Mastercard Case Study: Extracting all crypto snippets")
    print("=" * 70)

    # Find Mastercard folder
    mastercard_folder = CLOUD_FOLDER / MASTERCARD_CIK

    if not mastercard_folder.exists():
        print(f"Error: Mastercard folder not found at {mastercard_folder}")
        return

    print(f"\nScanning folder: {mastercard_folder}")

    # Get all text files
    files = []
    for ext in [".txt", ".htm", ".html"]:
        files.extend(mastercard_folder.glob(f"*{ext}"))

    print(f"Found {len(files)} files")

    # Extract snippets from all files
    all_snippets = []

    for filepath in sorted(files):
        # Parse filename
        file_info = parse_filename(filepath.name)
        if not file_info:
            continue

        # Read file
        text = read_file_text(filepath)
        if not text:
            continue

        # Extract all snippets
        snippets = extract_all_snippets(text)

        if snippets:
            print(f"  {filepath.name}: {len(snippets)} snippets")

            for keyword, snippet in snippets:
                all_snippets.append(
                    {
                        "Company Name": COMPANY_NAME,
                        "CIK": MASTERCARD_CIK,
                        "Filing Type": file_info["form_type"],
                        "Filing Date": file_info["filing_date"],
                        "Keyword": keyword,
                        "Snippet": snippet,
                    }
                )

    print(f"\nTotal snippets extracted: {len(all_snippets)}")

    if not all_snippets:
        print("No snippets found!")
        return

    # Create DataFrame
    df = pd.DataFrame(all_snippets)

    # Sort by filing date
    df = df.sort_values("Filing Date").reset_index(drop=True)

    # Save to Excel
    df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")

    print(f"\n✓ Saved to: {OUTPUT_FILE}")
    print(f"\nKeyword distribution:")
    print(df["Keyword"].value_counts())
    print(f"\nFilings by year:")
    df["Year"] = df["Filing Date"].str[:4]
    print(df["Year"].value_counts().sort_index())
    print(f"\nSample snippet:")
    if len(df) > 0:
        print(f"Date: {df.iloc[0]['Filing Date']}")
        print(f"Type: {df.iloc[0]['Filing Type']}")
        print(f"Keyword: {df.iloc[0]['Keyword']}")
        print(f"Snippet: {df.iloc[0]['Snippet'][:300]}...")


if __name__ == "__main__":
    main()
