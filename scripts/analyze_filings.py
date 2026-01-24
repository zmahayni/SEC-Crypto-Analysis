#!/usr/bin/env python3
"""
Analyze saved SEC filings for crypto keywords and generate Excel report.
One row per (filing × keyword) combination.
"""

import re
import pathlib
import pandas as pd
from collections import defaultdict
from typing import Dict, Set, Tuple

# =============================================================================
# CONFIG
# =============================================================================

SCRIPT_DIR = pathlib.Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data"

HOME = pathlib.Path.home()
INPUT_XLSX = DATA_DIR / "Publicly_Trade_Companies_SEC.xlsx"
CLOUD_FOLDER = (
    HOME
    / "Library/CloudStorage/OneDrive-UniversityofTulsa/NSF-BSF Precautions - crypto10k"
)
OUTPUT_XLSX = DATA_DIR / "crypto_keyword_hits.xlsx"

# Same keywords as in scan.py
KEYWORDS = re.compile(
    r"\b("
    r"bitcoin|blockchain|ethereum|cryptocurrency|"
    r"digital[- ]asset|distributed[- ]ledger|non[- ]fungible[- ]token|crypto[- ]asset"
    r")\b",
    re.I,
)

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def load_company_names() -> Dict[str, str]:
    """Load CIK -> Company Name mapping from input Excel."""
    try:
        df = pd.read_excel(INPUT_XLSX, engine="openpyxl", dtype=str).rename(
            columns={"CIK": "cik", "Cik": "cik", "name": "name", "Name": "name"}
        )
        df = df.dropna(subset=["cik"]).assign(
            cik=lambda d: d["cik"]
            .astype(str)
            .str.replace(r"\D", "", regex=True)
            .str.zfill(10),
            name=lambda d: d.get("name", "").astype(str).fillna(""),
        )
        df = df.drop_duplicates(subset=["cik"]).reset_index(drop=True)
        return dict(zip(df["cik"], df["name"]))
    except Exception as e:
        print(f"Warning: Could not load company names: {e}")
        return {}


def parse_filename(filename: str) -> Tuple[str, str, str, str] | None:
    """
    Parse filename format: {CIK}_{FORM_TYPE}_{FILING_DATE}_{ACCESSION_NUMBER}.txt
    Returns: (cik, form_type, filing_date, accession_number) or None if invalid
    """
    try:
        # Remove .txt extension
        name = filename.replace(".txt", "").replace(".html", "").replace(".htm", "")

        parts = name.split("_")
        if len(parts) < 4:
            return None

        cik = parts[0]
        form_type = parts[1]
        filing_date = parts[2]
        accession_number = "_".join(
            parts[3:]
        )  # Handle accession numbers with underscores

        return (cik, form_type, filing_date, accession_number)
    except Exception:
        return None


def extract_keywords_from_text(text: str) -> Set[str]:
    """Extract all matching keywords from text."""
    matches = KEYWORDS.findall(text)
    return set(m.lower() for m in matches)


def read_file_text(filepath: pathlib.Path) -> str:
    """Read file content, handling various encodings."""
    try:
        return filepath.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        try:
            return filepath.read_text(encoding="latin-1", errors="ignore")
        except Exception:
            return ""


def get_sic_for_cik(cik: str, cik_folder: pathlib.Path) -> str:
    """Read SIC code from SIC.txt file."""
    sic_file = cik_folder / "SIC.txt"
    if sic_file.exists():
        try:
            sic = sic_file.read_text(encoding="utf-8").strip()
            # Extract first 4 digits for SIC4
            sic_match = re.match(r"(\d{4})", sic)
            if sic_match:
                return sic_match.group(1)
            return sic[:4] if len(sic) >= 4 else sic
        except Exception:
            return ""
    return ""


# =============================================================================
# MAIN ANALYSIS
# =============================================================================


def analyze_filings() -> pd.DataFrame:
    """
    Walk through cloud folder, analyze files, and return DataFrame.
    One row per (filing × keyword).
    """
    company_names = load_company_names()

    # Track: (cik, form_type, filing_date, accession) -> set of keywords
    filing_keywords: Dict[Tuple[str, str, str, str], Set[str]] = defaultdict(set)

    print(f"Scanning folder: {CLOUD_FOLDER}")

    # Walk through CIK folders
    for cik_folder in sorted(CLOUD_FOLDER.iterdir()):
        if not cik_folder.is_dir():
            continue

        cik = cik_folder.name

        # Walk through files in CIK folder
        for filepath in cik_folder.iterdir():
            if filepath.is_dir():
                continue

            filename = filepath.name

            # Skip SIC.txt and other metadata
            if filename == "SIC.txt" or filename.startswith("."):
                continue

            # Parse filename
            parsed = parse_filename(filename)
            if not parsed:
                continue

            file_cik, form_type, filing_date, accession_number = parsed

            # Read file and extract keywords
            text = read_file_text(filepath)
            keywords = extract_keywords_from_text(text)

            # Only track if keywords found
            if keywords:
                key = (file_cik, form_type, filing_date, accession_number)
                filing_keywords[key].update(keywords)

    # Build output rows
    rows = []
    for (cik, form_type, filing_date, accession_number), keywords in sorted(
        filing_keywords.items()
    ):
        company_name = company_names.get(cik, "")
        sic = get_sic_for_cik(cik, CLOUD_FOLDER / cik)

        # One row per keyword
        for keyword in sorted(keywords):
            rows.append(
                {
                    "Company Name": company_name,
                    "CIK": cik,
                    "SIC": sic,
                    "Filing Type": form_type,
                    "Filing Date": filing_date,
                    "Keyword": keyword,
                }
            )

    return pd.DataFrame(rows)


def main():
    print("Starting SEC filing analysis...")

    # Analyze filings
    df = analyze_filings()

    print(f"\nFound {len(df)} keyword hits across filings")
    print(
        f"Unique filings: {df.groupby(['CIK', 'Filing Type', 'Filing Date']).ngroups}"
    )
    print(f"Unique keywords: {df['Keyword'].nunique()}")

    # Write to Excel
    output_path = OUTPUT_XLSX
    df.to_excel(output_path, index=False, engine="openpyxl")
    print(f"\nReport saved to: {output_path}")
    print(f"Total rows: {len(df)}")

    # Print summary
    print("\nKeyword distribution:")
    print(df["Keyword"].value_counts())

    print("\nFiling type distribution:")
    print(df["Filing Type"].value_counts())


if __name__ == "__main__":
    main()
