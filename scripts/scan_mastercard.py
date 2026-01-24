#!/usr/bin/env python3
"""
Scan Mastercard (CIK: 1141391) for crypto keywords in SEC filings.
Follows the same process as scan.py but for a single company.
"""

import re
import time
from pathlib import Path
from datetime import datetime
import requests
import json

# =============================================================================
# CONFIG
# =============================================================================

HOME = Path.home()
BASE_FOLDER = (
    HOME
    / "Library/CloudStorage/OneDrive-UniversityofTulsa/NSF-BSF Precautions - crypto10k"
)
TMP_ROOT = HOME / "edgar_tmp"
STAGE_DIR = TMP_ROOT / "stage"

MASTERCARD_CIK = "1141391"
COMPANY_NAME = "Mastercard"

YEARS_BACK = 5
FORMS = {"10-K", "10-Q", "8-K", "20-F", "40-F", "6-K"}

# Keywords (same as scan.py)
KEYWORDS = re.compile(
    r"\b("
    r"bitcoin|blockchain|ethereum|cryptocurrency|"
    r"digital[- ]asset|distributed[- ]ledger|non[- ]fungible[- ]token|crypto[- ]asset"
    r")\b",
    re.I,
)

MAX_RPS = 9.8
MAX_SAVE_MB_PER_FILE = 20

# SEC API headers
HEADERS = {
    "User-Agent": "University research project zade.mahayni@utulsa.edu",
    "Accept-Encoding": "gzip, deflate",
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def fetch_url(url: str) -> str:
    """Fetch URL content with rate limiting."""
    time.sleep(1.0 / MAX_RPS)

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            return resp.text
    except Exception as e:
        print(f"Error fetching {url}: {e}")

    return ""


def has_keyword(text: str) -> bool:
    """Check if text contains any keyword."""
    return bool(KEYWORDS.search(text))


def save_filing(
    cik: str, form_type: str, filing_date: str, accession: str, content: str
):
    """Save filing to disk."""
    cik_folder = BASE_FOLDER / cik.zfill(10)
    cik_folder.mkdir(parents=True, exist_ok=True)

    # Save SIC if not exists (placeholder)
    sic_file = cik_folder / "SIC.txt"
    if not sic_file.exists():
        sic_file.write_text("6199", encoding="utf-8")  # Credit card services

    # Save filing
    filename = f"{cik.zfill(10)}_{form_type}_{filing_date}_{accession}.txt"
    filepath = cik_folder / filename

    # Check size
    size_mb = len(content.encode("utf-8")) / (1024 * 1024)
    if size_mb > MAX_SAVE_MB_PER_FILE:
        print(f"  Skipping {filename} (too large: {size_mb:.1f} MB)")
        return

    filepath.write_text(content, encoding="utf-8")
    print(f"  Saved: {filename}")


def process_filing(cik: str, form_type: str, filing_date: str, accession: str):
    """Process a single filing."""
    # Build primary document URL
    accession_no_dash = accession.replace("-", "")
    base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_dash}"

    # Try to get the primary document
    primary_url = f"{base_url}/{accession}.txt"
    content = fetch_url(primary_url)

    if content and has_keyword(content):
        save_filing(cik, form_type, filing_date, accession, content)
        return True

    return False


def scan_mastercard():
    """Scan Mastercard for crypto keywords."""
    print(f"Scanning Mastercard (CIK: {MASTERCARD_CIK})")
    print("=" * 70)

    print("\nFetching recent filings...")

    now_year = datetime.now().year

    # Get submissions data
    submissions_url = (
        f"https://data.sec.gov/submissions/CIK{MASTERCARD_CIK.zfill(10)}.json"
    )
    submissions_text = fetch_url(submissions_url)

    if not submissions_text:
        print("Error: Could not fetch submissions data")
        return

    submissions = json.loads(submissions_text)

    recent = submissions.get("filings", {}).get("recent", {})

    if not recent:
        print("No recent filings found")
        return

    # Filter filings
    filings = []
    for i in range(len(recent.get("form", []))):
        form_type = recent["form"][i]
        filing_date = recent["filingDate"][i]
        accession = recent["accessionNumber"][i]

        year = int(filing_date[:4])

        if form_type in FORMS and year >= now_year - YEARS_BACK:
            filings.append(
                {
                    "form_type": form_type,
                    "filing_date": filing_date,
                    "accession": accession,
                }
            )

    print(f"Found {len(filings)} relevant filings")

    # Process filings
    hits = 0
    for i, filing in enumerate(filings, 1):
        print(
            f"\n[{i}/{len(filings)}] Processing {filing['form_type']} from {filing['filing_date']}..."
        )

        has_hit = process_filing(
            MASTERCARD_CIK,
            filing["form_type"],
            filing["filing_date"],
            filing["accession"],
        )

        if has_hit:
            hits += 1

    print("\n" + "=" * 70)
    print("Scan complete!")
    print(f"Total filings processed: {len(filings)}")
    print(f"Filings with crypto keywords: {hits}")
    print(f"Saved to: {BASE_FOLDER / MASTERCARD_CIK.zfill(10)}")


def main():
    scan_mastercard()


if __name__ == "__main__":
    main()
