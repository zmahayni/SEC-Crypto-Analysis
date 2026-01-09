#!/usr/bin/env python3
"""
Sheet 1: SIC code analysis
Rows = SIC codes
Columns = Total CIKs scanned, CIKs with hits, CIKs without hits, % with hits, % without hits
"""

import pathlib
import pandas as pd

# =============================================================================
# CONFIG
# =============================================================================

HOME = pathlib.Path.home()
PROGRESS_FILE = pathlib.Path("progress.txt")
KEYWORD_HITS_FILE = pathlib.Path("crypto_keyword_hits.xlsx")
OUTPUT_FILE = pathlib.Path("sic_hit_analysis.xlsx")
CLOUD_FOLDER = (
    HOME
    / "Library/CloudStorage/OneDrive-UniversityofTulsa/NSF-BSF Precautions - crypto10k"
)


# =============================================================================
# MAIN ANALYSIS
# =============================================================================


def load_all_ciks_with_sic() -> dict:
    """Load all CIKs from progress.txt and get their SIC codes from OneDrive folder."""
    # Read progress file
    with open(PROGRESS_FILE) as f:
        all_ciks = set(line.strip() for line in f if line.strip())

    print(f"Total CIKs scanned: {len(all_ciks)}")

    # Read SIC codes from OneDrive folder
    cik_to_sic = {}
    for cik in all_ciks:
        # CIK folders are stored with leading zeros (10 digits)
        cik_padded = cik.zfill(10)
        sic_file = CLOUD_FOLDER / cik_padded / "SIC.txt"
        if sic_file.exists():
            try:
                sic = sic_file.read_text(encoding="utf-8").strip()[:4]
                if sic:
                    # Store with normalized CIK (no leading zeros)
                    cik_normalized = cik.lstrip("0") or "0"
                    cik_to_sic[cik_normalized] = sic
            except Exception:
                pass

    print(f"CIKs with SIC data: {len(cik_to_sic)}")
    return cik_to_sic


def load_ciks_with_hits() -> set:
    """Load CIKs that have keyword hits from the keyword hits file."""
    try:
        df = pd.read_excel(KEYWORD_HITS_FILE, engine="openpyxl")
        # Normalize CIK format to match progress.txt (remove leading zeros for comparison)
        ciks_with_hits = set(
            str(cik).lstrip("0") or "0" for cik in df["CIK"].astype(str).unique()
        )
        print(f"CIKs with keyword hits: {len(ciks_with_hits)}")
        return ciks_with_hits
    except Exception as e:
        print(f"Error loading keyword hits: {e}")
        return set()


def create_sic_analysis() -> pd.DataFrame:
    """Create SIC analysis table."""
    # Load all CIKs and their SIC codes
    cik_to_sic = load_all_ciks_with_sic()

    # Load CIKs with hits
    ciks_with_hits = load_ciks_with_hits()

    # Group CIKs by SIC
    sic_data = {}
    for cik, sic in cik_to_sic.items():
        if sic not in sic_data:
            sic_data[sic] = {"total": 0, "with_hits": 0}
        sic_data[sic]["total"] += 1
        if cik in ciks_with_hits:
            sic_data[sic]["with_hits"] += 1

    # Build output rows
    rows = []
    for sic in sorted(sic_data.keys()):
        total = sic_data[sic]["total"]
        with_hits = sic_data[sic]["with_hits"]
        without_hits = total - with_hits
        pct_with_hits = (with_hits / total * 100) if total > 0 else 0
        pct_without_hits = (without_hits / total * 100) if total > 0 else 0

        rows.append(
            {
                "SIC": sic,
                "Total CIKs": total,
                "CIKs with Hits": with_hits,
                "CIKs without Hits": without_hits,
                "% with Hits": round(pct_with_hits, 2),
                "% without Hits": round(pct_without_hits, 2),
            }
        )

    return pd.DataFrame(rows)


def main():
    print("Creating SIC hit analysis...")

    df = create_sic_analysis()

    # Write to Excel
    df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")
    print(f"\nSIC analysis saved to: {OUTPUT_FILE}")
    print(f"Total SIC codes: {len(df)}")
    print("\nSample of results:")
    print(df.head(10).to_string())


if __name__ == "__main__":
    main()
