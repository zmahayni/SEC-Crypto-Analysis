#!/usr/bin/env python3
"""
Extract SIC2 codes from all processed CIKs.
Creates Excel sheet with: Company Name, CIK, SIC4, SIC2
"""

import pathlib
import pandas as pd

# =============================================================================
# CONFIG
# =============================================================================

HOME = pathlib.Path.home()
PROGRESS_FILE = pathlib.Path("progress.txt")
KEYWORD_HITS_FILE = pathlib.Path("crypto_keyword_hits.xlsx")
OUTPUT_FILE = pathlib.Path("cik_sic_codes.xlsx")
CLOUD_FOLDER = (
    HOME
    / "Library/CloudStorage/OneDrive-UniversityofTulsa/NSF-BSF Precautions - crypto10k"
)


# =============================================================================
# MAIN
# =============================================================================


def main():
    print("Extracting SIC2 codes from processed CIKs...")

    # Read progress file to get all scanned CIKs
    with open(PROGRESS_FILE) as f:
        all_ciks = set(line.strip() for line in f if line.strip())

    print(f"Total CIKs scanned: {len(all_ciks)}")

    # Load keyword hits to get company names
    df_hits = pd.read_excel(KEYWORD_HITS_FILE, engine="openpyxl")

    # Create CIK -> Company Name mapping from keyword hits
    cik_to_company = {}
    for _, row in df_hits.iterrows():
        cik = str(row["CIK"]).lstrip("0") or "0"
        company_name = row["Company Name"]
        if cik not in cik_to_company:
            cik_to_company[cik] = company_name

    print(f"Companies found in keyword hits: {len(cik_to_company)}")

    # Extract SIC4 and SIC2 from OneDrive folder
    rows = []
    for cik in sorted(all_ciks):
        cik_padded = cik.zfill(10)
        sic_file = CLOUD_FOLDER / cik_padded / "SIC.txt"

        if sic_file.exists():
            try:
                sic_full = sic_file.read_text(encoding="utf-8").strip()
                sic4 = sic_full[:4] if len(sic_full) >= 4 else sic_full
                sic2 = sic_full[:2] if len(sic_full) >= 2 else sic_full

                # Get company name if available
                company_name = cik_to_company.get(cik, "")

                rows.append(
                    {
                        "Company Name": company_name,
                        "CIK": cik,
                        "SIC4": sic4,
                        "SIC2": sic2,
                    }
                )
            except Exception as e:
                print(f"Error reading SIC for CIK {cik}: {e}")

    # Create DataFrame and save
    df = pd.DataFrame(rows)
    df = df.sort_values("CIK").reset_index(drop=True)

    df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")

    print(f"\n✓ Saved to: {OUTPUT_FILE}")
    print(f"Total rows: {len(df)}")
    print(f"Unique SIC2 codes: {df['SIC2'].nunique()}")
    print(f"Unique SIC4 codes: {df['SIC4'].nunique()}")
    print("\nSample of results:")
    print(df.head(10).to_string())


if __name__ == "__main__":
    main()
