#!/usr/bin/env python3
"""
Generate comprehensive analysis with 3 sheets:
1. SIC Hit Analysis - % of companies per SIC with hits
2. SIC Keyword Breakdown by Filings - % of filings per SIC mentioning each keyword
3. SIC Keyword Breakdown by Companies - % of companies (with hits) per SIC mentioning each keyword
All in one Excel file with multiple sheets.
"""

import pathlib
import pandas as pd

# =============================================================================
# CONFIG
# =============================================================================

SCRIPT_DIR = pathlib.Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data"
ROOT_DIR = SCRIPT_DIR.parent

HOME = pathlib.Path.home()
PROGRESS_FILE = ROOT_DIR / "progress.txt"
KEYWORD_HITS_FILE = DATA_DIR / "crypto_keyword_hits.xlsx"
OUTPUT_FILE = DATA_DIR / "crypto_analysis.xlsx"
CLOUD_FOLDER = (
    HOME
    / "Library/CloudStorage/OneDrive-UniversityofTulsa/NSF-BSF Precautions - crypto10k"
)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def load_all_ciks_with_sic() -> dict:
    """Load all CIKs from progress.txt and get their SIC codes from OneDrive folder."""
    with open(PROGRESS_FILE) as f:
        all_ciks = set(line.strip() for line in f if line.strip())

    print(f"Total CIKs scanned: {len(all_ciks)}")

    # Read SIC codes from OneDrive folder
    cik_to_sic = {}
    for cik in all_ciks:
        cik_padded = cik.zfill(10)
        sic_file = CLOUD_FOLDER / cik_padded / "SIC.txt"
        if sic_file.exists():
            try:
                sic = sic_file.read_text(encoding="utf-8").strip()[:4]
                if sic:
                    cik_normalized = cik.lstrip("0") or "0"
                    cik_to_sic[cik_normalized] = sic
            except Exception:
                pass

    print(f"CIKs with SIC data: {len(cik_to_sic)}")
    return cik_to_sic


def load_keyword_hits() -> pd.DataFrame:
    """Load keyword hits data."""
    df = pd.read_excel(KEYWORD_HITS_FILE, engine="openpyxl")
    print(f"Loaded {len(df)} keyword hit rows")
    return df


def load_ciks_with_hits() -> set:
    """Load CIKs that have keyword hits."""
    df = load_keyword_hits()
    ciks_with_hits = set(
        str(cik).lstrip("0") or "0" for cik in df["CIK"].astype(str).unique()
    )
    print(f"CIKs with keyword hits: {len(ciks_with_hits)}")
    return ciks_with_hits


# =============================================================================
# SHEET 1: SIC HIT ANALYSIS
# =============================================================================


def create_sheet1_sic_hit_analysis() -> pd.DataFrame:
    """
    Sheet 1: SIC Hit Analysis
    Rows = SIC codes
    Columns = Total CIKs, CIKs with Hits, CIKs without Hits, % with Hits, % without Hits
    """
    print("\n=== Creating Sheet 1: SIC Hit Analysis ===")

    cik_to_sic = load_all_ciks_with_sic()
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

    df = pd.DataFrame(rows)
    print(f"Sheet 1: {len(df)} SIC codes")
    return df


# =============================================================================
# SHEET 2: SIC KEYWORD BREAKDOWN BY FILINGS
# =============================================================================


def create_sheet2_sic_keyword_filings() -> pd.DataFrame:
    """
    Sheet 2: SIC Keyword Breakdown by Filings
    Rows = SIC codes
    Columns = Keywords
    Values = % of filings in that SIC mentioning each keyword
    """
    print("\n=== Creating Sheet 2: SIC Keyword Breakdown by Filings ===")

    df = load_keyword_hits()

    # Get all unique keywords
    all_keywords = sorted(df["Keyword"].unique())
    print(f"Unique keywords: {len(all_keywords)}")

    # Group by SIC
    sic_groups = df.groupby("SIC")

    rows = []
    for sic, group in sic_groups:
        # Total unique filings in this SIC
        total_filings = group[["Filing Type", "Filing Date"]].drop_duplicates().shape[0]
        row = {"SIC": sic, "Total Filings": total_filings}

        # For each keyword, count filings that mention it
        for keyword in all_keywords:
            filings_with_keyword = (
                group[group["Keyword"] == keyword][["Filing Type", "Filing Date"]]
                .drop_duplicates()
                .shape[0]
            )

            pct = (
                (filings_with_keyword / total_filings * 100) if total_filings > 0 else 0
            )
            row[keyword] = round(pct, 2)

        rows.append(row)

    df_sheet2 = pd.DataFrame(rows)
    df_sheet2 = df_sheet2.sort_values("SIC").reset_index(drop=True)
    print(f"Sheet 2: {len(df_sheet2)} SIC codes")
    return df_sheet2


# =============================================================================
# SHEET 3: SIC KEYWORD BREAKDOWN BY COMPANIES (WITH HITS)
# =============================================================================


def create_sheet3_sic_keyword_companies() -> pd.DataFrame:
    """
    Sheet 3: SIC Keyword Breakdown by Companies (with hits)
    Rows = SIC codes
    Columns = Keywords
    Values = % of companies (with hits) in that SIC mentioning each keyword
    """
    print("\n=== Creating Sheet 3: SIC Keyword Breakdown by Companies ===")

    df = load_keyword_hits()

    # Get all unique keywords
    all_keywords = sorted(df["Keyword"].unique())

    # Group by SIC
    sic_groups = df.groupby("SIC")

    rows = []
    for sic, group in sic_groups:
        # Total unique companies in this SIC (with hits)
        total_companies = group["CIK"].nunique()
        row = {"SIC": sic, "Total Companies": total_companies}

        # For each keyword, count companies that mention it
        for keyword in all_keywords:
            companies_with_keyword = group[group["Keyword"] == keyword]["CIK"].nunique()

            pct = (
                (companies_with_keyword / total_companies * 100)
                if total_companies > 0
                else 0
            )
            row[keyword] = round(pct, 2)

        rows.append(row)

    df_sheet3 = pd.DataFrame(rows)
    df_sheet3 = df_sheet3.sort_values("SIC").reset_index(drop=True)
    print(f"Sheet 3: {len(df_sheet3)} SIC codes")
    return df_sheet3


# =============================================================================
# MAIN
# =============================================================================


def main():
    print("Generating comprehensive crypto analysis...")

    # Create all 3 sheets
    sheet1 = create_sheet1_sic_hit_analysis()
    sheet2 = create_sheet2_sic_keyword_filings()
    sheet3 = create_sheet3_sic_keyword_companies()

    # Write all sheets to single Excel file
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        sheet1.to_excel(writer, sheet_name="SIC Hit Analysis", index=False)
        sheet2.to_excel(writer, sheet_name="SIC Keywords by Filings", index=False)
        sheet3.to_excel(writer, sheet_name="SIC Keywords by Companies", index=False)

    print(f"\n✓ All sheets saved to: {OUTPUT_FILE}")
    print(f"  - Sheet 1: SIC Hit Analysis ({len(sheet1)} rows)")
    print(f"  - Sheet 2: SIC Keywords by Filings ({len(sheet2)} rows)")
    print(f"  - Sheet 3: SIC Keywords by Companies ({len(sheet3)} rows)")


if __name__ == "__main__":
    main()
