#!/usr/bin/env python3
"""
SIC2 Temporal Analysis: Top 5 industries by crypto mentions, with year-by-year timelines.
Shows heterogeneous adoption patterns across industries (2020-2025).
"""

import pathlib
import pandas as pd
import matplotlib.pyplot as plt

# =============================================================================
# CONFIG
# =============================================================================

SCRIPT_DIR = pathlib.Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data"
ROOT_DIR = SCRIPT_DIR.parent

HOME = pathlib.Path.home()
CLOUD_FOLDER = (
    HOME
    / "Library/CloudStorage/OneDrive-UniversityofTulsa/NSF-BSF Precautions - crypto10k"
)
PROGRESS_FILE = ROOT_DIR / "progress.txt"
KEYWORD_HITS_FILE = DATA_DIR / "crypto_keyword_hits.xlsx"
OUTPUT_TABLE = DATA_DIR / "sic2_crypto_mentions_by_year.xlsx"
OUTPUT_CHART = DATA_DIR / "sic2_crypto_timeline.png"

YEARS = [2020, 2021, 2022, 2023, 2024, 2025]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def get_sic2_for_all_ciks() -> dict:
    """Get SIC2 code for all CIKs from OneDrive folder."""
    print("Loading SIC2 codes from OneDrive folder...")

    cik_to_sic2 = {}
    cik_folders = [f for f in CLOUD_FOLDER.iterdir() if f.is_dir()]

    for cik_folder in cik_folders:
        cik = cik_folder.name
        sic_file = cik_folder / "SIC.txt"

        if sic_file.exists():
            try:
                sic_full = sic_file.read_text(encoding="utf-8").strip()
                sic2 = sic_full[:2] if len(sic_full) >= 2 else ""
                if sic2:
                    # Store with stripped CIK format
                    cik_stripped = cik.lstrip("0") or "0"
                    cik_to_sic2[cik_stripped] = sic2
                    cik_to_sic2[cik] = sic2  # Also store padded version
            except Exception:
                pass

    print(f"Loaded SIC2 codes for {len(set(cik_to_sic2.values()))} unique SIC2 codes")
    return cik_to_sic2


# =============================================================================
# MAIN ANALYSIS
# =============================================================================


def main():
    print("SIC2 Temporal Analysis: Top 5 Industries (2020-2025)")
    print("=" * 70)

    # Load SIC2 mapping
    cik_to_sic2 = get_sic2_for_all_ciks()

    # Load keyword hits
    print("\nLoading keyword hits...")
    df = pd.read_excel(KEYWORD_HITS_FILE, engine="openpyxl")
    print(f"Total keyword hit records: {len(df)}")

    # Add SIC2 to keyword hits
    df["SIC2"] = df["CIK"].astype(str).map(cik_to_sic2)

    # Remove rows without SIC2
    df = df[df["SIC2"].notna()]
    print(f"Records with SIC2: {len(df)}")

    # Extract year from Filing Date
    df["Year"] = pd.to_datetime(df["Filing Date"], errors="coerce").dt.year

    # Identify top 5 SIC2 codes by total unique companies mentioning crypto (2020-2025)
    print("\nIdentifying top 5 SIC2 industries...")
    sic2_company_counts = (
        df.groupby("SIC2")["CIK"].nunique().sort_values(ascending=False)
    )
    top5_sic2 = sic2_company_counts.head(5).index.tolist()

    print("\nTop 5 SIC2 industries by companies mentioning crypto:")
    for i, sic2 in enumerate(top5_sic2, 1):
        print(f"  {i}. SIC2 {sic2}: {sic2_company_counts[sic2]} companies")

    # Count total companies per SIC2 (from all CIKs, not just those with hits)
    print("\nCounting total companies per SIC2...")
    sic2_totals = {}
    for cik, sic2 in cik_to_sic2.items():
        if sic2 in top5_sic2:
            sic2_totals[sic2] = sic2_totals.get(sic2, set())
            sic2_totals[sic2].add(cik)

    sic2_totals = {sic2: len(ciks) for sic2, ciks in sic2_totals.items()}

    # Build year-by-year data for top 5 SIC2 codes
    print("\nComputing year-by-year percentages...")
    results = []

    for sic2 in top5_sic2:
        total_companies = sic2_totals.get(sic2, 0)

        for year in YEARS:
            # Count unique companies in this SIC2 that mentioned crypto in this year
            year_sic2_df = df[(df["SIC2"] == sic2) & (df["Year"] == year)]
            companies_mentioning = year_sic2_df["CIK"].nunique()

            # Calculate percentage
            percentage = (
                (companies_mentioning / total_companies * 100)
                if total_companies > 0
                else 0
            )

            results.append(
                {
                    "Year": year,
                    "SIC2": sic2,
                    "Companies Mentioning Crypto": companies_mentioning,
                    "Total Companies in SIC2": total_companies,
                    "Percentage": round(percentage, 2),
                }
            )

    # Create DataFrame
    df_results = pd.DataFrame(results)

    # Save table to Excel
    df_results.to_excel(OUTPUT_TABLE, index=False, engine="openpyxl")
    print(f"\n✓ Table saved to: {OUTPUT_TABLE}")

    # Create line chart
    print("\nGenerating line chart...")

    # Define colors for each SIC2
    colors = ["#2E86AB", "#A23B72", "#F18F01", "#C73E1D", "#6A994E"]

    plt.figure(figsize=(12, 7))

    for i, sic2 in enumerate(top5_sic2):
        sic2_data = df_results[df_results["SIC2"] == sic2]
        plt.plot(
            sic2_data["Year"],
            sic2_data["Percentage"],
            marker="o",
            linewidth=2.5,
            markersize=7,
            color=colors[i],
            label=f"SIC2 {sic2}",
        )

    plt.xlabel("Year", fontsize=13, fontweight="bold")
    plt.ylabel(
        "Percentage of Companies Mentioning Crypto (%)", fontsize=13, fontweight="bold"
    )
    plt.title(
        "Crypto Adoption Across Top 5 Industries (SIC2)\n2020-2025",
        fontsize=15,
        fontweight="bold",
        pad=20,
    )

    plt.grid(True, alpha=0.3, linestyle="--")
    plt.xticks(YEARS)
    plt.ylim(bottom=0)
    plt.legend(loc="best", fontsize=11, framealpha=0.9)

    plt.tight_layout()
    plt.savefig(OUTPUT_CHART, dpi=300, bbox_inches="tight")
    print(f"✓ Chart saved to: {OUTPUT_CHART}")

    # Display summary
    print("\n" + "=" * 70)
    print("SUMMARY TABLE (Top 5 SIC2 Industries)")
    print("=" * 70)

    for sic2 in top5_sic2:
        print(f"\nSIC2 {sic2}:")
        sic2_data = df_results[df_results["SIC2"] == sic2]
        print(
            sic2_data[
                [
                    "Year",
                    "Companies Mentioning Crypto",
                    "Total Companies in SIC2",
                    "Percentage",
                ]
            ].to_string(index=False)
        )

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
