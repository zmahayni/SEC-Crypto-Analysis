#!/usr/bin/env python3
"""
Temporal analysis: Count unique companies mentioning crypto keywords per year (2020-2025).
Produces a table and line chart showing the percentage of companies mentioning crypto over time.
"""

import pathlib
import pandas as pd
import matplotlib.pyplot as plt

# =============================================================================
# CONFIG
# =============================================================================

HOME = pathlib.Path.home()
PROGRESS_FILE = pathlib.Path("progress.txt")
KEYWORD_HITS_FILE = pathlib.Path("crypto_keyword_hits.xlsx")
OUTPUT_TABLE = pathlib.Path("crypto_mentions_by_year.xlsx")
OUTPUT_CHART = pathlib.Path("crypto_mentions_timeline.png")

YEARS = [2020, 2021, 2022, 2023, 2024, 2025]


# =============================================================================
# MAIN ANALYSIS
# =============================================================================


def main():
    print("Temporal Analysis: Crypto Mentions 2020-2025")
    print("=" * 60)

    # Load total companies from progress.txt
    print("\nLoading total companies from progress.txt...")
    with open(PROGRESS_FILE) as f:
        all_ciks = set(line.strip() for line in f if line.strip())

    total_companies = len(all_ciks)
    print(f"Total companies in dataset: {total_companies}")

    # Load keyword hits
    print("\nLoading keyword hits...")
    df = pd.read_excel(KEYWORD_HITS_FILE, engine="openpyxl")
    print(f"Total keyword hit records: {len(df)}")

    # Extract year from Filing Date
    df["Year"] = pd.to_datetime(df["Filing Date"], errors="coerce").dt.year

    # Count unique companies per year
    results = []
    for year in YEARS:
        # Get unique CIKs for this year
        year_ciks = df[df["Year"] == year]["CIK"].nunique()

        # Calculate percentage
        percentage = (year_ciks / total_companies * 100) if total_companies > 0 else 0

        results.append(
            {
                "Year": year,
                "Companies Mentioning Crypto": year_ciks,
                "Total Companies": total_companies,
                "Percentage": round(percentage, 2),
            }
        )

        print(f"{year}: {year_ciks} companies ({percentage:.2f}%)")

    # Create DataFrame
    df_results = pd.DataFrame(results)

    # Save table to Excel
    df_results.to_excel(OUTPUT_TABLE, index=False, engine="openpyxl")
    print(f"\n✓ Table saved to: {OUTPUT_TABLE}")

    # Create line chart
    print("\nGenerating line chart...")

    plt.figure(figsize=(10, 6))
    plt.plot(
        df_results["Year"],
        df_results["Percentage"],
        marker="o",
        linewidth=2,
        markersize=8,
        color="#2E86AB",
    )

    plt.xlabel("Year", fontsize=12, fontweight="bold")
    plt.ylabel(
        "Percentage of Companies Mentioning Crypto (%)", fontsize=12, fontweight="bold"
    )
    plt.title(
        "Temporal Evolution of Crypto Mentions in SEC Filings\n(2020-2025)",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )

    plt.grid(True, alpha=0.3, linestyle="--")
    plt.xticks(YEARS)
    plt.ylim(bottom=0)

    # Add value labels on points
    for _, row in df_results.iterrows():
        plt.annotate(
            f"{row['Percentage']:.1f}%",
            xy=(row["Year"], row["Percentage"]),
            xytext=(0, 10),
            textcoords="offset points",
            ha="center",
            fontsize=9,
            fontweight="bold",
        )

    plt.tight_layout()
    plt.savefig(OUTPUT_CHART, dpi=300, bbox_inches="tight")
    print(f"✓ Chart saved to: {OUTPUT_CHART}")

    # Display summary
    print("\n" + "=" * 60)
    print("SUMMARY TABLE")
    print("=" * 60)
    print(df_results.to_string(index=False))
    print("\n" + "=" * 60)

    # Calculate growth
    if len(df_results) >= 2:
        first_year = df_results.iloc[0]
        last_year = df_results.iloc[-1]

        growth = last_year["Percentage"] - first_year["Percentage"]
        print(
            f"\nChange from {first_year['Year']} to {last_year['Year']}: {growth:+.2f} percentage points"
        )

        if first_year["Percentage"] > 0:
            pct_growth = (growth / first_year["Percentage"]) * 100
            print(f"Relative growth: {pct_growth:+.1f}%")


if __name__ == "__main__":
    main()
