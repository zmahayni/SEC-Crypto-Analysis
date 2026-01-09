#!/usr/bin/env python3
"""
Generate a pivot table: rows = SIC codes, columns = keywords
Values = percentage of companies with that SIC that mention the keyword
Reads from crypto_keyword_hits.xlsx
"""

import pathlib
import pandas as pd

# =============================================================================
# CONFIG
# =============================================================================

INPUT_XLSX = pathlib.Path("crypto_keyword_hits.xlsx")
OUTPUT_XLSX = pathlib.Path("sic_keyword_percentages.xlsx")


# =============================================================================
# MAIN ANALYSIS
# =============================================================================


def create_pivot_table() -> pd.DataFrame:
    """
    Read crypto_keyword_hits.xlsx and create pivot table:
    Rows = SIC codes, Columns = keywords
    Values = percentage of companies with that SIC that mention the keyword
    """
    print(f"Reading from: {INPUT_XLSX}")

    # Load the keyword hits data
    df = pd.read_excel(INPUT_XLSX, engine="openpyxl")

    print(f"Loaded {len(df)} rows")
    print(f"Columns: {list(df.columns)}")

    # Group by CIK to get unique companies per SIC
    # One row per (CIK, SIC, Keyword)
    company_sic_keywords = df[["CIK", "SIC", "Keyword"]].drop_duplicates()

    print(f"\nUnique CIK-SIC-Keyword combinations: {len(company_sic_keywords)}")

    # For each SIC, count total unique companies and companies with each keyword
    sic_groups = company_sic_keywords.groupby("SIC")

    pivot_data = []
    for sic, group in sic_groups:
        # Total unique companies in this SIC
        total_companies = group["CIK"].nunique()
        row = {"SIC": sic, "Company Count": total_companies}

        # For each keyword, count how many companies mention it
        for keyword in group["Keyword"].unique():
            companies_with_keyword = group[group["Keyword"] == keyword]["CIK"].nunique()
            percentage = (companies_with_keyword / total_companies) * 100
            row[keyword] = percentage

        pivot_data.append(row)

    pivot_df = pd.DataFrame(pivot_data)
    pivot_df = pivot_df.sort_values("SIC").reset_index(drop=True)

    return pivot_df


def main():
    print("Starting SIC-Keyword percentage analysis...")

    # Create pivot table
    pivot_df = create_pivot_table()

    # Write to Excel
    output_path = OUTPUT_XLSX
    pivot_df.to_excel(output_path, index=False, engine="openpyxl")
    print(f"\nPercentage table saved to: {output_path}")
    print(
        f"Table shape: {pivot_df.shape[0]} SIC codes × {pivot_df.shape[1] - 2} keywords"
    )

    # Print summary
    print("\nSample of results (first 10 SIC codes):")
    print(pivot_df.head(10).to_string())


if __name__ == "__main__":
    main()
