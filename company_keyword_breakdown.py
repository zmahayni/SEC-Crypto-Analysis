#!/usr/bin/env python3
"""
Sheet 2: Company keyword breakdown
Rows = Companies (CIKs) that had hits
Columns = Keywords
Values = Percentage of that company's filings that mention each keyword
"""

import pathlib
import pandas as pd

# =============================================================================
# CONFIG
# =============================================================================

KEYWORD_HITS_FILE = pathlib.Path("crypto_keyword_hits.xlsx")
OUTPUT_FILE = pathlib.Path("company_keyword_breakdown.xlsx")


# =============================================================================
# MAIN ANALYSIS
# =============================================================================


def create_company_keyword_breakdown() -> pd.DataFrame:
    """
    Create breakdown table:
    Rows = Companies (CIKs)
    Columns = Keywords
    Values = 100 if company mentions keyword, 0 if not
    """
    print(f"Reading from: {KEYWORD_HITS_FILE}")

    # Load keyword hits data
    df = pd.read_excel(KEYWORD_HITS_FILE, engine="openpyxl")

    print(f"Loaded {len(df)} rows")

    # Get unique companies and keywords
    companies = df["CIK"].unique()
    all_keywords = sorted(df["Keyword"].unique())

    print(f"Companies with hits: {len(companies)}")
    print(f"Unique keywords: {len(all_keywords)}")

    # For each company, check which keywords they mention
    rows = []
    for cik in sorted(companies):
        company_data = df[df["CIK"] == cik]

        # Get company name and SIC
        company_name = company_data["Company Name"].iloc[0]
        sic = company_data["SIC"].iloc[0]

        row = {
            "CIK": cik,
            "Company Name": company_name,
            "SIC": sic,
        }

        # For each keyword, check if company mentions it (100% if yes, 0% if no)
        company_keywords = set(company_data["Keyword"].unique())
        for keyword in all_keywords:
            row[keyword] = 100.0 if keyword in company_keywords else 0.0

        rows.append(row)

    return pd.DataFrame(rows)


def main():
    print("Creating company keyword breakdown...")

    df = create_company_keyword_breakdown()

    # Write to Excel
    df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")
    print(f"\nCompany breakdown saved to: {OUTPUT_FILE}")
    print(f"Total companies: {len(df)}")
    print("\nSample of results (first 5 companies):")
    print(df.head(5).to_string())


if __name__ == "__main__":
    main()
