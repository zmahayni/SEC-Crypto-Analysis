#!/usr/bin/env python3
"""
Analyze classified 10-K snippets.

Produces:
- classification_analysis.xlsx with 11 sheets (distribution, crosstabs, temporal, framing)
- 7 PNG charts (bar, stacked bar, line, heatmap, framing temporal, classification %, framing by SIC2)
"""

import pathlib
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR = pathlib.Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data"

INPUT_FILE = DATA_DIR / "10k_snippets_for_classification.xlsx"
OUTPUT_EXCEL = DATA_DIR / "classification_analysis.xlsx"

# Chart output paths
CHART_DISTRIBUTION = DATA_DIR / "classification_distribution.png"
CHART_BY_KEYWORD = DATA_DIR / "classification_by_keyword.png"
CHART_TEMPORAL = DATA_DIR / "classification_temporal.png"
CHART_BY_SIC2 = DATA_DIR / "classification_by_sic2.png"
CHART_FRAMING_TEMPORAL = DATA_DIR / "framing_temporal.png"
CHART_CLASSIFICATION_PCT = DATA_DIR / "classification_pct_temporal.png"
CHART_FRAMING_SIC2 = DATA_DIR / "framing_by_sic2.png"

# Chart colors
COLORS = ["#2E86AB", "#A23B72", "#F18F01", "#C73E1D", "#6A994E", "#7B2D8E", "#1B998B", "#E56B6F"]

# Framing categories
RISK_CATEGORIES = ["Risk", "Competitive risk", "Regulation"]
OPPORTUNITY_CATEGORIES = ["Business", "Tech for existing companies", "Investment"]

# Classification typo fixes
CLASSIFICATION_FIXES = {
    "RIsk": "Risk",
    "Competitive Risk": "Competitive risk",
    "Explicit Non-Use (switch from btc -> drone)": "Explicit Non-Use",
}


# ─────────────────────────────────────────────────────────────────────────────
# DATA CLEANING
# ─────────────────────────────────────────────────────────────────────────────

def clean_classification_column(df: pd.DataFrame) -> pd.DataFrame:
    """Fix classification typos and normalize values."""
    df = df.copy()
    df["Classification"] = df["Classification"].replace(CLASSIFICATION_FIXES)
    return df


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """Extract year and normalize fields."""
    df = df.copy()
    df["Year"] = pd.to_datetime(df["Filing Date"]).dt.year
    df["SIC2"] = df["SIC2"].astype("Int64")  # nullable int
    return df


def add_framing_column(df: pd.DataFrame) -> pd.DataFrame:
    """Add framing column categorizing classifications as Risk/Opportunity/Other."""
    df = df.copy()

    def get_framing(cls):
        if pd.isna(cls):
            return None
        if cls in RISK_CATEGORIES:
            return "Risk-oriented"
        if cls in OPPORTUNITY_CATEGORIES:
            return "Opportunity-oriented"
        return "Other"

    df["Framing"] = df["Classification"].apply(get_framing)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# ANALYSIS FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def analyze_overall_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """Overall classification distribution (excluding unclassified)."""
    classified = df[df["Classification"].notna()]
    counts = classified["Classification"].value_counts().sort_values(ascending=False)
    result = pd.DataFrame({
        "Classification": counts.index,
        "Count": counts.values,
        "Percentage": (counts.values / counts.sum() * 100).round(1)
    })
    return result


def analyze_by_sic2(df: pd.DataFrame) -> pd.DataFrame:
    """Crosstab of SIC2 × Classification."""
    classified = df[df["Classification"].notna()]
    crosstab = pd.crosstab(classified["SIC2"], classified["Classification"], margins=True, margins_name="Total")
    crosstab = crosstab.sort_values("Total", ascending=False)
    return crosstab


def analyze_by_keyword(df: pd.DataFrame) -> pd.DataFrame:
    """Crosstab of Keyword × Classification."""
    classified = df[df["Classification"].notna()]
    crosstab = pd.crosstab(classified["Keyword"], classified["Classification"], margins=True, margins_name="Total")
    crosstab = crosstab.sort_values("Total", ascending=False)
    return crosstab


def analyze_by_year(df: pd.DataFrame) -> pd.DataFrame:
    """Temporal breakdown by year with percentages."""
    classified = df[df["Classification"].notna()]
    crosstab = pd.crosstab(classified["Year"], classified["Classification"], margins=True, margins_name="Total")

    # Add percentage columns for each classification
    result = crosstab.copy()
    classifications = [c for c in crosstab.columns if c != "Total"]
    for cls in classifications:
        result[f"{cls} %"] = (crosstab[cls] / crosstab["Total"] * 100).round(1)

    return result


def analyze_sic2_temporal(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    """Long-format table: top N SIC2 codes over time."""
    classified = df[df["Classification"].notna()]

    # Get top N SIC2 codes
    top_sic2 = classified["SIC2"].value_counts().head(top_n).index.tolist()
    subset = classified[classified["SIC2"].isin(top_sic2)]

    # Group by SIC2, Year, Classification
    grouped = subset.groupby(["SIC2", "Year", "Classification"]).size().reset_index(name="Count")

    # Add percentage within each SIC2-Year group
    totals = grouped.groupby(["SIC2", "Year"])["Count"].transform("sum")
    grouped["Percentage"] = (grouped["Count"] / totals * 100).round(1)

    return grouped.sort_values(["SIC2", "Year", "Count"], ascending=[True, True, False])


def analyze_framing_by_year(df: pd.DataFrame) -> pd.DataFrame:
    """Framing counts and percentages by year."""
    classified = df[df["Classification"].notna()]
    crosstab = pd.crosstab(classified["Year"], classified["Framing"], margins=True, margins_name="Total")

    # Add percentage columns
    result = crosstab.copy()
    framings = [c for c in crosstab.columns if c != "Total"]
    for framing in framings:
        result[f"{framing} %"] = (crosstab[framing] / crosstab["Total"] * 100).round(1)

    return result


def analyze_framing_by_sic2(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    """Framing breakdown for top N SIC2 codes."""
    classified = df[df["Classification"].notna()]

    # Get top N SIC2 codes
    top_sic2 = classified["SIC2"].value_counts().head(top_n).index.tolist()
    subset = classified[classified["SIC2"].isin(top_sic2)]

    crosstab = pd.crosstab(subset["SIC2"], subset["Framing"], margins=True, margins_name="Total")

    # Add percentage columns
    result = crosstab.copy()
    framings = [c for c in crosstab.columns if c != "Total"]
    for framing in framings:
        result[f"{framing} %"] = (crosstab[framing] / crosstab["Total"] * 100).round(1)

    # Sort by total
    result = result.sort_values("Total", ascending=False)
    return result


def analyze_classification_pct_by_year(df: pd.DataFrame) -> pd.DataFrame:
    """Percentage trends for all classifications by year."""
    classified = df[df["Classification"].notna()]
    crosstab = pd.crosstab(classified["Year"], classified["Classification"])

    # Convert to percentages (row-wise)
    pct = crosstab.div(crosstab.sum(axis=1), axis=0) * 100
    pct = pct.round(1)

    # Add count column for reference
    pct["Total Count"] = crosstab.sum(axis=1)

    return pct


def analyze_sic2_temporal_detail(df: pd.DataFrame, sic2: int) -> pd.DataFrame:
    """Year × Classification for a specific SIC2 code."""
    classified = df[(df["Classification"].notna()) & (df["SIC2"] == sic2)]

    if len(classified) == 0:
        return pd.DataFrame()

    crosstab = pd.crosstab(classified["Year"], classified["Classification"], margins=True, margins_name="Total")

    # Add percentage columns
    result = crosstab.copy()
    classifications = [c for c in crosstab.columns if c != "Total"]
    for cls in classifications:
        result[f"{cls} %"] = (crosstab[cls] / crosstab["Total"] * 100).round(1)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# PLOTTING FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def plot_distribution(df: pd.DataFrame, output_path: pathlib.Path):
    """Horizontal bar chart of overall distribution."""
    dist = analyze_overall_distribution(df)

    fig, ax = plt.subplots(figsize=(12, 7))
    y_pos = range(len(dist))
    bars = ax.barh(y_pos, dist["Count"], color=COLORS[:len(dist)])

    ax.set_yticks(y_pos)
    ax.set_yticklabels(dist["Classification"])
    ax.invert_yaxis()  # Highest at top
    ax.set_xlabel("Count")
    ax.set_title("Classification Distribution of 10-K Crypto Snippets")
    ax.grid(axis="x", alpha=0.3)

    # Add count labels
    for bar, count, pct in zip(bars, dist["Count"], dist["Percentage"]):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                f"{count} ({pct}%)", va="center", fontsize=10)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path.name}")


def plot_by_keyword(df: pd.DataFrame, output_path: pathlib.Path):
    """Stacked bar chart of classification breakdown per keyword."""
    classified = df[df["Classification"].notna()]
    crosstab = pd.crosstab(classified["Keyword"], classified["Classification"])

    # Sort keywords by total count
    crosstab = crosstab.loc[crosstab.sum(axis=1).sort_values(ascending=False).index]

    fig, ax = plt.subplots(figsize=(14, 8))
    crosstab.plot(kind="barh", stacked=True, ax=ax, color=COLORS[:len(crosstab.columns)])

    ax.set_xlabel("Count")
    ax.set_ylabel("Keyword")
    ax.set_title("Classification Breakdown by Keyword")
    ax.legend(title="Classification", bbox_to_anchor=(1.02, 1), loc="upper left")
    ax.grid(axis="x", alpha=0.3)
    ax.invert_yaxis()

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path.name}")


def plot_temporal(df: pd.DataFrame, output_path: pathlib.Path, top_n: int = 5):
    """Line chart of top N classifications over time."""
    classified = df[df["Classification"].notna()]
    crosstab = pd.crosstab(classified["Year"], classified["Classification"])

    # Get top N classifications by total
    top_classifications = crosstab.sum().sort_values(ascending=False).head(top_n).index.tolist()

    fig, ax = plt.subplots(figsize=(12, 7))

    for i, cls in enumerate(top_classifications):
        ax.plot(crosstab.index, crosstab[cls], marker="o", linewidth=2,
                label=cls, color=COLORS[i % len(COLORS)])

    ax.set_xlabel("Year")
    ax.set_ylabel("Count")
    ax.set_title("Top 5 Classifications Over Time")
    ax.legend(title="Classification", bbox_to_anchor=(1.02, 1), loc="upper left")
    ax.grid(alpha=0.3)
    ax.set_xticks(crosstab.index)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path.name}")


def plot_by_sic2(df: pd.DataFrame, output_path: pathlib.Path, top_n: int = 10):
    """Heatmap of top N SIC2 × Classification."""
    classified = df[df["Classification"].notna()]

    # Get top N SIC2 codes
    top_sic2 = classified["SIC2"].value_counts().head(top_n).index.tolist()
    subset = classified[classified["SIC2"].isin(top_sic2)]

    crosstab = pd.crosstab(subset["SIC2"], subset["Classification"])
    # Sort by total
    crosstab = crosstab.loc[crosstab.sum(axis=1).sort_values(ascending=False).index]

    fig, ax = plt.subplots(figsize=(14, 8))

    # Create heatmap
    im = ax.imshow(crosstab.values, cmap="YlOrRd", aspect="auto")

    # Set ticks
    ax.set_xticks(range(len(crosstab.columns)))
    ax.set_xticklabels(crosstab.columns, rotation=45, ha="right")
    ax.set_yticks(range(len(crosstab.index)))
    ax.set_yticklabels([f"SIC {int(s)}" for s in crosstab.index])

    # Add value annotations
    for i in range(len(crosstab.index)):
        for j in range(len(crosstab.columns)):
            val = crosstab.iloc[i, j]
            if val > 0:
                text_color = "white" if val > crosstab.values.max() / 2 else "black"
                ax.text(j, i, str(val), ha="center", va="center", color=text_color, fontsize=9)

    ax.set_xlabel("Classification")
    ax.set_ylabel("SIC Code (2-digit)")
    ax.set_title("Classification Distribution by Industry (Top 10 SIC2)")

    # Colorbar
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label("Count")

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path.name}")


def plot_framing_temporal(df: pd.DataFrame, output_path: pathlib.Path):
    """Line chart of Risk vs Opportunity vs Other over time."""
    classified = df[df["Classification"].notna()]
    crosstab = pd.crosstab(classified["Year"], classified["Framing"])

    # Convert to percentages for normalized view
    pct = crosstab.div(crosstab.sum(axis=1), axis=0) * 100

    # Ensure consistent ordering
    order = ["Risk-oriented", "Opportunity-oriented", "Other"]
    cols = [c for c in order if c in pct.columns]
    pct = pct[cols]

    fig, ax = plt.subplots(figsize=(12, 7))

    # Line chart
    colors = {"Risk-oriented": "#C73E1D", "Opportunity-oriented": "#6A994E", "Other": "#7B2D8E"}
    for framing in cols:
        ax.plot(pct.index, pct[framing], marker="o", linewidth=2.5,
                label=framing, color=colors[framing], markersize=8)

    ax.set_xlabel("Year")
    ax.set_ylabel("Percentage (%)")
    ax.set_title("Crypto Framing Over Time: Risk vs Opportunity")
    ax.legend(loc="best")
    ax.set_ylim(0, 105)
    ax.set_xticks(pct.index)
    ax.grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path.name}")


def plot_classification_pct_temporal(df: pd.DataFrame, output_path: pathlib.Path):
    """Line chart showing percentage trends for all classifications."""
    classified = df[df["Classification"].notna()]
    crosstab = pd.crosstab(classified["Year"], classified["Classification"])

    # Convert to percentages
    pct = crosstab.div(crosstab.sum(axis=1), axis=0) * 100

    fig, ax = plt.subplots(figsize=(14, 8))

    # Plot all classifications
    for i, cls in enumerate(pct.columns):
        ax.plot(pct.index, pct[cls], marker="o", linewidth=2,
                label=cls, color=COLORS[i % len(COLORS)])

    ax.set_xlabel("Year")
    ax.set_ylabel("Percentage (%)")
    ax.set_title("Classification Trends Over Time (% of Each Year)")
    ax.legend(title="Classification", bbox_to_anchor=(1.02, 1), loc="upper left")
    ax.grid(alpha=0.3)
    ax.set_xticks(pct.index)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path.name}")


def plot_framing_by_sic2(df: pd.DataFrame, output_path: pathlib.Path, top_n: int = 5):
    """Grouped bar chart of framing breakdown by top SIC2 sectors."""
    classified = df[df["Classification"].notna()]

    # Get top N SIC2 codes
    top_sic2 = classified["SIC2"].value_counts().head(top_n).index.tolist()
    subset = classified[classified["SIC2"].isin(top_sic2)]

    crosstab = pd.crosstab(subset["SIC2"], subset["Framing"])
    # Convert to percentages
    pct = crosstab.div(crosstab.sum(axis=1), axis=0) * 100

    # Sort by total count
    order = classified[classified["SIC2"].isin(top_sic2)]["SIC2"].value_counts().index.tolist()
    pct = pct.reindex(order)

    # SIC2 code names for labels
    sic2_names = {
        73: "73 (Business Services)",
        60: "60 (Banking)",
        67: "67 (Holding Companies)",
        62: "62 (Security Brokers)",
        61: "61 (Credit Agencies)",
    }

    fig, ax = plt.subplots(figsize=(12, 7))

    x = np.arange(len(pct))
    width = 0.25

    # Ensure consistent ordering
    framings = ["Risk-oriented", "Opportunity-oriented", "Other"]
    colors = {"Risk-oriented": "#C73E1D", "Opportunity-oriented": "#6A994E", "Other": "#7B2D8E"}

    for i, framing in enumerate(framings):
        if framing in pct.columns:
            values = pct[framing].values
        else:
            values = [0] * len(pct)
        bars = ax.bar(x + i * width, values, width, label=framing, color=colors[framing])

        # Add value labels
        for bar, val in zip(bars, values):
            if val > 5:  # Only label if significant
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                        f"{val:.0f}%", ha="center", va="bottom", fontsize=9)

    ax.set_xlabel("Industry Sector (SIC2)")
    ax.set_ylabel("Percentage (%)")
    ax.set_title("Crypto Framing by Industry Sector")
    ax.set_xticks(x + width)
    ax.set_xticklabels([sic2_names.get(int(s), f"SIC {int(s)}") for s in pct.index], rotation=15, ha="right")
    ax.legend(title="Framing")
    ax.set_ylim(0, 100)
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path.name}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("Loading data...")
    df = pd.read_excel(INPUT_FILE)

    # Clean and prepare
    print("Cleaning classification values...")
    df = clean_classification_column(df)
    df = prepare_data(df)
    df = add_framing_column(df)

    # Filter out 2026 data (only 1 data point, not meaningful)
    df_temporal = df[df["Year"] < 2026]
    excluded_2026 = len(df) - len(df_temporal)
    if excluded_2026 > 0:
        print(f"  Excluding {excluded_2026} row(s) from 2026 for temporal analysis")

    # Summary stats
    total = len(df)
    classified = df["Classification"].notna().sum()
    unclassified = df["Classification"].isna().sum()
    unique_classifications = df[df["Classification"].notna()]["Classification"].nunique()

    print(f"\nSummary:")
    print(f"  Total snippets: {total}")
    print(f"  Classified: {classified}")
    print(f"  Unclassified: {unclassified}")
    print(f"  Unique classifications: {unique_classifications}")

    # Show distribution
    print("\nClassification Distribution:")
    dist = analyze_overall_distribution(df)
    for _, row in dist.iterrows():
        print(f"  {row['Classification']}: {row['Count']} ({row['Percentage']}%)")

    # Show framing summary by year
    print("\nFraming Summary by Year (2020-2025):")
    framing_df = analyze_framing_by_year(df_temporal)
    for year in sorted(df_temporal[df_temporal["Classification"].notna()]["Year"].unique()):
        if year in framing_df.index:
            row = framing_df.loc[year]
            risk_pct = row.get("Risk-oriented %", 0)
            opp_pct = row.get("Opportunity-oriented %", 0)
            other_pct = row.get("Other %", 0)
            total_count = row.get("Total", 0)
            print(f"  {year}: Risk {risk_pct:.1f}% | Opportunity {opp_pct:.1f}% | Other {other_pct:.1f}% (n={int(total_count)})")

    # Generate Excel output
    print(f"\nWriting Excel to {OUTPUT_EXCEL.name}...")
    with pd.ExcelWriter(OUTPUT_EXCEL, engine="openpyxl") as writer:
        # Original 5 sheets
        analyze_overall_distribution(df).to_excel(writer, sheet_name="Overall Distribution", index=False)
        analyze_by_sic2(df).to_excel(writer, sheet_name="By SIC2")
        analyze_by_keyword(df).to_excel(writer, sheet_name="By Keyword")
        analyze_by_year(df_temporal).to_excel(writer, sheet_name="By Year")
        analyze_sic2_temporal(df_temporal).to_excel(writer, sheet_name="SIC2 Temporal", index=False)

        # New 6 sheets for framing analysis
        analyze_framing_by_year(df_temporal).to_excel(writer, sheet_name="Framing by Year")
        analyze_framing_by_sic2(df_temporal).to_excel(writer, sheet_name="Framing by SIC2")
        analyze_classification_pct_by_year(df_temporal).to_excel(writer, sheet_name="Classification % by Year")

        # SIC2-specific temporal breakdowns
        for sic2, name in [(73, "SIC2 73 Temporal"), (60, "SIC2 60 Temporal"), (67, "SIC2 67 Temporal")]:
            sic2_df = analyze_sic2_temporal_detail(df_temporal, sic2)
            if not sic2_df.empty:
                sic2_df.to_excel(writer, sheet_name=name)

    print(f"  Saved: {OUTPUT_EXCEL.name}")

    # Generate charts
    print("\nGenerating charts...")
    # Original 4 charts (use temporal-filtered data)
    plot_distribution(df, CHART_DISTRIBUTION)
    plot_by_keyword(df, CHART_BY_KEYWORD)
    plot_temporal(df_temporal, CHART_TEMPORAL)
    plot_by_sic2(df, CHART_BY_SIC2)

    # New 3 charts
    plot_framing_temporal(df_temporal, CHART_FRAMING_TEMPORAL)
    plot_classification_pct_temporal(df_temporal, CHART_CLASSIFICATION_PCT)
    plot_framing_by_sic2(df_temporal, CHART_FRAMING_SIC2)

    print("\nDone!")


if __name__ == "__main__":
    main()
