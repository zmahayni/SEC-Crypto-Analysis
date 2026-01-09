#!/usr/bin/env python3
"""
Clean up snippets in mastercard_crypto_snippets.xlsx to make them readable.
Removes HTML tags, entities, and other artifacts.
"""

import re
import pathlib
import pandas as pd
from bs4 import BeautifulSoup

INPUT_FILE = pathlib.Path("mastercard_crypto_snippets.xlsx")
OUTPUT_FILE = pathlib.Path("mastercard_crypto_snippets_cleaned.xlsx")


def clean_snippet(text: str) -> str:
    """
    Clean a snippet by removing HTML tags and entities.
    """
    if not text or pd.isna(text):
        return ""

    text = str(text)

    # Try BeautifulSoup first for HTML content
    try:
        soup = BeautifulSoup(text, "lxml")

        # Remove script, style, table tags
        for tag in soup(["script", "style", "table", "noscript", "svg"]):
            tag.decompose()

        # Get text
        text = soup.get_text(separator=" ")
    except Exception:
        pass

    # Remove any remaining HTML tags
    text = re.sub(r"<[^>]+>", " ", text)

    # Decode HTML entities
    text = text.replace("&nbsp;", " ")
    text = text.replace("&amp;", "&")
    text = text.replace("&lt;", "<")
    text = text.replace("&gt;", ">")
    text = text.replace("&quot;", '"')
    text = text.replace("&#39;", "'")
    text = text.replace("&#59;", ";")
    text = text.replace("&apos;", "'")

    # Decode numeric entities
    text = re.sub(r"&#(\d+);", lambda m: chr(int(m.group(1))), text)
    text = re.sub(r"&#x([0-9a-fA-F]+);", lambda m: chr(int(m.group(1), 16)), text)

    # Remove font/style attributes that might have leaked through
    text = re.sub(r"font-[a-z\-]+:[^;]+;?", " ", text)
    text = re.sub(r"color:#[0-9a-fA-F]+;?", " ", text)
    text = re.sub(r'style="[^"]*"', " ", text)

    # Collapse multiple spaces
    text = re.sub(r"\s+", " ", text)

    # Clean up
    text = text.strip()

    return text


def main():
    print(f"Loading {INPUT_FILE}...")

    # Read Excel file
    df = pd.read_excel(INPUT_FILE, engine="openpyxl")

    print(f"Found {len(df)} snippets")
    print("\nCleaning snippets...")

    # Clean the Snippet column
    df["Snippet"] = df["Snippet"].apply(clean_snippet)

    # Save to new file
    df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")

    print(f"\n✓ Cleaned snippets saved to: {OUTPUT_FILE}")
    print("\nSample cleaned snippet:")
    if len(df) > 0:
        print(f"Company: {df.iloc[0]['Company Name']}")
        print(f"Date: {df.iloc[0]['Filing Date']}")
        print(f"Keyword: {df.iloc[0]['Keyword']}")
        print(f"Snippet: {df.iloc[0]['Snippet'][:300]}...")


if __name__ == "__main__":
    main()
