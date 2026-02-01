#!/usr/bin/env python3
"""
Auto-classify SEC crypto snippets using hybrid approach:
- High-confidence auto-classify
- Flag uncertain cases for manual review
"""

import pathlib
import re
import pandas as pd

SCRIPT_DIR = pathlib.Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data"

INPUT_FILE = DATA_DIR / "10k_snippets_for_classification.xlsx"
OUTPUT_FILE = DATA_DIR / "10k_snippets_for_classification.xlsx"

CONFIDENCE_THRESHOLD = 0.7

# Keyword patterns for each category (compiled regex)
PATTERNS = {
    "Investment": re.compile(
        r"\b(invest(s|ed|ing|ment)?|treasury|treasuries|hold(s|ing)?|held|"
        r"purchase[ds]?|acquir(e|ed|ing)|portfolio|allocation|asset[s]?)\b",
        re.I
    ),
    "Risk": re.compile(
        r"\b(risk[s]?|volatil(e|ity)|uncertain(ty)?|may (decline|decrease|fluctuate)|"
        r"subject to|could (harm|adversely|negatively)|exposure|vulnerable|threat)\b",
        re.I
    ),
    "Competitive risk": re.compile(
        r"\b(compet(e|itor|ition|itive)|rival[s]?|market share|"
        r"competitive (position|pressure|landscape|threat)|disrupt(ion|ive)?)\b",
        re.I
    ),
    "Regulation": re.compile(
        r"\b(regulat(e|ed|ion|ory|or)|compliance|SEC|CFTC|legal|"
        r"law(s|ful)?|legislation|license|permit|framework|jurisdiction)\b",
        re.I
    ),
    "Employment": re.compile(
        r"\b(officer[s]?|director[s]?|executive[s]?|CEO|CFO|CTO|"
        r"board (member|of directors)|management|background|experience|"
        r"served as|position|appointed|hire[ds]?|compensation)\b",
        re.I
    ),
    "Tech for existing companies": re.compile(
        r"\b(implement(ed|ing|ation)?|platform[s]?|integrat(e|ed|ion)|"
        r"technology|system[s]?|infrastructure|solution[s]?|develop(ed|ing|ment)?|"
        r"software|application[s]?|network|protocol)\b",
        re.I
    ),
    "Business": re.compile(
        r"\b(business|service[s]?|product[s]?|offer(s|ed|ing)?|"
        r"partner(ship)?|strategic|initiative|customer[s]?|client[s]?|"
        r"revenue|operation[s]?|market|expand|growth)\b",
        re.I
    ),
}

# Section to category mappings (primary signals)
SECTION_SIGNALS = {
    "Item 1A": ["Risk", "Competitive risk", "Regulation"],
    "Item 1:": ["Business", "Tech for existing companies"],
    "Item 10": ["Employment"],
    "Item 11": ["Employment"],
    "Item 7:": ["Investment", "Business"],
    "Item 7A": ["Risk", "Investment"],
    "Item 8": ["Investment"],
    "Item 9B": ["Business", "Employment"],
}


def count_pattern_matches(text: str, pattern: re.Pattern) -> int:
    """Count how many times a pattern matches in text."""
    return len(pattern.findall(text))


def classify_snippet(section: str, keyword: str, snippet: str) -> tuple[str, float]:
    """
    Classify a snippet and return (category, confidence).

    Returns:
        tuple of (classification, confidence_score)
    """
    section = str(section) if pd.notna(section) else ""
    snippet = str(snippet) if pd.notna(snippet) else ""
    keyword = str(keyword) if pd.notna(keyword) else ""

    # Score each category
    scores = {cat: 0.0 for cat in PATTERNS.keys()}

    # 1. Section-based scoring (strong signal)
    section_boost = 0.4
    for section_prefix, categories in SECTION_SIGNALS.items():
        if section_prefix in section:
            for cat in categories:
                scores[cat] += section_boost
            break

    # 2. Keyword pattern matching in snippet
    text_to_search = f"{keyword} {snippet}"
    total_matches = 0
    match_counts = {}

    for cat, pattern in PATTERNS.items():
        count = count_pattern_matches(text_to_search, pattern)
        match_counts[cat] = count
        total_matches += count

    # Normalize pattern scores
    if total_matches > 0:
        for cat in scores:
            # Scale to 0.0-0.5 range
            scores[cat] += (match_counts[cat] / total_matches) * 0.5

    # 3. Apply priority rules for specific combinations

    # Employment: Item 10/11 with officer/director mention
    if ("Item 10" in section or "Item 11" in section) and match_counts.get("Employment", 0) > 0:
        scores["Employment"] += 0.3

    # Regulation: Item 1A with regulatory language
    if "Item 1A" in section and match_counts.get("Regulation", 0) > 1:
        scores["Regulation"] += 0.2

    # Competitive risk: Item 1A with competitive language
    if "Item 1A" in section and match_counts.get("Competitive risk", 0) > 0:
        scores["Competitive risk"] += 0.15

    # Tech: Item 1 with implementation language
    if "Item 1:" in section and match_counts.get("Tech for existing companies", 0) > 1:
        scores["Tech for existing companies"] += 0.15

    # Investment: Strong investment language anywhere
    if match_counts.get("Investment", 0) > 2:
        scores["Investment"] += 0.2

    # Find best category
    best_cat = max(scores, key=scores.get)
    best_score = scores[best_cat]

    # Normalize confidence to 0-1 range
    confidence = min(best_score, 1.0)

    # Default fallbacks based on section if score is low
    if confidence < 0.3:
        if "Item 1A" in section:
            best_cat = "Risk"
            confidence = 0.4
        elif "Item 1:" in section:
            best_cat = "Business"
            confidence = 0.4
        elif "Item 10" in section or "Item 11" in section:
            best_cat = "Employment"
            confidence = 0.4

    return best_cat, confidence


def main():
    print(f"Loading {INPUT_FILE}")
    df = pd.read_excel(INPUT_FILE)

    print(f"Processing {len(df)} snippets...")

    classifications = []
    confidences = []

    for idx, row in df.iterrows():
        section = row.get("Section", "")
        keyword = row.get("Keyword", "")
        snippet = row.get("Snippet", "")

        category, confidence = classify_snippet(section, keyword, snippet)

        if confidence >= CONFIDENCE_THRESHOLD:
            classifications.append(category)
        else:
            classifications.append(f"REVIEW: {category}")

        confidences.append(round(confidence, 2))

    df["Classification"] = classifications
    df["Confidence"] = confidences

    # Summary statistics
    print("\n" + "="*60)
    print("CLASSIFICATION SUMMARY")
    print("="*60)

    auto_classified = sum(1 for c in classifications if not c.startswith("REVIEW:"))
    needs_review = sum(1 for c in classifications if c.startswith("REVIEW:"))

    print(f"\nAuto-classified: {auto_classified} ({auto_classified/len(df)*100:.1f}%)")
    print(f"Needs review:    {needs_review} ({needs_review/len(df)*100:.1f}%)")

    print("\nBreakdown by category:")
    # Count both auto and review versions
    category_counts = {}
    for c in classifications:
        cat = c.replace("REVIEW: ", "")
        category_counts[cat] = category_counts.get(cat, 0) + 1

    for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
        auto = sum(1 for c in classifications if c == cat)
        review = sum(1 for c in classifications if c == f"REVIEW: {cat}")
        print(f"  {cat:30s}: {count:3d} total ({auto:3d} auto, {review:3d} review)")

    print("\nConfidence distribution:")
    conf_high = sum(1 for c in confidences if c >= 0.8)
    conf_med = sum(1 for c in confidences if 0.7 <= c < 0.8)
    conf_low = sum(1 for c in confidences if c < 0.7)
    print(f"  High (>=0.8): {conf_high}")
    print(f"  Medium (0.7-0.8): {conf_med}")
    print(f"  Low (<0.7): {conf_low}")

    # Save
    print(f"\nSaving to {OUTPUT_FILE}")
    df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")
    print("Done!")


if __name__ == "__main__":
    main()
