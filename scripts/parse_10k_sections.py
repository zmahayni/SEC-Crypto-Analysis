#!/usr/bin/env python3
"""
Parse 10-K filings to extract individual sections (Items).
Start with test mode on a few files, then run on all 1,723 when validated.
"""

import re
import pathlib
import json
from bs4 import BeautifulSoup
from typing import Dict, List, Tuple, Optional

# =============================================================================
# CONFIG
# =============================================================================

HOME = pathlib.Path.home()
FULL_10K_FOLDER = (
    HOME / "Library/CloudStorage/OneDrive-UniversityofTulsa/NSF-BSF Precautions - crypto10k/full_10ks"
)
OUTPUT_FOLDER = HOME / "edgar_tmp" / "parsed_sections"

# Standard 10-K sections we want to extract
STANDARD_ITEMS = [
    "1",    # Business
    "1A",   # Risk Factors
    "1B",   # Unresolved Staff Comments
    "2",    # Properties
    "3",    # Legal Proceedings
    "4",    # Mine Safety Disclosures
    "5",    # Market for Registrant's Common Equity
    "6",    # [Reserved]
    "7",    # Management's Discussion and Analysis (MD&A)
    "7A",   # Quantitative and Qualitative Disclosures About Market Risk
    "8",    # Financial Statements and Supplementary Data
    "9",    # Changes in and Disagreements with Accountants
    "9A",   # Controls and Procedures
    "9B",   # Other Information
    "9C",   # Disclosure Regarding Foreign Jurisdictions
    "10",   # Directors, Executive Officers and Corporate Governance
    "11",   # Executive Compensation
    "12",   # Security Ownership of Certain Beneficial Owners and Management
    "13",   # Certain Relationships and Related Transactions
    "14",   # Principal Accounting Fees and Services
    "15",   # Exhibits, Financial Statement Schedules
]

# =============================================================================
# HTML EXTRACTION
# =============================================================================

def extract_html_from_file(filepath: pathlib.Path) -> Optional[str]:
    """
    Extract HTML content from SEC filing.
    Handles both .txt files (with <DOCUMENT> wrapper) and .htm files.
    """
    try:
        content = filepath.read_text(encoding='utf-8', errors='ignore')

        # If it's a .txt file, extract HTML from <DOCUMENT> tags
        if filepath.suffix == '.txt':
            # Look for <DOCUMENT> ... </DOCUMENT> block containing HTML
            doc_match = re.search(r'<DOCUMENT>(.*?)</DOCUMENT>', content, re.DOTALL)
            if doc_match:
                doc_content = doc_match.group(1)
                # Within the document, find the HTML (usually after <TEXT> tag)
                text_match = re.search(r'<TEXT>(.*)', doc_content, re.DOTALL)
                if text_match:
                    return text_match.group(1)
                # If no <TEXT> tag, the whole document might be HTML
                return doc_content

        # For .htm files, content is already HTML
        return content

    except Exception as e:
        print(f"Error reading {filepath.name}: {e}")
        return None


# =============================================================================
# SECTION PARSING
# =============================================================================

def find_section_boundaries(html_content: str) -> List[Tuple[str, int, str, any]]:
    """
    Find all Item sections in the HTML.

    Returns list of (item_number, position, section_title, html_element) tuples.
    E.g., [("1A", 0, "Risk Factors", <div>), ("7", 1, "Management's Discussion and Analysis", <div>), ...]
    """
    soup = BeautifulSoup(html_content, 'lxml')

    # Pattern to match "ITEM X" or "ITEM XA" with the section title
    # e.g., "ITEM 1A.       RISK FACTORS"
    item_pattern = re.compile(
        r'ITEM\s+(\d+[A-Z]?)[\.\s]+(.+)',
        re.IGNORECASE
    )

    sections = []

    # Find all div/p tags that might contain Item markers
    for tag in soup.find_all(['div', 'p', 'b', 'strong']):
        text = tag.get_text(strip=True)

        # Skip if too short or too long (table of contents vs actual headers)
        if len(text) < 5 or len(text) > 200:
            continue

        # Match the pattern
        match = item_pattern.match(text)
        if match:
            item_num = match.group(1).upper()
            title = match.group(2).strip()

            # Clean up title
            title = ' '.join(title.split())

            # Only include standard 10-K items
            if item_num in STANDARD_ITEMS:
                # Exclude table of contents entries (they're usually just "ITEM X.")
                if len(title) > 3:  # Real headers have actual titles
                    sections.append((item_num, len(sections), title, tag))

    # Remove duplicates (keep only first occurrence of each item)
    seen_items = set()
    unique_sections = []
    for item_num, pos, title, tag in sections:
        if item_num not in seen_items:
            unique_sections.append((item_num, pos, title, tag))
            seen_items.add(item_num)

    return unique_sections


def extract_section_content(html_content: str, sections: List[Tuple[str, int, str, any]]) -> Dict[str, str]:
    """
    Extract the text content for each section.

    Args:
        html_content: Full HTML of the 10-K
        sections: List of (item_num, position, title, tag) from find_section_boundaries()

    Returns:
        Dict mapping item_num -> section_content_dict
    """
    soup = BeautifulSoup(html_content, 'lxml')

    section_content = {}

    for i, (item_num, pos, title, start_tag) in enumerate(sections):
        # Find the next section header (or None if this is the last section)
        next_tag = sections[i + 1][3] if i + 1 < len(sections) else None

        # Extract all content between this section header and the next one
        content_parts = []
        current = start_tag.find_next_sibling()

        # Traverse siblings until we hit the next section header
        while current:
            # Stop if we've reached the next section
            if next_tag and current == next_tag:
                break
            if next_tag and next_tag in current.parents:
                break

            # Add this element's text
            if hasattr(current, 'get_text'):
                text = current.get_text(separator='\n', strip=True)
                if text:
                    content_parts.append(text)

            current = current.find_next_sibling()

        # Combine all content
        content = '\n\n'.join(content_parts)

        # Clean up: remove excessive whitespace
        content = re.sub(r'\n\s*\n\s*\n+', '\n\n', content)

        section_content[item_num] = {
            'title': title,
            'content': content.strip(),
            'char_length': len(content)
        }

    return section_content


# =============================================================================
# MAIN PARSING FUNCTION
# =============================================================================

def parse_10k_file(filepath: pathlib.Path) -> Optional[Dict]:
    """
    Parse a single 10-K file and extract all sections.

    Returns:
        Dict with structure:
        {
            'filename': str,
            'cik': str,
            'sections': {
                '1A': {'title': 'Risk Factors', 'content': '...', 'char_length': 12345},
                '7': {...},
                ...
            }
        }
    """
    print(f"Parsing: {filepath.name}")

    # Extract HTML
    html_content = extract_html_from_file(filepath)
    if not html_content:
        print(f"  ✗ Could not extract HTML")
        return None

    # Find section boundaries
    sections = find_section_boundaries(html_content)
    if not sections:
        print(f"  ✗ No sections found")
        return None

    print(f"  ✓ Found {len(sections)} sections: {[f'{s[0]}:{s[2][:20]}' for s in sections]}")

    # Extract content for each section
    section_content = extract_section_content(html_content, sections)

    # Parse filename for metadata
    filename = filepath.name
    parts = filename.replace('.txt', '').replace('.htm', '').split('_')
    cik = parts[0] if len(parts) > 0 else 'unknown'

    return {
        'filename': filename,
        'cik': cik,
        'filepath': str(filepath),
        'sections_found': [s[0] for s in sections],
        'sections': section_content
    }


# =============================================================================
# TEST MODE
# =============================================================================

def test_on_sample_files(num_files: int = 5, save_output: bool = False):
    """
    Test the parser on a few sample files.

    Args:
        num_files: Number of sample files to test
        save_output: If True, save parsed sections to test_output/ folder
    """
    print("=" * 60)
    print("TEST MODE: Parsing sample files")
    print("=" * 60)

    if not FULL_10K_FOLDER.exists():
        print(f"ERROR: Folder not found: {FULL_10K_FOLDER}")
        return

    # Find first N 10-K files
    sample_files = []
    for cik_folder in sorted(FULL_10K_FOLDER.iterdir()):
        if not cik_folder.is_dir():
            continue

        for filepath in cik_folder.iterdir():
            if filepath.is_file() and '_10-K_' in filepath.name:
                sample_files.append(filepath)
                if len(sample_files) >= num_files:
                    break

        if len(sample_files) >= num_files:
            break

    print(f"\nTesting on {len(sample_files)} files:\n")

    # Parse each sample file
    results = []
    test_output_dir = pathlib.Path("test_output")

    if save_output:
        test_output_dir.mkdir(exist_ok=True)
        print(f"Saving parsed sections to: {test_output_dir.absolute()}\n")

    for filepath in sample_files:
        result = parse_10k_file(filepath)
        if result:
            results.append(result)

            # Print summary
            print(f"\n  Summary for {result['filename']}:")
            print(f"    CIK: {result['cik']}")
            print(f"    Sections found: {len(result['sections'])}")
            for item_num, data in sorted(result['sections'].items(), key=lambda x: STANDARD_ITEMS.index(x[0]) if x[0] in STANDARD_ITEMS else 999):
                print(f"      Item {item_num}: {data['title'][:50]}... ({data['char_length']:,} chars)")

            # Save to file if requested
            if save_output:
                output_file = test_output_dir / f"{result['filename'].replace('.txt', '_parsed.json')}"
                import json
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                print(f"    Saved to: {output_file.name}")
        print()

    # Overall summary
    print("=" * 60)
    print(f"TEST SUMMARY:")
    print(f"  Files parsed: {len(results)}/{len(sample_files)}")
    if results:
        avg_sections = sum(len(r['sections']) for r in results) / len(results)
        print(f"  Average sections per file: {avg_sections:.1f}")

        # Which sections were found most commonly?
        section_counts = {}
        for r in results:
            for item_num in r['sections_found']:
                section_counts[item_num] = section_counts.get(item_num, 0) + 1

        print(f"\n  Section frequency:")
        for item_num in STANDARD_ITEMS:
            if item_num in section_counts:
                count = section_counts[item_num]
                pct = (count / len(results)) * 100
                print(f"    Item {item_num}: {count}/{len(results)} ({pct:.0f}%)")

    print("=" * 60)

    return results


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Main entry point."""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        # Test mode
        num_files = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        save_output = '--save' in sys.argv
        test_on_sample_files(num_files, save_output)
    else:
        print("Usage:")
        print("  python parse_10k_sections.py --test [num_files] [--save]")
        print()
        print("Start with test mode to validate parsing logic:")
        print("  python parse_10k_sections.py --test 5")
        print()
        print("Save parsed sections to test_output/ folder for inspection:")
        print("  python parse_10k_sections.py --test 3 --save")


if __name__ == "__main__":
    main()
