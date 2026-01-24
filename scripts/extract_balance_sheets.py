#!/usr/bin/env python3
"""
Extract balance sheet tables from 10-K Item 8 sections.
Outputs both structured JSON and human-readable formatted text.
"""

import re
import pathlib
import json
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Tuple
import sys

# Add parse_10k_sections to path to reuse functions
sys.path.insert(0, str(pathlib.Path(__file__).parent))
from parse_10k_sections import extract_html_from_file, parse_10k_file

# =============================================================================
# CONFIG
# =============================================================================

HOME = pathlib.Path.home()
FULL_10K_FOLDER = (
    HOME / "Library/CloudStorage/OneDrive-UniversityofTulsa/NSF-BSF Precautions - crypto10k/full_10ks"
)

# =============================================================================
# BALANCE SHEET DETECTION
# =============================================================================

def find_balance_sheet_tables(html_content: str) -> List[BeautifulSoup]:
    """
    Find balance sheet table elements in HTML.

    Args:
        html_content: Full HTML or Item 8 section HTML

    Returns:
        List of BeautifulSoup table elements
    """
    soup = BeautifulSoup(html_content, 'lxml')

    # Pattern to match balance sheet titles
    title_pattern = re.compile(
        r'(CONSOLIDATED\s+)?BALANCE\s+SHEET',
        re.IGNORECASE
    )

    tables = []

    # Search for balance sheet title, then find following table
    for elem in soup.find_all(['p', 'div', 'td', 'b', 'strong']):
        text = elem.get_text(strip=True)

        if title_pattern.search(text):
            # Found a balance sheet title, look for next table
            current = elem

            # Search siblings and descendants for table
            for _ in range(10):  # Look ahead up to 10 elements
                current = current.find_next(['table', 'p', 'div'])
                if not current:
                    break

                if current.name == 'table':
                    # Verify it's actually a balance sheet table (has rows/columns)
                    rows = current.find_all('tr')
                    if len(rows) > 5:  # Balance sheet should have many rows
                        tables.append(current)
                        break

    return tables


# =============================================================================
# TABLE PARSING
# =============================================================================

def get_indent_level(cell) -> int:
    """
    Determine indentation level of a table cell based on CSS styling.

    Returns:
        Integer level (0 = no indent, 1 = first level, 2 = second level, etc.)
    """
    if not cell:
        return 0

    # Check style attribute for margin-left or text-indent
    style = cell.get('style', '')

    # Extract margin-left value
    margin_match = re.search(r'margin-left:\s*(\d+)', style)
    if margin_match:
        pixels = int(margin_match.group(1))
        # Convert pixels to indent level (rough approximation)
        # Typically 18-20pt per level
        return min(pixels // 18, 5)

    # Check text-indent
    indent_match = re.search(r'text-indent:\s*(-?\d+)', style)
    if indent_match:
        pixels = abs(int(indent_match.group(1)))
        return min(pixels // 18, 5)

    return 0


def is_subtotal_row(cells: List) -> bool:
    """
    Determine if a row represents a subtotal based on styling.
    Subtotals typically have bold text and/or border lines.
    """
    if not cells:
        return False

    first_cell = cells[0]

    # Check for bold tags
    if first_cell.find(['b', 'strong']):
        return True

    # Check for border styling
    style = first_cell.get('style', '')
    if 'border-top' in style or 'border-bottom' in style:
        return True

    # Check text content for "Total" keyword
    text = first_cell.get_text(strip=True)
    if re.match(r'Total\s+', text, re.IGNORECASE):
        return True

    return False


def normalize_financial_value(text: str) -> Optional[float]:
    """
    Convert financial value string to float.

    Handles:
    - Commas: "26,800" -> 26800.0
    - Parentheses (negatives): "(1,500)" -> -1500.0
    - Currency symbols: "$26,800" -> 26800.0
    - Dashes (zero/None): "—" or "-" -> None
    - Empty cells -> None

    Args:
        text: String representation of number

    Returns:
        Float value or None if unparseable
    """
    if not text:
        return None

    # Clean up text
    text = text.strip()

    # Handle dashes and empty cells
    if text in ['—', '-', '–', '']:
        return None

    # Check for parentheses (negative)
    is_negative = False
    if text.startswith('(') and text.endswith(')'):
        is_negative = True
        text = text[1:-1]

    # Remove currency symbols, commas, whitespace
    text = re.sub(r'[\$,\s]', '', text)

    # Try to convert to float
    try:
        value = float(text)
        return -value if is_negative else value
    except ValueError:
        return None


def parse_balance_sheet_table(table: BeautifulSoup) -> Optional[Dict]:
    """
    Parse a balance sheet table into structured format.

    Returns:
        Dict with structure:
        {
            "years": ["2024", "2023", "2022"],
            "line_items": [
                {
                    "name": "Cash and cash equivalents",
                    "level": 2,
                    "is_subtotal": False,
                    "values": [26800.0, 20500.0, 15000.0]
                },
                ...
            ]
        }
    """
    if not table:
        return None

    rows = table.find_all('tr')
    if len(rows) < 5:
        return None

    # Extract column headers (years) from first few rows
    years = []
    header_rows = rows[:5]  # Check first 5 rows for headers

    for row in header_rows:
        cells = row.find_all(['th', 'td'])
        for cell in cells[1:]:  # Skip first column (labels)
            text = cell.get_text(strip=True)
            # Look for year patterns (4 digits)
            year_match = re.search(r'(20\d{2})', text)
            if year_match:
                years.append(year_match.group(1))

        if years:
            break

    if not years:
        # Fallback: assume 2-3 year columns
        years = ["Year_1", "Year_2", "Year_3"]

    # Parse data rows
    line_items = []

    for row in rows[1:]:  # Skip header row(s)
        cells = row.find_all(['td', 'th'])

        if len(cells) < 2:
            continue

        # First cell is the line item name
        name_cell = cells[0]
        name = name_cell.get_text(strip=True)

        # Skip empty rows or page markers
        if not name or len(name) < 2:
            continue
        if 'page' in name.lower() or name.isdigit():
            continue

        # Get indent level
        level = get_indent_level(name_cell)

        # Check if subtotal
        is_subtotal = is_subtotal_row(cells)

        # Extract numeric values
        values = []
        for cell in cells[1:len(years)+1]:  # Only as many as years
            value_text = cell.get_text(strip=True)
            value = normalize_financial_value(value_text)
            values.append(value)

        # Pad values if needed
        while len(values) < len(years):
            values.append(None)

        line_items.append({
            "name": name,
            "level": level,
            "is_subtotal": is_subtotal,
            "values": values
        })

    return {
        "years": years,
        "line_items": line_items
    }


# =============================================================================
# KEY METRICS EXTRACTION
# =============================================================================

def extract_key_metrics(parsed_table: Dict) -> Dict:
    """
    Extract key financial metrics from parsed table.

    Searches for:
    - Total assets
    - Total liabilities
    - Total stockholders' equity / shareholders' equity
    - Total current assets
    - Total current liabilities

    Returns:
        Dict with most recent year values (first column)
    """
    if not parsed_table or not parsed_table.get('line_items'):
        return {}

    metrics = {}

    # Search patterns for key items
    patterns = {
        'total_assets': re.compile(r'^total\s+(consolidated\s+)?assets\s*$', re.IGNORECASE),
        'total_current_assets': re.compile(r'total\s+current\s+assets', re.IGNORECASE),
        'total_liabilities': re.compile(r'^total\s+liabilities\s*$', re.IGNORECASE),  # Exclude "and stockholders' equity"
        'total_current_liabilities': re.compile(r'total\s+current\s+liabilities', re.IGNORECASE),
        'stockholders_equity': re.compile(
            r'total\s+(stockholders|shareholders|shareowners).?\s+(equity|deficit)',
            re.IGNORECASE
        ),
    }

    for item in parsed_table['line_items']:
        name = item['name']

        for metric_key, pattern in patterns.items():
            if pattern.search(name):
                # Get first non-None value (most recent year available)
                for val in item['values']:
                    if val is not None:
                        metrics[metric_key] = val
                        break
                break  # Found this metric, move to next line item

    return metrics


# =============================================================================
# HUMAN-READABLE FORMATTING
# =============================================================================

def format_table_for_display(parsed_table: Dict) -> str:
    """
    Format parsed table as human-readable text with alignment and indentation.

    Returns:
        Formatted string suitable for display/review
    """
    if not parsed_table:
        return "No balance sheet data"

    years = parsed_table.get('years', [])
    line_items = parsed_table.get('line_items', [])

    if not line_items:
        return "No line items found"

    # Build header
    output = []
    output.append("Balance Sheet")
    output.append("=" * 80)
    output.append("")

    # Column headers
    header = f"{'Line Item':<50}"
    for year in years[:3]:  # Max 3 years for display
        header += f"{year:>15}"
    output.append(header)
    output.append("-" * 80)

    # Format each line item
    for item in line_items:
        name = item['name']
        level = item['level']
        values = item['values'][:3]  # Max 3 years
        is_subtotal = item['is_subtotal']

        # Add indentation
        indent = "  " * level
        display_name = indent + name

        # Truncate if too long
        if len(display_name) > 48:
            display_name = display_name[:45] + "..."

        # Format values
        line = f"{display_name:<50}"
        for val in values:
            if val is None:
                line += f"{'—':>15}"
            elif abs(val) >= 1_000_000:
                # Show in millions with 1 decimal
                line += f"{val/1_000_000:>14,.1f}M"
            else:
                # Show in thousands with commas
                line += f"{val:>14,.0f} "

        output.append(line)

        # Add separator line for subtotals
        if is_subtotal:
            output.append("-" * 80)

    output.append("=" * 80)

    return "\n".join(output)


# =============================================================================
# MAIN EXTRACTION FUNCTION
# =============================================================================

def extract_balance_sheet_from_10k(filepath: pathlib.Path) -> Optional[Dict]:
    """
    Extract balance sheet from a 10-K file.

    Returns:
        Dict with structure:
        {
            'filename': str,
            'cik': str,
            'balance_sheet_structured': {...},  # Parsed table
            'balance_sheet_formatted': str,      # Human-readable
            'key_metrics': {...}                 # Total Assets, etc.
        }
    """
    print(f"Extracting balance sheet from: {filepath.name}")

    # First parse the full 10-K to get Item 8
    parsed_10k = parse_10k_file(filepath)
    if not parsed_10k:
        print(f"  ✗ Could not parse 10-K")
        return None

    # Get Item 8 content
    item_8 = parsed_10k.get('sections', {}).get('8')
    if not item_8:
        print(f"  ✗ No Item 8 (Financial Statements) found")
        return None

    print(f"  ✓ Item 8 found ({item_8['char_length']:,} chars)")

    # Extract HTML for Item 8 (need to re-parse with HTML preserved)
    html_content = extract_html_from_file(filepath)
    if not html_content:
        return None

    # Find balance sheet tables
    tables = find_balance_sheet_tables(html_content)

    if not tables:
        print(f"  ✗ No balance sheet tables found")
        return None

    print(f"  ✓ Found {len(tables)} balance sheet table(s)")

    # Parse the first balance sheet table
    parsed_table = parse_balance_sheet_table(tables[0])

    if not parsed_table:
        print(f"  ✗ Could not parse balance sheet table")
        return None

    print(f"  ✓ Parsed {len(parsed_table['line_items'])} line items")

    # Extract key metrics
    metrics = extract_key_metrics(parsed_table)
    print(f"  ✓ Extracted {len(metrics)} key metrics")

    if metrics.get('total_assets'):
        print(f"    Total Assets: ${metrics['total_assets']:,.0f}")

    # Format for display
    formatted = format_table_for_display(parsed_table)

    return {
        'filename': filepath.name,
        'cik': parsed_10k['cik'],
        'balance_sheet_structured': parsed_table,
        'balance_sheet_formatted': formatted,
        'key_metrics': metrics
    }


# =============================================================================
# TEST MODE
# =============================================================================

def test_on_sample_files(num_files: int = 5, save_output: bool = False):
    """
    Test balance sheet extraction on sample files.
    """
    print("=" * 60)
    print("TEST MODE: Extracting balance sheets from sample files")
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

    # Test each file
    results = []
    test_output_dir = pathlib.Path("test_output/balance_sheets")

    if save_output:
        test_output_dir.mkdir(parents=True, exist_ok=True)
        print(f"Saving results to: {test_output_dir.absolute()}\n")

    for filepath in sample_files:
        result = extract_balance_sheet_from_10k(filepath)

        if result:
            results.append(result)

            # Print formatted balance sheet
            print("\n" + "=" * 60)
            print(result['balance_sheet_formatted'])
            print("=" * 60)

            # Save if requested
            if save_output:
                base_name = result['filename'].replace('.txt', '').replace('.htm', '')

                # Save JSON
                json_file = test_output_dir / f"{base_name}_balance_sheet.json"
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'filename': result['filename'],
                        'cik': result['cik'],
                        'balance_sheet': result['balance_sheet_structured'],
                        'key_metrics': result['key_metrics']
                    }, f, indent=2)

                # Save formatted text
                txt_file = test_output_dir / f"{base_name}_balance_sheet.txt"
                txt_file.write_text(result['balance_sheet_formatted'], encoding='utf-8')

                print(f"\n  Saved: {json_file.name} and {txt_file.name}")

        print("\n")

    # Summary
    print("=" * 60)
    print(f"TEST SUMMARY:")
    print(f"  Files processed: {len(results)}/{len(sample_files)}")

    if results:
        metrics_found = sum(1 for r in results if r.get('key_metrics'))
        print(f"  Balance sheets with key metrics: {metrics_found}/{len(results)}")

        # Show distribution of Total Assets
        total_assets = [r['key_metrics'].get('total_assets') for r in results if r.get('key_metrics', {}).get('total_assets')]
        if total_assets:
            print(f"\n  Total Assets range:")
            print(f"    Min: ${min(total_assets):,.0f}")
            print(f"    Max: ${max(total_assets):,.0f}")
            print(f"    Avg: ${sum(total_assets)/len(total_assets):,.0f}")

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
        print("  python extract_balance_sheets.py --test [num_files] [--save]")
        print()
        print("Test balance sheet extraction:")
        print("  python extract_balance_sheets.py --test 5")
        print()
        print("Save results to test_output/balance_sheets/:")
        print("  python extract_balance_sheets.py --test 3 --save")


if __name__ == "__main__":
    main()
