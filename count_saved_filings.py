#!/usr/bin/env python3
"""
Quick script to count saved filings in OneDrive folder.
"""

import pathlib
from collections import defaultdict

HOME = pathlib.Path.home()
ONEDRIVE_FOLDER = (
    HOME / "Library/CloudStorage/OneDrive-UniversityofTulsa/NSF-BSF Precautions - crypto10k"
)

def count_filings():
    """Count saved filings by type."""
    if not ONEDRIVE_FOLDER.exists():
        print(f"ERROR: OneDrive folder not found at {ONEDRIVE_FOLDER}")
        return

    # Count by file extension and form type
    by_extension = defaultdict(int)
    by_form_type = defaultdict(int)
    total_files = 0
    total_ciks = 0

    print(f"Scanning: {ONEDRIVE_FOLDER}\n")

    # Walk through CIK folders
    for cik_folder in ONEDRIVE_FOLDER.iterdir():
        if not cik_folder.is_dir():
            continue

        total_ciks += 1

        # Count files in this CIK folder
        for filepath in cik_folder.iterdir():
            if filepath.is_dir() or filepath.name in ["SIC.txt", "COMPLETE", ".STAGING"]:
                continue

            filename = filepath.name
            total_files += 1

            # Count by extension
            if filename.endswith('.txt'):
                by_extension['txt'] += 1
            elif filename.endswith('.htm') or filename.endswith('.html'):
                by_extension['html'] += 1
            else:
                by_extension['other'] += 1

            # Parse form type from filename: {CIK}_{FORM}_{DATE}_{ACCESSION}
            parts = filename.split('_')
            if len(parts) >= 2:
                form_type = parts[1]
                by_form_type[form_type] += 1

    # Print results
    print(f"Total CIKs with saved filings: {total_ciks}")
    print(f"Total saved files: {total_files}")
    print(f"\nBy file extension:")
    for ext, count in sorted(by_extension.items()):
        print(f"  .{ext}: {count}")

    print(f"\nBy form type:")
    for form, count in sorted(by_form_type.items(), key=lambda x: x[1], reverse=True):
        print(f"  {form}: {count}")

if __name__ == "__main__":
    count_filings()
