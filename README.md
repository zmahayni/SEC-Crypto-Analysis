## Forms Scanned
`10-K, 10-Q, 8-K, 20-F, 40-F, 6-K`

## Inputs
- Excel `Publicly_Trade_Companies_SEC.xlsx` with columns: `cik`, `name` (first sheet). CIKs are normalized to 10-digit zero-padded strings.

## Destinations
- Staging (local): `~/edgar_tmp/stage/{CIK}/`
- OneDrive root: `~/Library/CloudStorage/OneDrive-UniversityofTulsa/NSF-BSF Precautions - crypto10k/{CIK}/`

Each company gets a folder named by its zero-padded CIK.

## Commands

Run from the repo root using Python 3.10+.

- Main scan to staging only:
```bash
python example.py scan --input ./Publicly_Trade_Companies_SEC.xlsx --years-back 5 --verbose
```

- Flush staged results to OneDrive (safe to run multiple times):
```bash
python example.py flush
```

- Pause (Ctrl-C) and resume:
```bash
# Pause: press Ctrl-C anytime. Progress is persisted to ~/edgar_tmp/progress.csv
# Resume: rerun the same scan command; processed accessions are skipped
python example.py scan --input ./Publicly_Trade_Companies_SEC.xlsx --years-back 5 --verbose
```

- Clear the temp staging directory (and optionally the progress file):
```bash
python example.py clear-temp
python example.py clear-temp --include-progress
```

## Output Naming
- HTML documents: `{CIK}_{FORM}_{YYYY-MM-DD}_{DOCNAME}.htm(l)`
- Master text fallback: `{CIK}_{FORM}_{YYYY-MM-DD}_{ACCESSION}.txt`
- SIC code per company is stored in staging as `SIC.txt` and appended to `~/edgar_tmp/sic_codes.csv`.

## Notes
- Staging keeps OneDrive quiet while scanning; use `flush` to move finished CIK folders.

## Keywords (exact, case-insensitive)
- bitcoin
- digital asset
- distributed ledger
- blockchain
- ethereum
- cryptocurrency
- non-fungible token
- tokenization
- crypto-asset

Hyphens between words are accepted where shown (e.g., `digital-asset`, `non-fungible token`). No shortened words (e.g., `ether`) are matched.

## License
Research/academic use. Review SEC fair access policies before running at scale.
# SEC-Crypto-Analysis