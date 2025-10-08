import sys, re
from pathlib import Path
import pandas as pd

def read_lines(p: Path):
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            return p.read_text(encoding=enc).splitlines()
        except Exception:
            pass
    return p.read_text(encoding="utf-8", errors="ignore").splitlines()

def parse_one(txt: Path):
    lines = read_lines(txt)

    # Find the start of <SEC-DOCUMENT>
    start = next((i for i, l in enumerate(lines) if l.strip().startswith("<SEC-DOCUMENT>")), None)
    if start is None:
        return None

    header = "\n".join(lines[start:start+50])
    filed = re.search(r"FILED AS OF DATE:\s*([0-9]{8})", header)
    cik   = re.search(r"CENTRAL INDEX KEY:\s*([0-9]+)", header)
    sic   = re.search(r"STANDARD INDUSTRIAL CLASSIFICATION:.*?\[([0-9]{4})\]", header)

    return {
        "FILED AS OF DATE": filed.group(1) if filed else "",
        "CIK": f"CIK {cik.group(1).zfill(10)}" if cik else "",
        "SIC Code": sic.group(1) if sic else ""
    } if (filed or cik or sic) else None

def main():
    if len(sys.argv) < 2:
        print("Usage: python extract_cik_sic_filed.py <root_folder>")
        sys.exit(1)

    root = Path(sys.argv[1]).expanduser()
    if not root.exists():
        print(f"Path does not exist: {root}")
        sys.exit(1)

    records = [rec for txt in root.rglob("*.txt") if (rec := parse_one(txt))]
    if not records:
        print("No records found.")
        sys.exit(0)

    df = pd.DataFrame(records, columns=["FILED AS OF DATE", "CIK", "SIC Code"])
    out_dir = root / "excel"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "edgar_s4_summary.xlsx"

    with pd.ExcelWriter(out_path, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="S-4 Summary")

    print(f"Wrote {len(df)} rows to: {out_path}")

if __name__ == "__main__":
    main()
