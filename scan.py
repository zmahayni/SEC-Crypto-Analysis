# =============================================================================
# SEC-Crypto-Analysis - High-Performance Version
# =============================================================================
# Optimizations:
# 1. Improved HTTP connection pooling and reuse (pool_connections=64, pool_maxsize=256)
# 2. Smart retry handling with Retry-After header support
# 3. Global document executor for better resource utilization
# 4. High concurrency settings (CIK_CONCURRENCY=6, DOC_CONCURRENCY=15)
# 5. Early termination in stream scanning when keywords are found
# 6. HEAD requests to check file size before downloading
# 7. Increased chunk sizes for better throughput (256KB)
# 8. PyMuPDF for faster PDF processing with page limiting
# 9. In-memory PDF processing instead of temp files
# 10. Higher RPS limit (9.5) while respecting SEC guidelines
# =============================================================================

import argparse
import re
import shutil
import time
import pathlib
import datetime as dt
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from collections import deque
import signal
import sys

# =============================================================================
# CONFIG (edit these)
# =============================================================================

HOME = pathlib.Path.home()

INPUT_XLSX = pathlib.Path(
    "Publicly_Trade_Companies_SEC.xlsx"
)  # Excel with columns: cik, name
BASE_FOLDER = (
    HOME
    / "Library/CloudStorage/OneDrive-UniversityofTulsa/NSF-BSF Precautions - crypto10k"
)
TMP_ROOT = HOME / "edgar_tmp"
STAGE_DIR = TMP_ROOT / "stage"
PROGRESS_FILE = TMP_ROOT / "progress.txt"

YEARS_BACK = 5
FORMS = {"10-K", "10-Q", "8-K", "20-F", "40-F", "6-K"}

# Keywords (exact, case-insensitive; allow hyphens/spaces within phrases)
KEYWORDS = re.compile(
    r"\b("
    r"bitcoin|blockchain|ethereum|cryptocurrency|"
    r"digital[- ]asset|distributed[- ]ledger|non[- ]fungible[- ]token|crypto[- ]asset"
    r")\b",
    re.I,
)

# Throughput
MAX_RPS = 9.8  # global cap - pushing close to SEC limit
CIK_CONCURRENCY = 10  # parallel CIKs (increased from 6)
DOC_CONCURRENCY = 20  # parallel exhibits per filing (increased from 15)
MAX_SAVE_MB_PER_FILE = 20  # don't save > 20MB files

# Include PDFs? (needs pdfminer.six; will skip gracefully if not installed)
# Disabled for speed - PDFs are slow to process
INCLUDE_PDF_EXHIBITS = False

# HTTP headers
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36 "
    "Uni Tulsa research - zade@example.com"
)

# Endpoints
SUB_JSON = "https://data.sec.gov/submissions/CIK{cik}.json"
IDX_JSON = "https://www.sec.gov/Archives/edgar/data/{cik_nolead}/{folder}/index.json"
DOC_PATH = "https://www.sec.gov/Archives/edgar/data/{cik_nolead}/{folder}/{name}"
TXT_PATH = "https://www.sec.gov/Archives/edgar/data/{cik_nolead}/{acc_dash}.txt"

# Backoff for 429s/network hiccups
MAX_RETRIES = 3
BACKOFF = [15, 30, 60]  # seconds - reduced wait times

# =============================================================================
# Internal state / helpers
# =============================================================================

_TLS = threading.local()
STOP_EVENT = threading.Event()
# Global document executor for better resource utilization
DOC_EXECUTOR = ThreadPoolExecutor(max_workers=DOC_CONCURRENCY)
# Script start time for runtime tracking
SCRIPT_START_TIME = time.time()


def _get_session():
    s = getattr(_TLS, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": UA, "Accept": "application/json"})

        # Configure connection pooling and retries
        retry_strategy = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=128,
            pool_maxsize=512,
            pool_block=True,
        )
        s.mount("http://", adapter)
        s.mount("https://", adapter)

        _TLS.session = s
    return s


class RateLimiter:
    def __init__(self, max_rps: float):
        self.min_interval = 1.0 / max(0.5, float(max_rps))
        self.lock = threading.Lock()
        self.last = 0.0

    def acquire(self):
        with self.lock:
            now = time.perf_counter()
            wait = self.min_interval - (now - self.last)
            if wait > 0:
                time.sleep(wait)
                now = time.perf_counter()
            self.last = now


RATE_LIMITER = RateLimiter(MAX_RPS)

REQ_LOCK = threading.Lock()
_REQ_TIMES: deque = deque()
_REQ_COUNT = 0


def _record_request(window_sec: float = 10.0):
    """Lightweight telemetry (not printed unless error)."""
    global _REQ_COUNT
    now = time.perf_counter()
    with REQ_LOCK:
        _REQ_TIMES.append(now)
        cutoff = now - window_sec
        while _REQ_TIMES and _REQ_TIMES[0] < cutoff:
            _REQ_TIMES.popleft()
        _REQ_COUNT += 1


def format_runtime():
    """Format runtime as HH:MM:SS"""
    runtime_seconds = int(time.time() - SCRIPT_START_TIME)
    hours, remainder = divmod(runtime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def info(msg: str):
    # minimal console output for: new CIK start, saves, errors
    print(f"[{format_runtime()}] {msg}", flush=True)


def error(msg: str):
    print(f"[{format_runtime()}] ERROR: {msg}", file=sys.stderr, flush=True)


def ensure_dirs():
    STAGE_DIR.mkdir(parents=True, exist_ok=True)
    TMP_ROOT.mkdir(parents=True, exist_ok=True)


def get_with_backoff(url: str, label: str, stream: bool = False):
    for attempt in range(MAX_RETRIES):
        if STOP_EVENT.is_set():
            return None
        RATE_LIMITER.acquire()
        try:
            r = _get_session().get(url, timeout=(15, 60), stream=stream)
            _record_request()
        except requests.RequestException as e:
            wait = BACKOFF[min(attempt, len(BACKOFF) - 1)]
            error(
                f"net error on {label}: {e.__class__.__name__}; sleep {wait}s (try {attempt + 1})"
            )
            time.sleep(wait)
            continue
        if r.status_code != 429:
            return r

        # Handle 429 with Retry-After if available
        retry_after = r.headers.get("Retry-After")
        if retry_after:
            try:
                # Retry-After can be seconds or HTTP date
                wait = float(retry_after)
            except ValueError:
                # If it's an HTTP date, default to our backoff
                wait = BACKOFF[min(attempt, len(BACKOFF) - 1)]
        else:
            wait = BACKOFF[min(attempt, len(BACKOFF) - 1)]

        info(f"429 on {label}; sleeping {wait}s (try {attempt + 1})")
        time.sleep(wait)
    error(f"gave up on {label} after {MAX_RETRIES} retries")
    return None


def save_text_raw(text: str, path: pathlib.Path):
    path.write_text(text, encoding="utf-8", errors="ignore")


def size_under_limit(resp: requests.Response) -> bool:
    try:
        cl = resp.headers.get("Content-Length")
        if cl is None:  # unknown size
            return True
        mb = int(cl) / (1024 * 1024)
        return mb <= MAX_SAVE_MB_PER_FILE
    except Exception:
        return True


# PDF text extraction (lazy import)
def pdf_to_text_bytes(content: bytes) -> str:
    try:
        # Try to use PyMuPDF (fitz) if available - much faster than pdfminer
        try:
            import fitz

            with fitz.open(stream=content, filetype="pdf") as doc:
                # Only extract first 10 pages for speed
                max_pages = min(10, doc.page_count)
                text = ""
                for i in range(max_pages):
                    text += doc[i].get_text()
                return text
        except ImportError:
            # Fall back to pdfminer if PyMuPDF not available
            from pdfminer.high_level import extract_text
            import io

            # Use BytesIO instead of temp file
            with io.BytesIO(content) as pdf_stream:
                text = extract_text(pdf_stream) or ""
                return text
    except Exception as e:
        error(f"PDF extraction failed: {e}")
        return ""


# Stream-scan text-like resources
def stream_scan_for_keywords(url: str):
    r = get_with_backoff(url, url, stream=True)
    if not r or r.status_code != 200:
        return False, ""
    r.encoding = r.encoding or "utf-8"
    text_buf = []
    acc_len = 0
    limit = 1_000_000
    found_keyword = False
    try:
        # Increased chunk size for better throughput
        for chunk in r.iter_content(chunk_size=256_000, decode_unicode=True):
            if not chunk:
                continue
            # Check for keywords first before appending to buffer
            if not found_keyword and KEYWORDS.search(chunk):
                found_keyword = True

            text_buf.append(chunk)
            acc_len += len(chunk)
            if acc_len > limit:
                s = "".join(text_buf)
                s = s[-limit:]
                text_buf = [s]
                acc_len = len(s)

            # Super early return if we've found a keyword
            if found_keyword and acc_len > 25_000:  # Minimal context for maximum speed
                return True, "".join(text_buf)

        return found_keyword, "".join(text_buf)
    finally:
        try:
            r.close()
        except Exception:
            pass


# Fetch whole file bytes (for PDFs)
def fetch_bytes(url: str) -> bytes:
    # First check size with HEAD request
    head = get_with_backoff(url, f"HEAD {url}", stream=False)
    if head and head.status_code == 200 and not size_under_limit(head):
        info(f"skip large download (> {MAX_SAVE_MB_PER_FILE} MB) based on HEAD")
        return b""

    r = get_with_backoff(url, url, stream=True)
    if not r or r.status_code != 200:
        return b""
    try:
        chunks = []
        total = 0
        # Increased chunk size for better throughput
        for chunk in r.iter_content(chunk_size=256_000):
            if not chunk:
                continue
            chunks.append(chunk)
            total += len(chunk)
            if total > MAX_SAVE_MB_PER_FILE * 1024 * 1024:
                info(f"skip large download (> {MAX_SAVE_MB_PER_FILE} MB)")
                return b""
        return b"".join(chunks)
    finally:
        try:
            r.close()
        except Exception:
            pass


def choose_docs(manifest, primary=None):
    """
    Return ALL exhibits, primary first, then the rest.
    Includes .htm/.html/.txt and, optionally, .pdf.
    """
    primary = (primary or "").lower()
    m = [d for d in manifest if "name" in d]
    # primary first
    out = []
    for d in m:
        nm = d["name"].lower()
        if nm == primary and (
            nm.endswith((".htm", ".html", ".txt"))
            or (INCLUDE_PDF_EXHIBITS and nm.endswith(".pdf"))
        ):
            out.append(d)
            break
    # then all others
    for d in m:
        nm = d["name"].lower()
        if d in out:
            continue
        if "index" in nm:
            continue
        if nm.endswith((".htm", ".html", ".txt")) or (
            INCLUDE_PDF_EXHIBITS and nm.endswith(".pdf")
        ):
            out.append(d)
    return out


def process_doc(cik10, form, filed, cik_int, folder, d, cik_stage) -> int:
    name_doc = d["name"]
    url = DOC_PATH.format(cik_nolead=cik_int, folder=folder, name=name_doc)
    # handle PDF separately
    if INCLUDE_PDF_EXHIBITS and name_doc.lower().endswith(".pdf"):
        # HEAD/size check
        head = get_with_backoff(url, f"HEAD {name_doc}")
        if head and head.status_code == 200 and not size_under_limit(head):
            return 0
        data = fetch_bytes(url)
        if not data:
            return 0
        text = pdf_to_text_bytes(data)
        if text and KEYWORDS.search(text):
            out = cik_stage / f"{cik10}_{form}_{filed}_{name_doc}"
            out.write_bytes(data)
            info(f"saved {out.name}")
            return 1
        return 0
    # text-like
    hit, content = stream_scan_for_keywords(url)
    if not hit:
        return 0
    # Optional HEAD to enforce save size cap
    head = get_with_backoff(url, f"HEAD {name_doc}")
    if head and head.status_code == 200 and not size_under_limit(head):
        info(f"skip saving large file (> {MAX_SAVE_MB_PER_FILE} MB): {name_doc}")
        return 1
    out_name = f"{cik10}_{form}_{filed}_{name_doc}"
    save_text_raw(content, cik_stage / out_name)
    info(f"saved {out_name}")
    return 1


def flush_to_onedrive(dest: pathlib.Path):
    ensure_dirs()
    dest.mkdir(parents=True, exist_ok=True)
    moved = 0
    for cik_dir in STAGE_DIR.iterdir():
        if not cik_dir.is_dir():
            continue
        if not (cik_dir / "COMPLETE").exists():
            continue  # skip in-progress
        target = dest / cik_dir.name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.mkdir(exist_ok=True, parents=True)
        for p in cik_dir.iterdir():
            if p.name in {"COMPLETE", ".STAGING"}:
                continue
            shutil.move(str(p), str(target / p.name))
        try:
            cik_dir.rmdir()
        except OSError:
            pass
        moved += 1
    info(f"Flushed {moved} CIK folder(s) to OneDrive")


def on_sigint(sig, frame):
    total_runtime = format_runtime()
    info(
        f"Ctrl-C detected after {total_runtime} runtime → flushing completed CIKs to OneDrive before exit…"
    )
    STOP_EVENT.set()
    flush_to_onedrive(BASE_FOLDER)
    info(f"Total runtime: {total_runtime} - Exiting now.")
    sys.exit(130)


# =============================================================================
# Core
# =============================================================================


def process_filing(cik10, form, filed, acc_dash, cik_int, cik_stage, primary_name):
    folder = acc_dash.replace("-", "")

    # 1) Try master .txt first
    txt_url = TXT_PATH.format(cik_nolead=cik_int, acc_dash=acc_dash)
    hit_txt, content_txt = stream_scan_for_keywords(txt_url)
    if hit_txt:
        head = get_with_backoff(txt_url, f"HEAD {acc_dash}.txt")
        if not head or head.status_code != 200 or size_under_limit(head):
            out = cik_stage / f"{cik10}_{form}_{filed}_{acc_dash}.txt"
            save_text_raw(content_txt, out)
            info(f"saved {out.name}")
        return

    # 2) Fallback: index.json → primary + ALL exhibits
    idx_url = IDX_JSON.format(cik_nolead=cik_int, folder=folder)
    r_idx = get_with_backoff(idx_url, f"{acc_dash} index.json")
    if not r_idx or r_idx.status_code != 200:
        return

    try:
        manifest = r_idx.json()["directory"]["item"]
    except Exception:
        manifest = []

    docs = choose_docs(manifest, primary=primary_name)
    if not docs:
        return

    # primary first, then exhibits (with concurrency)
    hits = 0
    if DOC_CONCURRENCY <= 1:
        for d in docs:
            if STOP_EVENT.is_set():
                break
            hits += process_doc(cik10, form, filed, cik_int, folder, d, cik_stage)
    else:
        # run primary first synchronously to allow early exit on hit
        hits += process_doc(cik10, form, filed, cik_int, folder, docs[0], cik_stage)
        if hits == 0 and len(docs) > 1:
            # Use global executor instead of creating a new one each time
            futures = [
                DOC_EXECUTOR.submit(
                    process_doc, cik10, form, filed, cik_int, folder, d, cik_stage
                )
                for d in docs[1:]
            ]
            for fu in as_completed(futures):
                if STOP_EVENT.is_set():
                    break
                try:
                    hits += fu.result()
                except Exception as e:
                    error(f"doc worker error: {e}")


def process_cik(cik10: str, name: str, cik_index: int = 0, total_ciks: int = 0):
    if STOP_EVENT.is_set():
        return
    # CIK staging
    cik_stage = STAGE_DIR / cik10
    cik_stage.mkdir(parents=True, exist_ok=True)
    (cik_stage / ".STAGING").write_text("in-progress", encoding="utf-8")

    # Show progress as count/total
    if total_ciks > 0:
        info(f"{cik_index}/{total_ciks}    CIK {cik10} – {name}")
    else:
        info(f"Start CIK {cik10} – {name}")

    # submissions JSON
    r_meta = get_with_backoff(SUB_JSON.format(cik=cik10), "submissions JSON")
    if not r_meta or r_meta.status_code != 200:
        error("could not fetch submissions JSON")
        return
    meta = r_meta.json()

    # SIC file for every company
    sic = str(meta.get("sic") or meta.get("companyInfo", {}).get("sic") or "")
    try:
        (cik_stage / "SIC.txt").write_text(sic, encoding="utf-8")
    except Exception as e:
        error(f"write SIC failed: {e}")

    recent = meta.get("filings", {}).get("recent", {})
    if not recent:
        (cik_stage / "COMPLETE").write_text("0 filings", encoding="utf-8")
        (cik_stage / ".STAGING").unlink(missing_ok=True)
        return

    now_year = dt.datetime.utcnow().year
    accs = []
    for i in range(len(recent.get("accessionNumber", []))):
        try:
            if (
                recent["form"][i] in FORMS
                and int(recent["filingDate"][i][:4]) >= now_year - YEARS_BACK
            ):
                accs.append(
                    (
                        recent["accessionNumber"][i],
                        recent["form"][i],
                        recent["filingDate"][i],
                        (recent.get("primaryDocument") or [None])[i],
                    )
                )
        except Exception:
            continue

    for acc_dash, form, filed, primary_name in accs:
        if STOP_EVENT.is_set():
            break
        try:
            process_filing(
                cik10, form, filed, acc_dash, int(cik10), cik_stage, primary_name
            )
        except Exception as e:
            error(f"filing error {acc_dash}: {e}")

    if not STOP_EVENT.is_set():
        (cik_stage / "COMPLETE").write_text("done", encoding="utf-8")
        (cik_stage / ".STAGING").unlink(missing_ok=True)
        # Record progress
        try:
            PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(PROGRESS_FILE, "a", encoding="utf-8") as f:
                f.write(f"{cik10}\n")
        except Exception as e:
            error(f"could not write progress for {cik10}: {e}")


def read_ciks():
    df = pd.read_excel(INPUT_XLSX, engine="openpyxl", dtype=str).rename(
        columns={"CIK": "cik", "Cik": "cik", "name": "name", "Name": "name"}
    )
    df = df.dropna(subset=["cik"]).assign(
        cik=lambda d: d["cik"]
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.zfill(10),
        name=lambda d: d.get("name", "").astype(str).fillna(""),
    )
    df = df.drop_duplicates(subset=["cik"]).reset_index(drop=True)
    return [(r["cik"], r["name"]) for _, r in df.iterrows()]


def get_last_processed_cik() -> str | None:
    try:
        if PROGRESS_FILE.exists():
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                lines = [ln.strip() for ln in f.readlines() if ln.strip()]
                if lines:
                    return lines[-1]
    except Exception as e:
        error(f"failed reading progress file: {e}")
    return None


def run(start_from_cik: str | None):
    ensure_dirs()
    ciks = read_ciks()
    total_ciks = len(ciks)

    # If user wants to start from a specific CIK, skip until we find it
    if start_from_cik:
        start_from_cik = str(start_from_cik).zfill(10)
        try:
            idx = [c for c, _ in ciks].index(start_from_cik)
            ciks = ciks[idx:]
            info(f"Starting from CIK {start_from_cik} ({idx + 1}/{total_ciks})")
        except ValueError:
            info(f"start_from_cik {start_from_cik} not found; starting from beginning")

    if CIK_CONCURRENCY <= 1:
        for i, (cik10, name) in enumerate(ciks):
            if STOP_EVENT.is_set():
                break
            # Calculate the actual index in the full list
            actual_idx = total_ciks - len(ciks) + i + 1
            process_cik(cik10, name, actual_idx, total_ciks)
    else:
        with ThreadPoolExecutor(max_workers=CIK_CONCURRENCY) as ex:
            # Create a list to track the index of each CIK
            cik_indices = [
                (i + total_ciks - len(ciks) + 1, cik10, name)
                for i, (cik10, name) in enumerate(ciks)
            ]
            futures = [
                ex.submit(process_cik, cik10, name, idx, total_ciks)
                for idx, cik10, name in cik_indices
            ]
            for fu in as_completed(futures):
                if STOP_EVENT.is_set():
                    break
                try:
                    fu.result()
                except Exception as e:
                    error(f"CIK worker error: {e}")


# =============================================================================
# CLI (only one optional arg)
# =============================================================================


def parse_args():
    p = argparse.ArgumentParser(
        description="EDGAR crypto scan (simple config, resumable via --start-from-cik or --resume-from-last)"
    )
    p.add_argument(
        "--start-from-cik",
        type=str,
        default=None,
        help="10-digit CIK to start from (inclusive)",
    )
    p.add_argument(
        "--resume-from-last",
        action="store_true",
        help="Resume from the last completed CIK recorded in progress.txt (ignored if --start-from-cik is provided)",
    )
    return p.parse_args()


def main():
    signal.signal(signal.SIGINT, on_sigint)
    args = parse_args()
    info("Starting SEC-Crypto-Analysis script - Runtime: 00:00:00")
    try:
        start_from = args.start_from_cik
        if not start_from and args.resume_from_last:
            lf = get_last_processed_cik()
            if lf:
                # Start from the CIK AFTER the last completed one
                ciks = [c for c, _ in read_ciks()]
                try:
                    idx = ciks.index(lf)
                    if idx + 1 < len(ciks):
                        start_from = ciks[idx + 1]
                        info(
                            f"Resuming from after last completed CIK: {lf} → starting at {start_from}"
                        )
                    else:
                        info(
                            "Progress indicates all CIKs complete; starting from beginning"
                        )
                except ValueError:
                    info(
                        "Last completed CIK not found in input; starting from beginning"
                    )
            else:
                info("No progress file found or empty; starting from the beginning")
        run(start_from)
        # final flush on normal completion too
        flush_to_onedrive(BASE_FOLDER)
        total_runtime = format_runtime()
        info(f"Script completed successfully - Total runtime: {total_runtime}")
    finally:
        # Clean up global executor
        DOC_EXECUTOR.shutdown(wait=False)


if __name__ == "__main__":
    main()
