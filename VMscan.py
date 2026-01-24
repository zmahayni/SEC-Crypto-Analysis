# =============================================================================
# SEC-Crypto-Analysis - VM Version (local storage + Mac pull)
# =============================================================================
# Optimized for low-resource VMs (2GB RAM, 10GB storage):
# 1. Saves files locally - Mac pulls via rsync when online
# 2. Reduced concurrency for limited RAM
# 3. Pauses scanning when storage threshold exceeded
# 4. Resumes automatically when storage freed (after Mac pulls)
# =============================================================================

import argparse
import re
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

INPUT_XLSX = pathlib.Path("Publicly_Trade_Companies_SEC.xlsx")

# Local staging directory - Mac will pull from here
TMP_ROOT = HOME / "edgar_tmp"
STAGE_DIR = TMP_ROOT / "stage"
PROGRESS_FILE = TMP_ROOT / "progress.txt"

# Storage limits (in MB) - pause scanning when exceeded
MAX_STORAGE_MB = 7000  # 7GB - leave headroom on 10GB disk
RESUME_STORAGE_MB = 5000  # Resume when below 5GB
STORAGE_CHECK_INTERVAL = 60  # Check storage every 60 seconds when paused

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

# Throughput - REDUCED for 2GB RAM VM
MAX_RPS = 9.5  # global cap - respecting SEC limit
CIK_CONCURRENCY = 3  # parallel CIKs (reduced from 10)
DOC_CONCURRENCY = 8  # parallel exhibits per filing (reduced from 20)
MAX_SAVE_MB_PER_FILE = 20  # don't save > 20MB files

# Include PDFs? (disabled for speed)
INCLUDE_PDF_EXHIBITS = False

# HTTP headers
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36 "
    "Uni Tulsa research - zam3395@utulsa.edu"
)

# Endpoints
SUB_JSON = "https://data.sec.gov/submissions/CIK{cik}.json"
IDX_JSON = "https://www.sec.gov/Archives/edgar/data/{cik_nolead}/{folder}/index.json"
DOC_PATH = "https://www.sec.gov/Archives/edgar/data/{cik_nolead}/{folder}/{name}"
TXT_PATH = "https://www.sec.gov/Archives/edgar/data/{cik_nolead}/{acc_dash}.txt"

# Backoff for 429s/network hiccups
MAX_RETRIES = 3
BACKOFF = [15, 30, 60]

# =============================================================================
# Internal state / helpers
# =============================================================================

_TLS = threading.local()
STOP_EVENT = threading.Event()
DOC_EXECUTOR = ThreadPoolExecutor(max_workers=DOC_CONCURRENCY)
SCRIPT_START_TIME = time.time()

# Storage tracking
_STORAGE_PAUSED = False
_STORAGE_LOCK = threading.Lock()


def _get_session():
    s = getattr(_TLS, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": UA, "Accept": "application/json"})

        retry_strategy = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=32,  # Reduced for low RAM
            pool_maxsize=64,  # Reduced for low RAM
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
    global _REQ_COUNT
    now = time.perf_counter()
    with REQ_LOCK:
        _REQ_TIMES.append(now)
        cutoff = now - window_sec
        while _REQ_TIMES and _REQ_TIMES[0] < cutoff:
            _REQ_TIMES.popleft()
        _REQ_COUNT += 1


def format_runtime():
    runtime_seconds = int(time.time() - SCRIPT_START_TIME)
    hours, remainder = divmod(runtime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def info(msg: str):
    print(f"[{format_runtime()}] {msg}", flush=True)


def error(msg: str):
    print(f"[{format_runtime()}] ERROR: {msg}", file=sys.stderr, flush=True)


def ensure_dirs():
    STAGE_DIR.mkdir(parents=True, exist_ok=True)
    TMP_ROOT.mkdir(parents=True, exist_ok=True)


def get_staging_size_mb() -> float:
    """Calculate total size of staging directory in MB."""
    total = 0
    try:
        for f in STAGE_DIR.rglob("*"):
            if f.is_file():
                total += f.stat().st_size
    except Exception:
        pass
    return total / (1024 * 1024)


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

        retry_after = r.headers.get("Retry-After")
        if retry_after:
            try:
                wait = float(retry_after)
            except ValueError:
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
        if cl is None:
            return True
        mb = int(cl) / (1024 * 1024)
        return mb <= MAX_SAVE_MB_PER_FILE
    except Exception:
        return True


def pdf_to_text_bytes(content: bytes) -> str:
    try:
        try:
            import fitz

            with fitz.open(stream=content, filetype="pdf") as doc:
                max_pages = min(10, doc.page_count)
                text = ""
                for i in range(max_pages):
                    text += doc[i].get_text()
                return text
        except ImportError:
            from pdfminer.high_level import extract_text
            import io

            with io.BytesIO(content) as pdf_stream:
                text = extract_text(pdf_stream) or ""
                return text
    except Exception as e:
        error(f"PDF extraction failed: {e}")
        return ""


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
        for chunk in r.iter_content(chunk_size=256_000, decode_unicode=True):
            if not chunk:
                continue
            if not found_keyword and KEYWORDS.search(chunk):
                found_keyword = True

            text_buf.append(chunk)
            acc_len += len(chunk)
            if acc_len > limit:
                s = "".join(text_buf)
                s = s[-limit:]
                text_buf = [s]
                acc_len = len(s)

            if found_keyword and acc_len > 25_000:
                return True, "".join(text_buf)

        return found_keyword, "".join(text_buf)
    finally:
        try:
            r.close()
        except Exception:
            pass


def fetch_bytes(url: str) -> bytes:
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
    primary = (primary or "").lower()
    m = [d for d in manifest if "name" in d]
    out = []
    for d in m:
        nm = d["name"].lower()
        if nm == primary and (
            nm.endswith((".htm", ".html", ".txt"))
            or (INCLUDE_PDF_EXHIBITS and nm.endswith(".pdf"))
        ):
            out.append(d)
            break
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
    if INCLUDE_PDF_EXHIBITS and name_doc.lower().endswith(".pdf"):
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
    hit, content = stream_scan_for_keywords(url)
    if not hit:
        return 0
    head = get_with_backoff(url, f"HEAD {name_doc}")
    if head and head.status_code == 200 and not size_under_limit(head):
        info(f"skip saving large file (> {MAX_SAVE_MB_PER_FILE} MB): {name_doc}")
        return 1
    out_name = f"{cik10}_{form}_{filed}_{name_doc}"
    save_text_raw(content, cik_stage / out_name)
    info(f"saved {out_name}")
    return 1


def get_completed_count():
    """Count completed CIK folders waiting for transfer."""
    count = 0
    try:
        for cik_dir in STAGE_DIR.iterdir():
            if cik_dir.is_dir() and (cik_dir / "COMPLETE").exists():
                count += 1
    except Exception:
        pass
    return count


def wait_for_storage():
    """Block until storage drops below resume threshold."""
    global _STORAGE_PAUSED

    with _STORAGE_LOCK:
        if _STORAGE_PAUSED:
            return  # Already waiting in another thread

    size_mb = get_staging_size_mb()
    if size_mb < MAX_STORAGE_MB:
        return  # Storage is fine

    with _STORAGE_LOCK:
        _STORAGE_PAUSED = True

    completed = get_completed_count()
    info(f"STORAGE LIMIT: {size_mb:.0f}MB used (max {MAX_STORAGE_MB}MB)")
    info(f"Pausing scan - {completed} completed CIK folders waiting for Mac to pull")
    info(f"Run pull script on Mac, then scanning will resume automatically")

    while not STOP_EVENT.is_set():
        time.sleep(STORAGE_CHECK_INTERVAL)
        size_mb = get_staging_size_mb()
        completed = get_completed_count()

        if size_mb < RESUME_STORAGE_MB:
            info(f"Storage freed: {size_mb:.0f}MB - resuming scan")
            with _STORAGE_LOCK:
                _STORAGE_PAUSED = False
            return

        # Periodic status update
        info(f"Still waiting: {size_mb:.0f}MB used, {completed} folders pending transfer")


def on_sigint(sig, frame):
    total_runtime = format_runtime()
    completed = get_completed_count()
    size_mb = get_staging_size_mb()
    info(f"Ctrl-C detected after {total_runtime}")
    info(f"Local storage: {size_mb:.0f}MB, {completed} completed CIK folders")
    info(f"Run pull script on Mac to transfer files before they're lost!")
    STOP_EVENT.set()
    sys.exit(130)


# =============================================================================
# Core
# =============================================================================


def process_filing(cik10, form, filed, acc_dash, cik_int, cik_stage, primary_name):
    folder = acc_dash.replace("-", "")

    txt_url = TXT_PATH.format(cik_nolead=cik_int, acc_dash=acc_dash)
    hit_txt, content_txt = stream_scan_for_keywords(txt_url)
    if hit_txt:
        head = get_with_backoff(txt_url, f"HEAD {acc_dash}.txt")
        if not head or head.status_code != 200 or size_under_limit(head):
            out = cik_stage / f"{cik10}_{form}_{filed}_{acc_dash}.txt"
            save_text_raw(content_txt, out)
            info(f"saved {out.name}")
        return

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

    hits = 0
    if DOC_CONCURRENCY <= 1:
        for d in docs:
            if STOP_EVENT.is_set():
                break
            hits += process_doc(cik10, form, filed, cik_int, folder, d, cik_stage)
    else:
        hits += process_doc(cik10, form, filed, cik_int, folder, docs[0], cik_stage)
        if hits == 0 and len(docs) > 1:
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
    global _COMPLETED_SINCE_FLUSH

    if STOP_EVENT.is_set():
        return

    cik_stage = STAGE_DIR / cik10
    cik_stage.mkdir(parents=True, exist_ok=True)
    (cik_stage / ".STAGING").write_text("in-progress", encoding="utf-8")

    if total_ciks > 0:
        info(f"{cik_index}/{total_ciks}    CIK {cik10} – {name}")
    else:
        info(f"Start CIK {cik10} – {name}")

    r_meta = get_with_backoff(SUB_JSON.format(cik=cik10), "submissions JSON")
    if not r_meta or r_meta.status_code != 200:
        error("could not fetch submissions JSON")
        return
    meta = r_meta.json()

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

        # Check storage and pause if needed
        wait_for_storage()


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
            actual_idx = total_ciks - len(ciks) + i + 1
            process_cik(cik10, name, actual_idx, total_ciks)
    else:
        with ThreadPoolExecutor(max_workers=CIK_CONCURRENCY) as ex:
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
# CLI
# =============================================================================


def parse_args():
    p = argparse.ArgumentParser(
        description="EDGAR crypto scan - VM version with rclone support"
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
        help="Resume from the last completed CIK recorded in progress.txt",
    )
    p.add_argument(
        "--status",
        action="store_true",
        help="Show storage status and exit",
    )
    return p.parse_args()


def show_status():
    """Show current storage and progress status."""
    ensure_dirs()
    size_mb = get_staging_size_mb()
    completed = get_completed_count()

    # Count in-progress
    in_progress = 0
    try:
        for cik_dir in STAGE_DIR.iterdir():
            if cik_dir.is_dir() and (cik_dir / ".STAGING").exists():
                in_progress += 1
    except Exception:
        pass

    # Get progress info
    last_cik = get_last_processed_cik()

    print(f"Storage used: {size_mb:.0f}MB / {MAX_STORAGE_MB}MB limit")
    print(f"Completed CIK folders (awaiting transfer): {completed}")
    print(f"In-progress CIK folders: {in_progress}")
    print(f"Last completed CIK: {last_cik or 'None'}")
    print(f"Staging directory: {STAGE_DIR}")

    if size_mb > RESUME_STORAGE_MB:
        print(f"\nWARNING: Storage above resume threshold ({RESUME_STORAGE_MB}MB)")
        print("Run pull script on Mac to free space")


def main():
    signal.signal(signal.SIGINT, on_sigint)
    args = parse_args()

    if args.status:
        show_status()
        sys.exit(0)

    info("Starting SEC-Crypto-Analysis (VM version) - Runtime: 00:00:00")
    info(f"Storage limit: {MAX_STORAGE_MB}MB, resume at: {RESUME_STORAGE_MB}MB")
    info(f"Staging directory: {STAGE_DIR}")

    try:
        start_from = args.start_from_cik
        if not start_from and args.resume_from_last:
            lf = get_last_processed_cik()
            if lf:
                ciks = [c for c, _ in read_ciks()]
                try:
                    idx = ciks.index(lf)
                    if idx + 1 < len(ciks):
                        start_from = ciks[idx + 1]
                        info(f"Resuming after {lf} → starting at {start_from}")
                    else:
                        info("All CIKs complete; starting from beginning")
                except ValueError:
                    info("Last CIK not found in input; starting from beginning")
            else:
                info("No progress file; starting from the beginning")

        run(start_from)

        total_runtime = format_runtime()
        completed = get_completed_count()
        size_mb = get_staging_size_mb()
        info(f"Script completed - Total runtime: {total_runtime}")
        info(f"Final status: {size_mb:.0f}MB used, {completed} folders awaiting transfer")
    finally:
        DOC_EXECUTOR.shutdown(wait=False)


if __name__ == "__main__":
    main()
