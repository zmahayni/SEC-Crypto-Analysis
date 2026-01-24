#!/usr/bin/env python3
"""
Download full 10-K filings for all partial 10-K files already saved.
Supports pause/resume via progress tracking.
"""

import pathlib
import time
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import threading
import signal
import sys

# =============================================================================
# CONFIG
# =============================================================================

HOME = pathlib.Path.home()
ONEDRIVE_FOLDER = (
    HOME / "Library/CloudStorage/OneDrive-SharedLibraries-UniversityofTulsa/NSF-BSF Precautions - crypto10k"
)
FULL_10K_FOLDER = ONEDRIVE_FOLDER / "full_10ks"
PROGRESS_FILE = HOME / "edgar_tmp" / "full_10k_progress.txt"

# SEC endpoint for full filing text
TXT_PATH = "https://www.sec.gov/Archives/edgar/data/{cik_nolead}/{acc_dash}.txt"

# Rate limiting
MAX_RPS = 9.5  # Stay under SEC's 10 req/sec limit
MAX_RETRIES = 3
BACKOFF = [15, 30, 60]  # seconds

# User agent (required by SEC)
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36 "
    "Uni Tulsa research - zam3395@utulsa.edu"
)

# =============================================================================
# STATE & HELPERS
# =============================================================================

_TLS = threading.local()
STOP_EVENT = threading.Event()
SCRIPT_START_TIME = time.time()


def _get_session():
    """Get thread-local session with connection pooling."""
    s = getattr(_TLS, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": UA})

        retry_strategy = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=32,
            pool_maxsize=64,
        )
        s.mount("http://", adapter)
        s.mount("https://", adapter)

        _TLS.session = s
    return s


class RateLimiter:
    """Thread-safe rate limiter."""
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


def format_runtime():
    """Format runtime as HH:MM:SS."""
    runtime_seconds = int(time.time() - SCRIPT_START_TIME)
    hours, remainder = divmod(runtime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def info(msg: str):
    """Print info message with timestamp."""
    print(f"[{format_runtime()}] {msg}", flush=True)


def error(msg: str):
    """Print error message."""
    print(f"[{format_runtime()}] ERROR: {msg}", file=sys.stderr, flush=True)


def get_with_backoff(url: str, label: str):
    """Download with retry logic and backoff."""
    for attempt in range(MAX_RETRIES):
        if STOP_EVENT.is_set():
            return None

        RATE_LIMITER.acquire()

        try:
            r = _get_session().get(url, timeout=(15, 120))
        except requests.RequestException as e:
            wait = BACKOFF[min(attempt, len(BACKOFF) - 1)]
            error(f"Network error on {label}: {e.__class__.__name__}; sleep {wait}s (try {attempt + 1})")
            time.sleep(wait)
            continue

        if r.status_code == 200:
            return r

        if r.status_code != 429:
            error(f"HTTP {r.status_code} on {label}")
            return None

        # Handle 429
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

    error(f"Gave up on {label} after {MAX_RETRIES} retries")
    return None


def parse_filename(filename: str):
    """
    Parse filename format: {CIK}_{FORM}_{DATE}_{ACCESSION}.txt
    Returns: (cik, form, date, accession) or None
    """
    try:
        # Remove extension
        name = filename.replace('.txt', '').replace('.html', '').replace('.htm', '')
        parts = name.split('_')

        if len(parts) < 4:
            return None

        cik = parts[0]
        form = parts[1]
        date = parts[2]
        accession = '_'.join(parts[3:])  # Accession may have underscores

        return (cik, form, date, accession)
    except Exception:
        return None


def load_progress():
    """Load set of already downloaded filings."""
    try:
        if PROGRESS_FILE.exists():
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return set(line.strip() for line in f if line.strip())
    except Exception as e:
        error(f"Failed to load progress file: {e}")
    return set()


def save_progress(filename: str):
    """Record a successfully downloaded filing."""
    try:
        PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(PROGRESS_FILE, 'a', encoding='utf-8') as f:
            f.write(f"{filename}\n")
    except Exception as e:
        error(f"Failed to save progress: {e}")


def on_sigint(sig, frame):
    """Handle Ctrl-C gracefully."""
    info("Ctrl-C detected - stopping gracefully...")
    STOP_EVENT.set()
    sys.exit(130)


# =============================================================================
# MAIN LOGIC
# =============================================================================

def download_full_10k(cik: str, accession: str, output_path: pathlib.Path):
    """
    Download full 10-K filing.

    Args:
        cik: 10-digit zero-padded CIK
        accession: Accession number with dashes (e.g., 0001437749-25-010478)
        output_path: Where to save the file
    """
    # Convert CIK to integer string (no leading zeros) for URL
    cik_int = str(int(cik))

    # Build URL
    url = TXT_PATH.format(cik_nolead=cik_int, acc_dash=accession)

    # Download
    resp = get_with_backoff(url, f"{cik} {accession}")
    if not resp:
        return False

    # Save
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(resp.text, encoding='utf-8', errors='ignore')
        return True
    except Exception as e:
        error(f"Failed to save {output_path.name}: {e}")
        return False


def find_all_10k_files():
    """
    Walk through OneDrive folder and find all 10-K files.
    Returns list of (cik, filename, filepath) tuples.
    """
    files_to_download = []

    if not ONEDRIVE_FOLDER.exists():
        error(f"OneDrive folder not found: {ONEDRIVE_FOLDER}")
        return files_to_download

    info(f"Scanning for 10-K files in: {ONEDRIVE_FOLDER}")

    for cik_folder in sorted(ONEDRIVE_FOLDER.iterdir()):
        if not cik_folder.is_dir() or cik_folder.name == "full_10ks":
            continue

        cik = cik_folder.name

        for filepath in cik_folder.iterdir():
            if filepath.is_dir():
                continue

            filename = filepath.name

            # Skip metadata files
            if filename in ["SIC.txt", "COMPLETE", ".STAGING"]:
                continue

            # Only process 10-K .txt files (these have proper accession numbers)
            # .htm files have document names, not accession numbers
            if "_10-K_" not in filename:
                continue

            if not filename.endswith('.txt'):
                continue

            files_to_download.append((cik, filename, filepath))

    return files_to_download


def main():
    """Main execution."""
    signal.signal(signal.SIGINT, on_sigint)

    info("Starting full 10-K download script")
    info(f"Output folder: {FULL_10K_FOLDER}")

    # Load progress
    completed = load_progress()
    info(f"Found {len(completed)} already completed in progress file")

    # Find all 10-K files
    all_files = find_all_10k_files()
    info(f"Found {len(all_files)} total 10-K files")

    # Filter out already completed
    to_download = [(cik, fn, fp) for (cik, fn, fp) in all_files if fn not in completed]
    info(f"{len(to_download)} files to download (skipping {len(all_files) - len(to_download)} already done)")

    if len(to_download) == 0:
        info("Nothing to download - all files already completed!")
        return

    # Download each file
    downloaded = 0
    failed = 0

    for idx, (cik, filename, original_path) in enumerate(to_download, 1):
        if STOP_EVENT.is_set():
            break

        # Parse filename to get accession number
        parsed = parse_filename(filename)
        if not parsed:
            error(f"Could not parse filename: {filename}")
            failed += 1
            continue

        cik_parsed, form, date, accession = parsed

        # Output path in new folder structure
        output_path = FULL_10K_FOLDER / cik / filename

        # Skip if already exists (extra safety)
        if output_path.exists():
            info(f"[{idx}/{len(to_download)}] Skip (exists): {filename}")
            save_progress(filename)
            continue

        # Download
        info(f"[{idx}/{len(to_download)}] Downloading: {filename}")
        success = download_full_10k(cik, accession, output_path)

        if success:
            downloaded += 1
            save_progress(filename)
            info(f"  ✓ Saved: {output_path.name} ({output_path.stat().st_size // 1024} KB)")
        else:
            failed += 1

    # Final summary
    total_runtime = format_runtime()
    info("=" * 60)
    info(f"Download complete!")
    info(f"  Successfully downloaded: {downloaded}")
    info(f"  Failed: {failed}")
    info(f"  Total runtime: {total_runtime}")
    info(f"  Output location: {FULL_10K_FOLDER}")


if __name__ == "__main__":
    main()
