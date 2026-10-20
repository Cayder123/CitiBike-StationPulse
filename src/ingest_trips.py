"""Historical trip data ingestion CLI for CitiBike-StationPulse.

Downloads official Citi Bike trip data archives (CSV / ZIP) from the official S3 bucket,
ensuring immutable storage, atomic writes, SHA-256 verification, and run-isolated manifests.
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# Ensure project root is in sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.common.config import PipelineConfig
from src.common.hashing import calculate_sha256_file
from src.common.logging_utils import setup_logger
from src.common.manifest import create_manifest, write_manifest_atomic

PIPELINE_VERSION = "0.1.0"


def build_tripdata_url(base_url: str, dataset: str, year: int, month: int) -> Tuple[str, str]:
    """Construct the official S3 URL and filename for a given year and month.

    Args:
        base_url: Base repository URL (e.g., https://s3.amazonaws.com/tripdata)
        dataset: Dataset variant ('jc' for Jersey City, 'nyc' for NYC fleet)
        year: 4-digit year (e.g. 2024)
        month: 1-12 month

    Returns:
        Tuple of (full_url, filename)
    """
    base = base_url.rstrip("/")
    month_str = f"{month:02d}"
    if dataset.lower() == "jc":
        filename = f"JC-{year}{month_str}-citibike-tripdata.csv.zip"
    else:
        filename = f"{year}{month_str}-citibike-tripdata.zip"

    url = f"{base}/{filename}"
    return url, filename


def count_records_in_zip(zip_path: Path) -> Tuple[Optional[int], List[str]]:
    """Safely count data records across all CSV files in a ZIP archive without full disk extraction.

    Args:
        zip_path: Path to the downloaded .zip file.

    Returns:
        Tuple of (total_records, list_of_csv_files_found). If counting fails, returns (None, []).
    """
    total_records = 0
    csv_names = []
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            for item in zf.infolist():
                if item.filename.lower().endswith(".csv") and not item.filename.startswith("__MACOSX"):
                    csv_names.append(item.filename)
                    with zf.open(item, "r") as f:
                        # Stream lines in text mode (UTF-8 with error replace)
                        text_reader = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                        header = text_reader.readline()
                        if not header:
                            continue  # Empty CSV
                        rows = sum(1 for line in text_reader if line.strip())
                        total_records += rows
        return total_records, csv_names
    except Exception:
        return None, csv_names


def download_stream(
    url: str,
    target_path: Path,
    timeout: int = 30,
    max_download_mb: int = 500,
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    logger=None,
) -> int:
    """Download a file via streaming HTTP GET with retry, timeout, size limit, and atomic write.

    Writes to `<target_path>.tmp` first and atomically replaces upon success.

    Returns:
        Total bytes written.

    Raises:
        ValueError: If payload is empty or exceeds max_download_mb.
        HTTPError: If HTTP status is not 200.
    """
    if logger is None:
        logger = setup_logger("ingest_trips")

    max_bytes = max_download_mb * 1024 * 1024
    temp_path = target_path.with_suffix(f"{target_path.suffix}.tmp")
    target_path.parent.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    retries = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    logger.info(f"Initiating streaming download from: {url}")
    try:
        with session.get(url, stream=True, timeout=timeout) as response:
            if response.status_code != 200:
                raise requests.HTTPError(
                    f"HTTP GET returned status code {response.status_code} for URL: {url}"
                )

            content_length = response.headers.get("Content-Length")
            if content_length:
                content_len_int = int(content_length)
                logger.info(f"Remote Content-Length: {content_len_int:,} bytes ({content_len_int / (1024 * 1024):.2f} MB)")
                if content_len_int == 0:
                    raise ValueError(f"Remote file at {url} has Content-Length 0 (Empty payload).")
                if content_len_int > max_bytes:
                    raise ValueError(
                        f"Remote file ({content_len_int / (1024 * 1024):.2f} MB) exceeds maximum allowed limit of {max_download_mb} MB. "
                        f"Increase --max-download-mb if you wish to proceed."
                    )

            bytes_written = 0
            with open(temp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=128 * 1024):
                    if chunk:
                        f.write(chunk)
                        bytes_written += len(chunk)
                        if bytes_written > max_bytes:
                            raise ValueError(
                                f"Downloaded bytes ({bytes_written / (1024 * 1024):.2f} MB) exceeded maximum limit of {max_download_mb} MB during streaming."
                            )

            if bytes_written == 0:
                raise ValueError(f"Downloaded payload was empty (0 bytes) from {url}.")

            # Atomic rename
            temp_path.replace(target_path)
            logger.info(f"Download complete: {bytes_written:,} bytes saved atomically to {target_path}")
            return bytes_written

    except Exception:
        # Clean up partial temp file on failure
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass
        raise


def run_ingestion(
    dataset: str = "jc",
    year: Optional[int] = None,
    month: Optional[int] = None,
    source_url: Optional[str] = None,
    raw_root: Optional[Path | str] = None,
    timeout: int = 30,
    max_download_mb: int = 500,
    dry_run: bool = False,
    skip_existing: bool = False,
    count_records: bool = True,
    log_file: Optional[Path | str] = None,
) -> int:
    """Execute the end-to-end trip data ingestion workflow."""
    logger = setup_logger("ingest_trips", log_file=log_file)
    config = PipelineConfig.load()

    raw_base = Path(raw_root) if raw_root else config.raw_root

    # Determine URL and filename
    if source_url:
        target_url = source_url
        filename = target_url.split("/")[-1].split("?")[0]
        snapshot_label = filename
    else:
        y = year if year is not None else config.trips.default_year
        m = month if month is not None else config.trips.default_month
        target_url, filename = build_tripdata_url(config.trips.base_url, dataset, y, m)
        snapshot_label = f"{y}-{m:02d}"

    logger.info("==================================================")
    logger.info("       CitiBike Trip Data Ingestion Pipeline      ")
    logger.info("==================================================")
    logger.info(f"Source URL       : {target_url}")
    logger.info(f"Snapshot Period  : {snapshot_label}")
    logger.info(f"Dataset Variant  : {dataset}")
    logger.info(f"Max Download MB  : {max_download_mb} MB")
    logger.info(f"Dry-run Mode     : {dry_run}")

    # Dry-run validation
    if dry_run:
        logger.info("[DRY RUN] Validating remote URL headers without downloading...")
        try:
            head_resp = requests.head(target_url, timeout=timeout, allow_redirects=True)
            logger.info(f"[DRY RUN] HTTP Status: {head_resp.status_code}")
            content_length = head_resp.headers.get("Content-Length")
            if content_length:
                size_mb = int(content_length) / (1024 * 1024)
                logger.info(f"[DRY RUN] Content-Length: {int(content_length):,} bytes ({size_mb:.2f} MB)")
            if head_resp.status_code == 200:
                logger.info("[DRY RUN] Verification Succeeded. File is reachable and ready for ingestion.")
                return 0
            else:
                logger.error(f"[DRY RUN] Target returned non-200 code: {head_resp.status_code}")
                return 1
        except Exception as e:
            logger.error(f"[DRY RUN] Connection error during URL check: {e}")
            return 1

    # Prepare immutable partition path:
    # data/raw/citibike_trips/ingest_date=YYYY-MM-DD/run_<UTC_TIMESTAMP>/
    utc_now = datetime.now(timezone.utc)
    date_partition = utc_now.strftime("%Y-%m-%d")
    run_id = f"run_{utc_now.strftime('%Y%m%dT%H%M%SZ')}"
    destination_dir = raw_base / "citibike_trips" / f"ingest_date={date_partition}" / run_id
    target_file_path = destination_dir / filename
    manifest_path = destination_dir / "manifest.json"

    # Execute streaming download
    try:
        bytes_downloaded = download_stream(
            url=target_url,
            target_path=target_file_path,
            timeout=timeout,
            max_download_mb=max_download_mb,
            max_retries=config.max_retries,
            backoff_factor=config.retry_backoff_factor,
            logger=logger,
        )
    except Exception as e:
        logger.error(f"Download failed: {e}")
        # Record failed manifest if destination directory exists
        try:
            fail_manifest = create_manifest(
                dataset="citibike_trips",
                source_url=target_url,
                source_snapshot=snapshot_label,
                run_id=run_id,
                filename=filename,
                bytes_count=0,
                sha256="",
                destination=str(target_file_path),
                status="failed",
                notes=f"Error: {e}",
            )
            write_manifest_atomic(fail_manifest, manifest_path)
        except Exception:
            pass
        return 1

    # Calculate SHA-256
    logger.info("Computing SHA-256 checksum on downloaded artifact...")
    checksum = calculate_sha256_file(target_file_path)
    logger.info(f"SHA-256 Checksum : {checksum}")

    # Inspect records if archive is a ZIP file and count is enabled
    total_records = None
    notes = None
    if count_records and filename.lower().endswith(".zip"):
        logger.info("Scanning ZIP archive for internal CSV record counts (streaming)...")
        rec_count, csvs = count_records_in_zip(target_file_path)
        total_records = rec_count
        if total_records is not None:
            logger.info(f"Verified Records : {total_records:,} across {len(csvs)} CSV file(s) [{', '.join(csvs)}]")
            notes = f"Internal CSV files: {len(csvs)} files ({', '.join(csvs)})"
        else:
            logger.warning("Record count could not be safely derived from ZIP archive.")

    # Write Manifest
    logger.info(f"Generating manifest at {manifest_path}...")
    manifest = create_manifest(
        dataset="citibike_trips",
        source_url=target_url,
        source_snapshot=snapshot_label,
        run_id=run_id,
        filename=filename,
        bytes_count=bytes_downloaded,
        sha256=checksum,
        destination=str(target_file_path.relative_to(project_root) if target_file_path.is_relative_to(project_root) else target_file_path),
        status="success",
        record_count=total_records,
        pipeline_version=PIPELINE_VERSION,
        notes=notes,
    )
    write_manifest_atomic(manifest, manifest_path)
    logger.info("Manifest created and validated successfully.")
    logger.info("==================================================")
    logger.info(f"Ingestion Succeeded! Artifact located at: {target_file_path}")
    logger.info("==================================================")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest Citi Bike monthly trip history data with provenance and integrity checks."
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Target year (e.g., 2024). Defaults to config value.",
    )
    parser.add_argument(
        "--month",
        type=int,
        default=None,
        help="Target month (1-12). Defaults to config value.",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default="jc",
        choices=["jc", "nyc"],
        help="Dataset variant: 'jc' (Jersey City, ~2MB) or 'nyc' (NYC full fleet, ~350MB). Default is 'jc'.",
    )
    parser.add_argument(
        "--source-url",
        type=str,
        default=None,
        help="Explicit source URL to override default S3 bucket URL generation.",
    )
    parser.add_argument(
        "--raw-root",
        type=str,
        default=None,
        help="Directory root for raw data storage (default: data/raw).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Download timeout in seconds (default: 30).",
    )
    parser.add_argument(
        "--max-download-mb",
        type=int,
        default=500,
        help="Maximum allowed download size in MB (default: 500).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform HTTP header check only without downloading data files.",
    )
    parser.add_argument(
        "--no-record-count",
        action="store_true",
        help="Skip uncompressing and counting records inside ZIP archive.",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Optional file path to persist execution logs.",
    )

    args = parser.parse_args()

    # Validate month if supplied
    if args.month is not None and not (1 <= args.month <= 12):
        print(f"Error: Month must be between 1 and 12, got {args.month}", file=sys.stderr)
        sys.exit(1)

    code = run_ingestion(
        dataset=args.dataset,
        year=args.year,
        month=args.month,
        source_url=args.source_url,
        raw_root=args.raw_root,
        timeout=args.timeout,
        max_download_mb=args.max_download_mb,
        dry_run=args.dry_run,
        count_records=not args.no_record_count,
        log_file=args.log_file,
    )
    sys.exit(code)


if __name__ == "__main__":
    main()
