"""Historical trip data ingestion CLI for CitiBike-StationPulse.

Downloads official Citi Bike trip data archives (CSV / ZIP) from the official S3 bucket,
ensuring immutable storage, atomic writes, SHA-256 verification, and run-isolated manifests.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
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
from src.common.logging_utils import setup_logger, sanitize_url
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

    Uses csv.reader to correctly handle multiline quoted fields and skip header rows for each member.

    Args:
        zip_path: Path to the downloaded .zip file.

    Returns:
        Tuple of (total_records, list_of_csv_files_found). If counting fails, returns (None, []).
    """
    total_records = 0
    csv_names = []
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            # Check archive integrity first
            bad_crc = zf.testzip()
            if bad_crc:
                raise ValueError(f"Corrupt ZIP archive member: {bad_crc}")

            for item in zf.infolist():
                norm_name = item.filename.replace("\\", "/")
                if norm_name.lower().endswith(".csv") and not norm_name.startswith("__MACOSX"):
                    csv_names.append(norm_name)
                    with zf.open(item, "r") as f:
                        text_reader = io.TextIOWrapper(f, encoding="utf-8", errors="replace", newline="")
                        reader = csv.reader(text_reader)
                        header = next(reader, None)
                        if header is None:
                            continue  # Empty CSV
                        file_rows = sum(1 for row in reader if row)
                        total_records += file_rows
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
        ValueError: If payload is empty, exceeds max_download_mb, or fails length checks.
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

    sanitized_url = sanitize_url(url)
    logger.info(f"Initiating streaming download from: {sanitized_url}")
    try:
        with session.get(url, stream=True, timeout=timeout) as response:
            if response.status_code != 200:
                raise requests.HTTPError(
                    f"HTTP GET returned status code {response.status_code} for URL: {sanitized_url}"
                )

            content_length = response.headers.get("Content-Length")
            content_len_int = None
            if content_length:
                content_len_int = int(content_length)
                logger.info(
                    f"Remote Content-Length: {content_len_int:,} bytes ({content_len_int / (1024 * 1024):.2f} MB)"
                )
                if content_len_int == 0:
                    raise ValueError(f"Remote file at {sanitized_url} has Content-Length 0 (Empty payload).")
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
                raise ValueError(f"Downloaded payload was empty (0 bytes) from {sanitized_url}.")

            if content_len_int is not None and bytes_written != content_len_int:
                raise ValueError(
                    f"Downloaded bytes ({bytes_written:,}) did not match remote Content-Length ({content_len_int:,})."
                )

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


def create_unique_run_dir(base_dir: Path, utc_now: datetime) -> Tuple[str, Path]:
    """Generate collision-free run ID and create exclusive partition directory."""
    date_partition = utc_now.strftime("%Y-%m-%d")
    base_timestamp = utc_now.strftime("%Y%m%dT%H%M%S")
    microsec = utc_now.microsecond

    parent_partition = base_dir / "citibike_trips" / f"ingest_date={date_partition}"
    parent_partition.mkdir(parents=True, exist_ok=True)

    # Attempt microsecond timestamp first
    counter = 0
    while True:
        suffix = f"_{counter:02d}" if counter > 0 else ""
        run_id = f"run_{base_timestamp}_{microsec:06d}Z{suffix}"
        target_dir = parent_partition / run_id
        try:
            target_dir.mkdir(parents=True, exist_ok=False)
            return run_id, target_dir
        except FileExistsError:
            counter += 1


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
    logger.info(f"Source URL       : {sanitize_url(target_url)}")
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

    # Skip only a previously successful artifact whose bytes still match its manifest.
    if skip_existing:
        date_partition = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        search_dir = raw_base / "citibike_trips" / f"ingest_date={date_partition}"
        if search_dir.is_dir():
            for existing_run in search_dir.glob("run_*/"):
                existing_file = existing_run / filename
                existing_manifest = existing_run / "manifest.json"
                if existing_file.is_file() and existing_manifest.is_file():
                    try:
                        manifest_data = json.loads(existing_manifest.read_text(encoding="utf-8"))
                        stored_hash = manifest_data.get("sha256")
                        if (
                            manifest_data.get("status") == "success"
                            and manifest_data.get("dataset") == "citibike_trips"
                            and manifest_data.get("source_snapshot") == snapshot_label
                            and manifest_data.get("filename") == filename
                            and manifest_data.get("bytes") == existing_file.stat().st_size
                            and isinstance(stored_hash, str)
                            and calculate_sha256_file(existing_file) == stored_hash
                        ):
                            logger.info(
                                "Skip policy active: found a valid stored artifact for "
                                f"{snapshot_label} at {existing_file}. Skipping download."
                            )
                            return 0
                    except (OSError, ValueError, json.JSONDecodeError) as exc:
                        logger.warning(f"Could not validate existing artifact at {existing_run}: {exc}")

    # Allocate exclusive partition directory
    utc_now = datetime.now(timezone.utc)
    run_id, destination_dir = create_unique_run_dir(raw_base, utc_now)
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
        # Clean up empty run directory so incomplete runs do not pollute storage
        if destination_dir.exists():
            try:
                for f in destination_dir.iterdir():
                    f.unlink()
                destination_dir.rmdir()
            except Exception:
                pass
        return 1

    # Verify ZIP integrity if file is a zip archive
    if filename.lower().endswith(".zip"):
        try:
            with zipfile.ZipFile(target_file_path, "r") as zf:
                bad_member = zf.testzip()
                if bad_member:
                    raise ValueError(f"Corrupt ZIP member CRC: {bad_member}")
        except Exception as ze:
            logger.error(f"ZIP archive integrity check failed: {ze}")
            if target_file_path.exists():
                target_file_path.unlink()
            if destination_dir.exists():
                try:
                    destination_dir.rmdir()
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
        logger.info("Scanning ZIP archive for internal CSV record counts (csv.reader)...")
        rec_count, csvs = count_records_in_zip(target_file_path)
        total_records = rec_count
        if total_records is not None:
            logger.info(f"Verified Records : {total_records:,} across {len(csvs)} CSV file(s) [{', '.join(csvs)}]")
            notes = f"Internal CSV files: {len(csvs)} files ({', '.join(csvs)})"
        else:
            logger.warning("Record count could not be safely derived from ZIP archive.")
            notes = "Record count: unknown / not derived"

    # Write Manifest
    logger.info(f"Generating manifest at {manifest_path}...")
    try:
        dest_str = (
            str(target_file_path.relative_to(project_root))
            if target_file_path.is_relative_to(project_root)
            else str(target_file_path)
        )
    except Exception:
        dest_str = str(target_file_path)

    manifest = create_manifest(
        dataset="citibike_trips",
        source_url=target_url,
        source_snapshot=snapshot_label,
        run_id=run_id,
        filename=filename,
        bytes_count=bytes_downloaded,
        sha256=checksum,
        destination=dest_str,
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
        "--skip-existing",
        action="store_true",
        help="Skip download if a prior successful artifact for the same monthly snapshot validates against its manifest.",
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

    # Input validations
    if args.timeout <= 0:
        print(f"Error: --timeout must be positive, got {args.timeout}", file=sys.stderr)
        sys.exit(1)
    if args.max_download_mb <= 0:
        print(f"Error: --max-download-mb must be positive, got {args.max_download_mb}", file=sys.stderr)
        sys.exit(1)
    if args.month is not None and not (1 <= args.month <= 12):
        print(f"Error: Month must be between 1 and 12, got {args.month}", file=sys.stderr)
        sys.exit(1)
    if args.year is not None and (args.year < 2013 or args.year > 2035):
        print(f"Error: Year must be between 2013 and 2035, got {args.year}", file=sys.stderr)
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
        skip_existing=args.skip_existing,
        count_records=not args.no_record_count,
        log_file=args.log_file,
    )
    sys.exit(code)


if __name__ == "__main__":
    main()
