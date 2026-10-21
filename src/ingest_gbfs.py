"""GBFS Live Station Feeds Ingestion CLI for CitiBike-StationPulse.

Auto-discovers feed endpoints from root gbfs.json, fetches station_information
and station_status snapshots, and records cryptographic manifests with UTC lineage.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
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
MAX_FEED_BYTES = 50 * 1024 * 1024  # 50 MB safety ceiling for JSON responses


def discover_feeds(
    root_url: str,
    preferred_lang: str = "en",
    timeout: int = 20,
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    logger=None,
) -> Tuple[str, Dict[str, str]]:
    """Fetch root GBFS feed and resolve feed URLs for the preferred language.

    Args:
        root_url: URL to gbfs.json discovery document.
        preferred_lang: Language code preference (e.g. 'en').

    Returns:
        Tuple of (gbfs_version, dictionary mapping feed_name to feed_url)

    Raises:
        ValueError: If feed structure cannot be parsed or required feeds are missing.
    """
    if logger is None:
        logger = setup_logger("ingest_gbfs")

    session = requests.Session()
    retries = Retry(total=max_retries, backoff_factor=backoff_factor, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", adapter := HTTPAdapter(max_retries=retries))
    session.mount("http://", adapter)

    sanitized_root = sanitize_url(root_url)
    logger.info(f"Discovering GBFS feeds from root endpoint: {sanitized_root}")
    resp = session.get(root_url, timeout=timeout)
    if resp.status_code != 200:
        raise requests.HTTPError(f"Root GBFS endpoint returned HTTP {resp.status_code}")

    root_data = resp.json()
    version = root_data.get("version", "unknown")
    logger.info(f"Advertised GBFS Version: {version}")

    data_section = root_data.get("data", {})
    feeds_list = []
    if preferred_lang in data_section and "feeds" in data_section[preferred_lang]:
        feeds_list = data_section[preferred_lang]["feeds"]
    elif "feeds" in data_section:
        feeds_list = data_section["feeds"]
    else:
        # Fallback to the first available language dictionary
        for lang_code, lang_val in data_section.items():
            if isinstance(lang_val, dict) and "feeds" in lang_val:
                logger.info(f"Using fallback language: {lang_code}")
                feeds_list = lang_val["feeds"]
                break

    feed_map: Dict[str, str] = {}
    for feed in feeds_list:
        name = feed.get("name")
        url = feed.get("url")
        if name and url:
            feed_map[name] = url

    return str(version), feed_map


def fetch_and_save_json(
    url: str,
    target_path: Path,
    timeout: int = 20,
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    logger=None,
) -> Tuple[int, Optional[int]]:
    """Fetch raw JSON payload from URL and save atomically to disk.

    Returns:
        Tuple of (bytes_written, station_count_if_applicable).
    """
    if logger is None:
        logger = setup_logger("ingest_gbfs")

    session = requests.Session()
    retries = Retry(total=max_retries, backoff_factor=backoff_factor, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", adapter := HTTPAdapter(max_retries=retries))
    session.mount("http://", adapter)

    temp_path = target_path.with_suffix(f"{target_path.suffix}.tmp")
    target_path.parent.mkdir(parents=True, exist_ok=True)

    sanitized_url = sanitize_url(url)
    try:
        with session.get(url, timeout=timeout, stream=True) as resp:
            if resp.status_code != 200:
                raise requests.HTTPError(f"HTTP {resp.status_code} fetching feed {sanitized_url}")

            bytes_written = 0
            with open(temp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        f.write(chunk)
                        bytes_written += len(chunk)
                        if bytes_written > MAX_FEED_BYTES:
                            raise ValueError(
                                f"GBFS response exceeded safety ceiling of {MAX_FEED_BYTES / (1024 * 1024)} MB."
                            )

            if bytes_written == 0:
                raise ValueError(f"Received empty response from {sanitized_url}")

            # Validate that saved file is valid JSON and extract station count
            station_count = None
            with open(temp_path, "r", encoding="utf-8") as f:
                parsed = json.load(f)
                stations = parsed.get("data", {}).get("stations")
                if isinstance(stations, list):
                    station_count = len(stations)

            # Atomic rename
            temp_path.replace(target_path)
            return bytes_written, station_count
    except Exception:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass
        raise


def create_unique_gbfs_dir(base_dir: Path, utc_now: datetime) -> Tuple[str, Path]:
    """Generate collision-free run ID and create exclusive partition directory."""
    date_partition = utc_now.strftime("%Y-%m-%d")
    base_timestamp = utc_now.strftime("%Y%m%dT%H%M%S")
    microsec = utc_now.microsecond

    parent_partition = base_dir / "gbfs" / f"ingest_date={date_partition}"
    parent_partition.mkdir(parents=True, exist_ok=True)

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


def run_gbfs_ingestion(
    feed_url: Optional[str] = None,
    snapshots_root: Optional[Path | str] = None,
    timeout: int = 20,
    dry_run: bool = False,
    log_file: Optional[Path | str] = None,
) -> int:
    """Execute live GBFS feed discovery, fetch snapshots, and record manifests."""
    logger = setup_logger("ingest_gbfs", log_file=log_file)
    config = PipelineConfig.load()

    root_url = feed_url or config.gbfs.root_url
    snapshots_base = Path(snapshots_root) if snapshots_root else config.snapshots_root

    logger.info("==================================================")
    logger.info("       CitiBike GBFS Live Snapshot Ingestion      ")
    logger.info("==================================================")
    logger.info(f"GBFS Root URL    : {sanitize_url(root_url)}")
    logger.info(f"Snapshots Root   : {snapshots_base}")
    logger.info(f"Dry-run Mode     : {dry_run}")

    # Discover feed URLs
    try:
        gbfs_ver, feeds = discover_feeds(
            root_url=root_url,
            preferred_lang=config.gbfs.preferred_language,
            timeout=timeout,
            max_retries=config.max_retries,
            backoff_factor=config.retry_backoff_factor,
            logger=logger,
        )
    except Exception as e:
        logger.error(f"Failed to discover GBFS feeds: {e}")
        return 1

    # Use required_feeds from configuration
    required_feeds = config.gbfs.required_feeds or ["station_information", "station_status"]
    missing = [f for f in required_feeds if f not in feeds]
    if missing:
        logger.error(f"Missing required feeds from GBFS discovery: {missing}. Available: {list(feeds.keys())}")
        return 1

    for feed_name in required_feeds:
        logger.info(f"Discovered {feed_name}: {sanitize_url(feeds[feed_name])}")

    if dry_run:
        logger.info("[DRY RUN] Feed endpoints successfully discovered. Skipping download.")
        return 0

    # Build collision-free partition directory
    utc_now = datetime.now(timezone.utc)
    run_id, destination_dir = create_unique_gbfs_dir(snapshots_base, utc_now)

    manifest_records: List[Dict[str, Any]] = []

    # Fetch each required feed snapshot
    all_success = True
    for feed_name in required_feeds:
        target_url = feeds[feed_name]
        filename = f"{feed_name}.json"
        target_file = destination_dir / filename
        logger.info(f"Fetching snapshot: {feed_name} -> {target_file}")

        try:
            bytes_saved, station_count = fetch_and_save_json(
                url=target_url,
                target_path=target_file,
                timeout=timeout,
                max_retries=config.max_retries,
                backoff_factor=config.retry_backoff_factor,
                logger=logger,
            )
            checksum = calculate_sha256_file(target_file)
            logger.info(f"Saved {bytes_saved:,} bytes. SHA-256: {checksum}. Stations: {station_count}")

            try:
                dest_str = (
                    str(target_file.relative_to(project_root))
                    if target_file.is_relative_to(project_root)
                    else str(target_file)
                )
            except Exception:
                dest_str = str(target_file)

            manifest_entry = create_manifest(
                dataset=f"gbfs_{feed_name}",
                source_url=target_url,
                source_snapshot="realtime_snapshot",
                run_id=run_id,
                filename=filename,
                bytes_count=bytes_saved,
                sha256=checksum,
                destination=dest_str,
                status="success",
                record_count=station_count,
                pipeline_version=PIPELINE_VERSION,
                notes=f"GBFS Version: {gbfs_ver}, Feed: {feed_name}",
            )
            manifest_records.append(manifest_entry)
        except Exception as e:
            logger.error(f"Failed to fetch {feed_name}: {e}")
            all_success = False
            fail_entry = create_manifest(
                dataset=f"gbfs_{feed_name}",
                source_url=target_url,
                source_snapshot="realtime_snapshot",
                run_id=run_id,
                filename=filename,
                bytes_count=0,
                sha256="0" * 64,
                destination=str(target_file),
                status="failed",
                notes=f"Error: {e}",
            )
            manifest_records.append(fail_entry)

    # Write unified partition manifest
    manifest_path = destination_dir / "manifest.json"
    write_manifest_atomic(manifest_records, manifest_path)
    logger.info(f"Snapshot manifest written successfully: {manifest_path}")

    if all_success:
        logger.info("==================================================")
        logger.info("GBFS Ingestion Finished Successfully [OK]")
        logger.info("==================================================")
        return 0
    else:
        logger.error("GBFS Ingestion completed with partial errors.")
        return 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest GBFS station information and status snapshots."
    )
    parser.add_argument(
        "--feed-url",
        type=str,
        default=None,
        help="Root GBFS discovery endpoint (default: https://gbfs.citibikenyc.com/gbfs/gbfs.json).",
    )
    parser.add_argument(
        "--snapshots-root",
        type=str,
        default=None,
        help="Root directory for snapshot outputs (default: data/snapshots).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="HTTP request timeout in seconds (default: 20).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover feed endpoints without saving files to disk.",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Optional file path to persist execution logs.",
    )

    args = parser.parse_args()
    if args.timeout <= 0:
        print(f"Error: --timeout must be positive, got {args.timeout}", file=sys.stderr)
        sys.exit(1)

    code = run_gbfs_ingestion(
        feed_url=args.feed_url,
        snapshots_root=args.snapshots_root,
        timeout=args.timeout,
        dry_run=args.dry_run,
        log_file=args.log_file,
    )
    sys.exit(code)


if __name__ == "__main__":
    main()
