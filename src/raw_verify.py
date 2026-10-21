"""Raw Data Verification CLI for CitiBike-StationPulse.

Inspects raw immutable artifacts (ZIP, CSV, JSON, Parquet) using checksums,
archive validation, and Apache Spark DataFrame schema and count verification.
"""

from __future__ import annotations

import argparse
import glob
import os
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import List, Optional

# Ensure project root is in sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.common.hashing import calculate_sha256_file
from src.common.logging_utils import setup_logger


def resolve_artifact_path(input_path_str: str, logger) -> Path:
    """Resolve input path, handling wildcards/globs safely and selecting the latest match if multiple."""
    if any(c in input_path_str for c in ("*", "?", "[")):
        matches = sorted(glob.glob(input_path_str, recursive=True))
        if not matches:
            raise FileNotFoundError(f"No files matched wildcard pattern: {input_path_str}")
        chosen = matches[-1]  # Pick latest lexicographically / alphabetically
        logger.info(f"Wildcard pattern resolved {len(matches)} matches. Selecting: {chosen}")
        return Path(chosen)
    return Path(input_path_str)


def is_safe_zip_path(target_dir: Path, member_name: str) -> bool:
    """Prevent Zip Slip vulnerability by ensuring resolved extraction path stays inside target_dir."""
    norm = member_name.replace("\\", "/")
    if norm.startswith("/") or ".." in norm.split("/"):
        return False
    destination = (target_dir / member_name).resolve()
    return destination.is_relative_to(target_dir.resolve())


def verify_zip_archive(zip_path: Path, logger) -> bool:
    """Verify ZIP file integrity, enumerate contents, and check for corruption."""
    logger.info(f"Validating ZIP archive integrity: {zip_path}")
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            bad_file = zf.testzip()
            if bad_file:
                logger.error(f"ZIP archive is corrupted! First bad CRC member: {bad_file}")
                return False

            infolist = zf.infolist()
            total_uncompressed = sum(f.file_size for f in infolist)
            logger.info(f"Archive OK: {len(infolist)} files inside ({total_uncompressed:,} uncompressed bytes)")
            for item in infolist:
                logger.info(f" - {item.filename}: {item.file_size:,} bytes")
        return True
    except Exception as e:
        logger.error(f"Failed to inspect ZIP archive: {e}")
        return False


def verify_spark_data(
    file_paths: List[Path],
    data_format: str,
    logger,
) -> bool:
    """Read dataset with PySpark, print schema, count rows, and display top 5 records."""
    try:
        import pyspark
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        logger.error("PySpark is not installed in the environment. Please run: pip install -r requirements.txt")
        return False

    logger.info(f"Initializing local SparkSession to verify {data_format.upper()} data...")
    spark = None
    try:
        spark = (
            SparkSession.builder.master("local[*]")
            .appName("StationPulse-RawVerify")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        path_strings = [str(p.resolve()) for p in file_paths]
        logger.info(f"Loading DataFrame with format='{data_format}' from {len(path_strings)} file(s)...")

        if data_format == "csv":
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(path_strings)
            logger.info("--- CSV Schema ---")
            df.printSchema()

            logger.info("Computing total record count across CSV members...")
            record_count = df.count()
            logger.info(f"Total Rows Verified: {record_count:,}")

            logger.info("--- Sample Top 5 Rows ---")
            df.show(5, truncate=False)

        elif data_format == "json":
            # Use multiline=True for standard formatted JSON objects (like GBFS)
            df = spark.read.option("multiline", "true").json(path_strings)
            logger.info("--- JSON Root Schema ---")
            df.printSchema()

            root_count = df.count()
            logger.info(f"JSON Root Records: {root_count}")

            # Inspect if this is a GBFS feed containing data.stations
            if "data" in df.columns:
                try:
                    stations_df = df.select(F.explode("data.stations").alias("station"))
                    station_count = stations_df.count()
                    logger.info(f"Verified Stations in Feed: {station_count:,}")
                    logger.info("--- Sample Top 5 Station Records ---")
                    stations_df.select("station.*").show(5, truncate=False)
                except Exception:
                    df.show(5, truncate=False)
            else:
                df.show(5, truncate=False)

        elif data_format == "parquet":
            df = spark.read.parquet(*path_strings)
            logger.info("--- Parquet Schema ---")
            df.printSchema()

            record_count = df.count()
            logger.info(f"Total Rows Verified: {record_count:,}")

            logger.info("--- Sample Top 5 Rows ---")
            df.show(5, truncate=False)

        else:
            logger.error(f"Unsupported format for Spark direct read: {data_format}")
            return False

        return True
    except Exception as e:
        logger.error(f"Spark verification failed: {e}", exc_info=True)
        return False
    finally:
        if spark is not None:
            logger.info("Stopping SparkSession cleanly...")
            try:
                spark.stop()
                logger.info("SparkSession stopped.")
            except Exception as se:
                logger.warning(f"Warning during spark.stop(): {se}")


def verify_artifact(
    input_path_str: str,
    explicit_format: Optional[str] = None,
    log_file: Optional[Path | str] = None,
) -> int:
    """Verify raw file existence, size, SHA-256, and Spark readability."""
    logger = setup_logger("raw_verify", log_file=log_file)
    logger.info("==================================================")
    logger.info("          StationPulse Raw Data Verifier          ")
    logger.info("==================================================")

    try:
        target_path = resolve_artifact_path(input_path_str, logger)
    except Exception as e:
        logger.error(f"Path resolution error: {e}")
        return 1

    if not target_path.exists():
        logger.error(f"Target path does not exist: {target_path}")
        return 1

    if target_path.is_dir():
        logger.error(f"Target path is a directory. Please point directly to a file: {target_path}")
        return 1

    file_size = target_path.stat().st_size
    logger.info(f"Target File      : {target_path}")
    logger.info(f"File Size        : {file_size:,} bytes ({file_size / (1024 * 1024):.2f} MB)")

    if file_size == 0:
        logger.error("Verification FAILED: Target file is empty (0 bytes).")
        return 1

    # Checksum calculation
    sha256 = calculate_sha256_file(target_path)
    logger.info(f"SHA-256 Checksum : {sha256}")

    # Determine file format
    fmt = explicit_format.lower() if explicit_format else target_path.suffix.lstrip(".").lower()
    logger.info(f"Detected Format  : {fmt.upper()}")

    # ZIP Archive handling
    if fmt == "zip":
        is_valid_zip = verify_zip_archive(target_path, logger)
        if not is_valid_zip:
            return 1

        logger.info("Extracting CSV members to isolated workspace for Spark verification...")
        with tempfile.TemporaryDirectory(prefix="stationpulse_verify_") as tmp_dir:
            tmp_path = Path(tmp_dir)
            extracted_csvs: List[Path] = []
            with zipfile.ZipFile(target_path, "r") as zf:
                for member in zf.namelist():
                    norm = member.replace("\\", "/")
                    if norm.lower().endswith(".csv") and not norm.startswith("__MACOSX"):
                        if not is_safe_zip_path(tmp_path, norm):
                            logger.error(f"Unsafe ZIP member path detected (Zip Slip): {member}")
                            return 1
                        out_path = tmp_path / norm
                        out_path.parent.mkdir(parents=True, exist_ok=True)
                        with zf.open(member) as source, open(out_path, "wb") as dest:
                            dest.write(source.read())
                        extracted_csvs.append(out_path)

            if not extracted_csvs:
                logger.warning("No CSV files found inside ZIP archive to test with Spark.")
                return 0

            logger.info(
                f"Extracted {len(extracted_csvs)} CSV member(s) to temporary directory. Testing with Spark..."
            )
            spark_ok = verify_spark_data(extracted_csvs, "csv", logger)
            if not spark_ok:
                return 1

        logger.info("==================================================")
        logger.info("Raw ZIP and All Internal CSVs Spark Verified [OK]")
        logger.info("==================================================")
        return 0

    elif fmt in ("csv", "json", "parquet"):
        spark_ok = verify_spark_data([target_path], fmt, logger)
        if not spark_ok:
            return 1
        logger.info("==================================================")
        logger.info("Spark Raw Verification Passed [OK]")
        logger.info("==================================================")
        return 0

    else:
        logger.error(f"Unsupported format '{fmt}'. Must be one of: zip, csv, json, parquet.")
        return 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify raw Citi Bike artifacts using checksums and Spark DataFrame inspect."
    )
    parser.add_argument(
        "--path",
        type=str,
        required=True,
        help="Path or glob pattern to the raw artifact file (e.g. data/raw/.../*.zip or data/snapshots/.../*.json)",
    )
    parser.add_argument(
        "--format",
        type=str,
        default=None,
        choices=["zip", "csv", "json", "parquet"],
        help="Explicit format override (optional).",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default="logs/raw_verify.txt",
        help="Path to save verification log output (default: logs/raw_verify.txt).",
    )

    args = parser.parse_args()
    code = verify_artifact(
        input_path_str=args.path,
        explicit_format=args.format,
        log_file=args.log_file,
    )
    sys.exit(code)


if __name__ == "__main__":
    main()
