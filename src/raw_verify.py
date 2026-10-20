"""Raw Data Verification CLI for CitiBike-StationPulse.

Inspects raw immutable artifacts (ZIP, CSV, JSON, Parquet) using checksums,
archive validation, and Apache Spark DataFrame schema and count verification.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Optional

# Ensure project root is in sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.common.hashing import calculate_sha256_file
from src.common.logging_utils import setup_logger


def verify_zip_archive(zip_path: Path, logger) -> bool:
    """Verify ZIP file integrity, enumerate contents, and check for corruption."""
    logger.info(f"Validating ZIP archive integrity: {zip_path}")
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            bad_file = zf.testzip()
            if bad_file:
                logger.error(f"ZIP archive is corrupted! First bad file: {bad_file}")
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
    file_path: Path,
    data_format: str,
    logger,
) -> bool:
    """Read dataset with PySpark, print schema, count rows, and display top 5 records."""
    try:
        import pyspark
        from pyspark.sql import SparkSession
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

        path_str = str(file_path.resolve())

        logger.info(f"Loading DataFrame with format='{data_format}' from: {path_str}")
        if data_format == "csv":
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(path_str)
        elif data_format == "json":
            df = spark.read.json(path_str)
        elif data_format == "parquet":
            df = spark.read.parquet(path_str)
        else:
            logger.error(f"Unsupported format for Spark direct read: {data_format}")
            return False

        logger.info("--- Data Schema ---")
        df.printSchema()

        logger.info("Computing record count...")
        record_count = df.count()
        logger.info(f"Total Rows Verified: {record_count:,}")

        logger.info("--- Sample Top 5 Rows ---")
        df.show(5, truncate=False)

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
    target_path: Path,
    explicit_format: Optional[str] = None,
    log_file: Optional[Path | str] = None,
) -> int:
    """Verify raw file existence, size, SHA-256, and Spark readability."""
    logger = setup_logger("raw_verify", log_file=log_file)
    logger.info("==================================================")
    logger.info("          StationPulse Raw Data Verifier          ")
    logger.info("==================================================")

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

        logger.info("To verify CSV content inside ZIP with Spark, extracting CSV to a temporary directory...")
        with tempfile.TemporaryDirectory(prefix="stationpulse_verify_") as tmp_dir:
            extracted_csvs = []
            with zipfile.ZipFile(target_path, "r") as zf:
                for member in zf.namelist():
                    if member.lower().endswith(".csv") and not member.startswith("__MACOSX"):
                        zf.extract(member, tmp_dir)
                        extracted_csvs.append(Path(tmp_dir) / member)

            if not extracted_csvs:
                logger.warning("No CSV files found inside ZIP archive to test with Spark.")
                return 0

            # Verify the first extracted CSV
            target_csv = extracted_csvs[0]
            logger.info(f"Testing extracted CSV with Spark: {target_csv.name}")
            spark_ok = verify_spark_data(target_csv, "csv", logger)
            if not spark_ok:
                return 1

        logger.info("==================================================")
        logger.info("Raw ZIP and Internal CSV Verification Passed [OK]")
        logger.info("==================================================")
        return 0

    elif fmt in ("csv", "json", "parquet"):
        spark_ok = verify_spark_data(target_path, fmt, logger)
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
        help="Path to the raw artifact file (e.g. data/raw/.../file.zip or data/snapshots/.../station_status.json)",
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
        target_path=Path(args.path),
        explicit_format=args.format,
        log_file=args.log_file,
    )
    sys.exit(code)


if __name__ == "__main__":
    main()
