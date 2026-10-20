"""Environment Verification Script for CitiBike-StationPulse.

Checks Python, Java, PySpark, and runs an in-memory smoke test:
    spark.range(1_000_000).count()
Shuts down Spark cleanly and returns exit code 0 on success, non-zero on failure.
"""

from __future__ import annotations

import argparse
import os
import platform
import subprocess
import sys
from pathlib import Path

# Add project root to path if executed directly
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.common.logging_utils import setup_logger


def get_java_version() -> str:
    """Retrieve Java version via java -version subprocess call."""
    try:
        proc = subprocess.run(
            ["java", "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
            check=False,
        )
        combined = (proc.stderr or "") + (proc.stdout or "")
        first_line = combined.strip().splitlines()[0] if combined.strip() else "Unknown Java version"
        return first_line
    except FileNotFoundError:
        return "NOT INSTALLED (java command not found in PATH)"
    except Exception as e:
        return f"ERROR checking Java: {e}"


def check_environment(log_file: Path | None = None) -> int:
    """Run all environment checks and PySpark smoke test.

    Returns:
        0 if all checks succeed, 1 if any failure occurs.
    """
    logger = setup_logger("env_check", log_file=log_file)
    logger.info("==================================================")
    logger.info("   StationPulse Environment Verification Check    ")
    logger.info("==================================================")

    # 1. OS & Python Check
    python_ver = platform.python_version()
    os_info = f"{platform.system()} {platform.release()} ({platform.architecture()[0]})"
    logger.info(f"Operating System : {os_info}")
    logger.info(f"Python Version   : {python_ver} ({sys.executable})")

    # 2. Java Check
    java_ver = get_java_version()
    logger.info(f"Java Runtime     : {java_ver}")
    if "NOT INSTALLED" in java_ver:
        logger.error("Java is required to run Apache Spark. Please install Java 17 or 21 LTS and set JAVA_HOME.")
        return 1

    # 3. PySpark import check
    try:
        import pyspark
        from pyspark.sql import SparkSession
        logger.info(f"PySpark Version  : {pyspark.__version__}")
    except ImportError:
        logger.error("PySpark is NOT installed. Run 'pip install -r requirements.txt' to install dependencies.")
        return 1

    # 4. PySpark Session Smoke Test
    logger.info("Initializing local SparkSession for smoke test...")
    spark = None
    try:
        # Suppress excessive Spark info logs during smoke test
        spark = (
            SparkSession.builder.master("local[*]")
            .appName("StationPulse-EnvCheck")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        spark_ver = spark.version
        logger.info(f"Active Spark Ver : {spark_ver}")

        # Run smoke test: spark.range(1_000_000).count()
        logger.info("Executing smoke test: spark.range(1_000_000).count() ...")
        cnt = spark.range(1_000_000).count()
        if cnt == 1_000_000:
            logger.info(f"Smoke Test PASSED: Generated and counted {cnt:,} records successfully.")
        else:
            logger.error(f"Smoke Test FAILED: Expected 1,000,000 records, got {cnt:,}")
            return 1

    except Exception as e:
        logger.error(f"SparkSession initialization or execution failed: {e}", exc_info=True)
        return 1
    finally:
        if spark is not None:
            logger.info("Stopping SparkSession cleanly...")
            try:
                spark.stop()
                logger.info("SparkSession stopped.")
            except Exception as se:
                logger.warning(f"Warning during spark.stop(): {se}")

    logger.info("==================================================")
    logger.info("All Environment Checks Passed Successfully! [OK]")
    logger.info("==================================================")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify local Python, Java, and PySpark execution environment for StationPulse."
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default="logs/env_check.txt",
        help="Optional path to save check output (default: logs/env_check.txt).",
    )
    parser.add_argument(
        "--no-file",
        action="store_true",
        help="Log strictly to console without writing to file.",
    )
    args = parser.parse_args()

    log_path = None if args.no_file else Path(args.log_file)
    exit_code = check_environment(log_path)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
