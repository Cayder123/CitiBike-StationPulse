"""Unit tests for the Spark processing pipeline in src/pipeline.py."""

import os
import sys
from datetime import datetime
from pathlib import Path
import pytest

# Ensure PySpark uses current venv python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Ensure JAVA_HOME is found even if terminal has not reloaded environment
if "JAVA_HOME" not in os.environ:
    candidate_jdk = Path(os.environ.get("USERPROFILE", "C:/Users/ASUS")) / "jdk-21"
    if candidate_jdk.exists():
        subdirs = [d for d in candidate_jdk.iterdir() if d.is_dir()]
        jdk_home = subdirs[0] if subdirs else candidate_jdk
        os.environ["JAVA_HOME"] = str(jdk_home)
        bin_dir = str(jdk_home / "bin")
        if bin_dir not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{bin_dir};{os.environ.get('PATH', '')}"

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    IntegerType,
)

from src.common.schemas import TRIPS_SCHEMA, STATIONS_SCHEMA
from src.pipeline import (
    clean_trips,
    transform_trips,
    join_stations,
    aggregate_trips,
    write_local_csv,
)


@pytest.fixture(scope="session")
def spark():
    """Create a lightweight local SparkSession for testing."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("StationPulse-Pipeline-Tests")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample_trips(spark):
    """Synthetic trip data covering deduplication, invalid records, and valid trips."""
    # Note: timestamps are in America/New_York context.
    # 2024-01-17 is Wednesday (weekday)
    # 2024-01-21 is Sunday (weekend)
    data = [
        # 1. Valid weekday trip (JC001 -> JC002, 10 min)
        (
            "R001",
            "classic_bike",
            datetime(2024, 1, 17, 14, 30, 0),  # Wednesday 14:30
            datetime(2024, 1, 17, 14, 40, 0),  # Wednesday 14:40
            "Station A",
            "JC001",
            "Station B",
            "JC002",
            40.71,
            -74.04,
            40.72,
            -74.05,
            "member",
        ),
        # 2. Duplicate ride_id (should be dropped)
        (
            "R001",
            "classic_bike",
            datetime(2024, 1, 17, 14, 30, 0),
            datetime(2024, 1, 17, 14, 40, 0),
            "Station A",
            "JC001",
            "Station B",
            "JC002",
            40.71,
            -74.04,
            40.72,
            -74.05,
            "member",
        ),
        # 3. Invalid: ended_at earlier than started_at (should be dropped)
        (
            "R002",
            "electric_bike",
            datetime(2024, 1, 17, 15, 0, 0),
            datetime(2024, 1, 17, 14, 55, 0),
            "Station A",
            "JC001",
            "Station B",
            "JC002",
            40.71,
            -74.04,
            40.72,
            -74.05,
            "casual",
        ),
        # 4. Invalid: same station & duration < 1 min (should be dropped)
        (
            "R003",
            "classic_bike",
            datetime(2024, 1, 17, 15, 0, 0),
            datetime(2024, 1, 17, 15, 0, 30),  # 30 seconds
            "Station A",
            "JC001",
            "Station A",
            "JC001",
            40.71,
            -74.04,
            40.71,
            -74.04,
            "member",
        ),
        # 5. Valid weekend trip (JC002 -> JC001, 15 min)
        (
            "R004",
            "electric_bike",
            datetime(2024, 1, 21, 18, 0, 0),  # Sunday
            datetime(2024, 1, 21, 18, 15, 0),
            "Station B",
            "JC002",
            "Station A",
            "JC001",
            40.72,
            -74.05,
            40.71,
            -74.04,
            "casual",
        ),
        # 6. Valid weekday trip across hour boundary (JC001 -> JC002)
        (
            "R005",
            "classic_bike",
            datetime(2024, 1, 17, 17, 50, 0),
            datetime(2024, 1, 17, 18, 10, 0),
            "Station A",
            "JC001",
            "Station B",
            "JC002",
            40.71,
            -74.04,
            40.72,
            -74.05,
            "member",
        ),
    ]
    return spark.createDataFrame(data, schema=TRIPS_SCHEMA)


@pytest.fixture
def sample_stations(spark):
    """Synthetic station dimension data."""
    data = [
        ("JC001", "Station A", 40.71, -74.04, 30),
        ("JC002", "Station B", 40.72, -74.05, 20),
    ]
    return spark.createDataFrame(data, schema=STATIONS_SCHEMA)


def test_clean_trips(sample_trips):
    """Verify cleaning drops duplicates, invalid timestamps, and short circular trips."""
    cleaned = clean_trips(sample_trips)
    # Expected remaining: R001, R004, R005 (total 3)
    assert cleaned.count() == 3

    ride_ids = [row.ride_id for row in cleaned.select("ride_id").collect()]
    assert sorted(ride_ids) == ["R001", "R004", "R005"]


def test_transform_trips(sample_trips):
    """Verify feature derivation: duration, day_type, and hour buckets."""
    cleaned = clean_trips(sample_trips)
    transformed = transform_trips(cleaned)

    columns = transformed.columns
    assert "trip_duration_min" in columns
    assert "day_type" in columns
    assert "hour_of_day" in columns
    assert "end_hour_of_day" in columns
    assert "end_day_type" in columns

    # Check R001 duration
    r001 = transformed.filter(transformed.ride_id == "R001").first()
    assert abs(r001.trip_duration_min - 10.0) < 1e-4
    assert r001.day_type in ["weekday", "weekend"]

    # Check R004 duration
    r004 = transformed.filter(transformed.ride_id == "R004").first()
    assert abs(r004.trip_duration_min - 15.0) < 1e-4

    # Check R005 duration (20 minutes)
    r005 = transformed.filter(transformed.ride_id == "R005").first()
    assert abs(r005.trip_duration_min - 20.0) < 1e-4


def test_join_stations(sample_trips, sample_stations):
    """Verify joining station dimension preserves row count and enriches fields."""
    cleaned = clean_trips(sample_trips)
    transformed = transform_trips(cleaned)
    joined = join_stations(transformed, sample_stations)

    assert joined.count() == transformed.count()
    assert "start_dim_name" in joined.columns
    assert "end_dim_name" in joined.columns
    assert "start_dim_capacity" in joined.columns
    assert "end_dim_capacity" in joined.columns


def test_aggregate_trips(sample_trips, sample_stations):
    """Verify hourly flow, monthly flow, and member/casual aggregations."""
    cleaned = clean_trips(sample_trips)
    transformed = transform_trips(cleaned)
    joined = join_stations(transformed, sample_stations)

    hourly_flow, monthly_flow, usage = aggregate_trips(joined, sample_stations)

    # Monthly flow verification
    monthly_rows = {row.station_id: row for row in monthly_flow.collect()}
    assert "JC001" in monthly_rows
    assert "JC002" in monthly_rows

    # JC001: Departures = R001, R005 (2). Arrivals = R004 (1). Net flow = 1 - 2 = -1
    jc001 = monthly_rows["JC001"]
    assert jc001.total_departures == 2
    assert jc001.total_arrivals == 1
    assert jc001.net_flow == -1

    # JC002: Departures = R004 (1). Arrivals = R001, R005 (2). Net flow = 2 - 1 = +1
    jc002 = monthly_rows["JC002"]
    assert jc002.total_departures == 1
    assert jc002.total_arrivals == 2
    assert jc002.net_flow == 1

    # Usage verification
    usage_dict = {row.member_casual: row for row in usage.collect()}
    assert "member" in usage_dict
    assert "casual" in usage_dict
    assert usage_dict["member"].total_trips == 2  # R001, R005
    assert usage_dict["casual"].total_trips == 1  # R004


def test_write_local_csv(tmp_path: Path, sample_trips):
    """Verify write_local_csv generates a valid CSV file without winutils."""
    out_file = tmp_path / "test_output.csv"
    write_local_csv(sample_trips, out_file, limit=2)

    assert out_file.exists()
    content = out_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(content) == 3  # Header + 2 data rows
    assert "ride_id" in content[0]
