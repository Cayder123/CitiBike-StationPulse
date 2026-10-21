"""Integration test for Spark raw verification using synthetic local fixtures."""

import json
import zipfile
import pytest
from pathlib import Path
from src.raw_verify import verify_artifact


def test_spark_verify_synthetic_zip(tmp_path: Path):
    """Test verify_artifact on a synthetic ZIP archive containing 2 CSV files."""
    zip_path = tmp_path / "synthetic_trips.zip"

    csv_content_1 = (
        "ride_id,rideable_type,started_at,ended_at,start_station_name\n"
        "R001,classic_bike,2024-01-01 10:00:00,2024-01-01 10:15:00,Station Alpha\n"
        "R002,electric_bike,2024-01-01 10:05:00,2024-01-01 10:20:00,Station Beta\n"
    )
    csv_content_2 = (
        "ride_id,rideable_type,started_at,ended_at,start_station_name\n"
        "R003,classic_bike,2024-01-01 11:00:00,2024-01-01 11:10:00,Station Gamma\n"
    )

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("tripdata_part1.csv", csv_content_1)
        zf.writestr("tripdata_part2.csv", csv_content_2)

    log_file = tmp_path / "test_verify.log"
    exit_code = verify_artifact(
        input_path_str=str(zip_path),
        explicit_format="zip",
        log_file=log_file,
    )
    assert exit_code == 0
    assert log_file.exists()
    log_text = log_file.read_text(encoding="utf-8")
    assert "Total Rows Verified: 3" in log_text
    assert "Raw ZIP and All Internal CSVs Spark Verified [OK]" in log_text


def test_spark_verify_gbfs_json(tmp_path: Path):
    """Test verify_artifact on a synthetic GBFS station_status JSON document."""
    json_path = tmp_path / "station_status.json"

    gbfs_data = {
        "last_updated": 1704067200,
        "ttl": 60,
        "version": "1.1",
        "data": {
            "stations": [
                {
                    "station_id": "HB101",
                    "num_bikes_available": 10,
                    "num_docks_available": 20,
                    "is_installed": 1,
                    "is_renting": 1,
                },
                {
                    "station_id": "HB102",
                    "num_bikes_available": 5,
                    "num_docks_available": 25,
                    "is_installed": 1,
                    "is_renting": 1,
                },
            ]
        },
    }
    json_path.write_text(json.dumps(gbfs_data, indent=2), encoding="utf-8")

    log_file = tmp_path / "test_gbfs_verify.log"
    exit_code = verify_artifact(
        input_path_str=str(json_path),
        explicit_format="json",
        log_file=log_file,
    )
    assert exit_code == 0
    assert log_file.exists()
    log_text = log_file.read_text(encoding="utf-8")
    assert "Verified Stations in Feed: 2" in log_text
    assert "Spark Raw Verification Passed [OK]" in log_text
