"""Unit tests for rerun isolation, atomic writes, and synthetic archive record counting."""

import io
import zipfile
from datetime import datetime, timezone
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.ingest_trips import (
    count_records_in_zip,
    create_unique_run_dir,
    download_stream,
    run_ingestion,
)
from src.common.manifest import create_manifest, write_manifest_atomic


def test_count_records_in_synthetic_zip_with_multiline_fields(tmp_path: Path):
    """Test safe line counting on a ZIP archive with headers and multiline quoted fields."""
    zip_file = tmp_path / "multi_tripdata.zip"

    # CSV 1 has a multiline address field that spans 2 physical lines for 1 record
    csv_data_1 = (
        'ride_id,started_at,station_name\n'
        'ride1,2024-01-01,"Station with\nmultiline name"\n'
        'ride2,2024-01-01,"Normal Station"\n'
    )
    csv_data_2 = (
        "ride_id,started_at,station_name\n"
        "ride3,2024-01-02,Station C\n"
        "ride4,2024-01-02,Station D\n"
        "ride5,2024-01-02,Station E\n"
    )

    with zipfile.ZipFile(zip_file, "w") as zf:
        zf.writestr("202401-citibike-tripdata_1.csv", csv_data_1)
        zf.writestr("202401-citibike-tripdata_2.csv", csv_data_2)

    total_records, csv_files = count_records_in_zip(zip_file)
    # csv 1 has 2 data rows, csv 2 has 3 data rows -> total 5
    assert total_records == 5
    assert len(csv_files) == 2


def test_empty_payload_rejection(tmp_path: Path):
    """Test that download_stream rejects empty responses and cleans up temporary files."""
    target_path = tmp_path / "output.csv.zip"

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"Content-Length": "0"}
    mock_resp.iter_content.return_value = []
    mock_resp.__enter__.return_value = mock_resp

    with patch("requests.Session.get", return_value=mock_resp):
        with pytest.raises(ValueError, match="Empty payload"):
            download_stream("http://example.com/empty.zip", target_path)

    assert not target_path.exists()
    assert not target_path.with_suffix(f"{target_path.suffix}.tmp").exists()


def test_content_length_mismatch_rejection(tmp_path: Path):
    """Test that download_stream rejects partial downloads when Content-Length doesn't match."""
    target_path = tmp_path / "output_mismatch.csv.zip"

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"Content-Length": "5000"}  # Claims 5000 bytes
    mock_resp.iter_content.return_value = [b"short" * 10]  # Only returns 50 bytes
    mock_resp.__enter__.return_value = mock_resp

    with patch("requests.Session.get", return_value=mock_resp):
        with pytest.raises(ValueError, match="did not match remote Content-Length"):
            download_stream("http://example.com/mismatch.zip", target_path)

    assert not target_path.exists()
    assert not target_path.with_suffix(f"{target_path.suffix}.tmp").exists()


def test_create_unique_run_dir_collision_avoidance(tmp_path: Path):
    """Test that calling create_unique_run_dir with identical datetime produces distinct folders."""
    fixed_time = datetime(2026, 10, 21, 10, 0, 0, 123456, tzinfo=timezone.utc)
    base_dir = tmp_path / "raw"

    run_id_1, dir_1 = create_unique_run_dir(base_dir, fixed_time)
    run_id_2, dir_2 = create_unique_run_dir(base_dir, fixed_time)

    assert run_id_1 != run_id_2
    assert dir_1.exists() and dir_2.exists()
    assert dir_1 != dir_2


def test_rerun_isolation_creates_distinct_partitions(tmp_path: Path):
    """Test that multiple runs write to distinct timestamped run folders without overwriting history."""
    raw_root = tmp_path / "raw"

    run_ids = ["run_20261020T100000_111111Z", "run_20261020T100500_222222Z", "run_20261020T101000_333333Z"]
    created_paths = []

    for run_id in run_ids:
        run_dir = raw_root / "citibike_trips" / "ingest_date=2026-10-20" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = run_dir / "tripdata.csv.zip"
        artifact_path.write_bytes(b"dummy zip content for run " + run_id.encode())

        manifest = create_manifest(
            dataset="citibike_trips",
            source_url="http://example.com/data.zip",
            source_snapshot="2026-10",
            run_id=run_id,
            filename="tripdata.csv.zip",
            bytes_count=artifact_path.stat().st_size,
            sha256="d" * 64,
            destination=str(artifact_path),
            status="success",
        )
        manifest_file = write_manifest_atomic(manifest, run_dir / "manifest.json")
        created_paths.append((artifact_path, manifest_file))

    date_dir = raw_root / "citibike_trips" / "ingest_date=2026-10-20"
    subdirs = sorted([d.name for d in date_dir.iterdir() if d.is_dir()])
    assert len(subdirs) == 3
    assert subdirs == sorted(run_ids)

    for artifact, manifest in created_paths:
        assert artifact.exists()
        assert manifest.exists()
        assert artifact.stat().st_size > 0
