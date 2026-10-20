"""Unit tests for rerun isolation, atomic writes, and synthetic archive record counting."""

import io
import zipfile
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.ingest_trips import count_records_in_zip, download_stream, run_ingestion
from src.common.manifest import create_manifest, write_manifest_atomic


def test_count_records_in_synthetic_zip(tmp_path: Path):
    """Test safe line counting on a ZIP archive containing multiple CSVs."""
    zip_file = tmp_path / "multi_tripdata.zip"

    csv_data_1 = "ride_id,started_at,ended_at\nride1,2024-01-01,2024-01-01\nride2,2024-01-01,2024-01-01\n"
    csv_data_2 = "ride_id,started_at,ended_at\nride3,2024-01-02,2024-01-02\nride4,2024-01-02,2024-01-02\nride5,2024-01-02,2024-01-02\n"

    with zipfile.ZipFile(zip_file, "w") as zf:
        zf.writestr("202401-citibike-tripdata_1.csv", csv_data_1)
        zf.writestr("202401-citibike-tripdata_2.csv", csv_data_2)

    total_records, csv_files = count_records_in_zip(zip_file)
    assert total_records == 5  # 2 rows in first, 3 rows in second
    assert len(csv_files) == 2
    assert "202401-citibike-tripdata_1.csv" in csv_files
    assert "202401-citibike-tripdata_2.csv" in csv_files


def test_empty_payload_rejection(tmp_path: Path):
    """Test that download_stream rejects empty responses and cleans up temporary files."""
    target_path = tmp_path / "output.csv.zip"

    # Mock response returning 0 bytes
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"Content-Length": "0"}
    mock_resp.iter_content.return_value = []
    mock_resp.__enter__.return_value = mock_resp

    with patch("requests.Session.get", return_value=mock_resp):
        with pytest.raises(ValueError, match="Empty payload"):
            download_stream("http://example.com/empty.zip", target_path)

    # Destination and temporary file must NOT exist
    assert not target_path.exists()
    assert not target_path.with_suffix(f"{target_path.suffix}.tmp").exists()


def test_rerun_isolation_creates_distinct_partitions(tmp_path: Path):
    """Test that multiple runs write to distinct timestamped run folders without overwriting history."""
    raw_root = tmp_path / "raw"

    # Simulate 3 sequential runs
    run_ids = ["run_20261020T100000Z", "run_20261020T100500Z", "run_20261020T101000Z"]
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
            sha256="abc123",
            destination=str(artifact_path),
            status="success",
        )
        manifest_file = write_manifest_atomic(manifest, run_dir / "manifest.json")
        created_paths.append((artifact_path, manifest_file))

    # Verify that all 3 run directories exist independently
    date_dir = raw_root / "citibike_trips" / "ingest_date=2026-10-20"
    subdirs = sorted([d.name for d in date_dir.iterdir() if d.is_dir()])
    assert len(subdirs) == 3
    assert subdirs == sorted(run_ids)

    # Verify content was preserved intact
    for artifact, manifest in created_paths:
        assert artifact.exists()
        assert manifest.exists()
        assert artifact.stat().st_size > 0
