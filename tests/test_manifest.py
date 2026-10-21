"""Unit tests for pipeline manifest creation, validation, and serialization."""

import json
import pytest
from pathlib import Path
from src.common.manifest import (
    create_manifest,
    validate_manifest_dict,
    write_manifest_atomic,
    REQUIRED_MANIFEST_FIELDS,
    MANIFEST_SCHEMA_VERSION,
)


def test_create_manifest_has_all_required_fields():
    dummy_hash = "a" * 64
    manifest = create_manifest(
        dataset="citibike_trips",
        source_url="https://s3.amazonaws.com/tripdata/JC-202401-citibike-tripdata.csv.zip",
        source_snapshot="2024-01",
        run_id="run_20261020T103000Z",
        filename="JC-202401-citibike-tripdata.csv.zip",
        bytes_count=1883307,
        sha256=dummy_hash,
        destination="data/raw/citibike_trips/ingest_date=2026-10-20/run_20261020T103000Z/file.zip",
        status="success",
        record_count=50000,
        pipeline_version="0.1.0",
        notes="Verified test run",
    )

    for field in REQUIRED_MANIFEST_FIELDS:
        assert field in manifest

    assert manifest["schema_version"] == MANIFEST_SCHEMA_VERSION
    assert manifest["dataset"] == "citibike_trips"
    assert manifest["status"] == "success"
    assert manifest["bytes"] == 1883307
    assert manifest["record_count"] == 50000
    assert manifest["sha256"] == dummy_hash


def test_validate_manifest_missing_keys():
    invalid_manifest = {
        "schema_version": "1.0.0",
        "dataset": "citibike_trips",
    }
    with pytest.raises(ValueError, match="Manifest missing required keys"):
        validate_manifest_dict(invalid_manifest)


def test_validate_manifest_invalid_status():
    manifest = create_manifest(
        dataset="test",
        source_url="http://example.com",
        source_snapshot="2024-01",
        run_id="run_1",
        filename="test.csv",
        bytes_count=100,
        sha256="b" * 64,
        destination="path/test.csv",
        status="success",
    )
    manifest["status"] = "unknown_status"
    with pytest.raises(ValueError, match="Invalid manifest status"):
        validate_manifest_dict(manifest)


def test_validate_manifest_invalid_sha256():
    manifest = {
        "schema_version": "1.0.0",
        "dataset": "test",
        "source_url": "http://example.com",
        "source_snapshot": "2024-01",
        "ingested_at_utc": "2026-10-21T00:00:00Z",
        "run_id": "run_1",
        "filename": "test.csv",
        "bytes": 100,
        "record_count": None,
        "sha256": "invalid_short_hash",
        "destination": "path/test.csv",
        "status": "success",
        "pipeline_version": "0.1.0",
        "git_commit": None,
        "notes": None,
    }
    with pytest.raises(ValueError, match="64-character hex SHA-256 string"):
        validate_manifest_dict(manifest)


def test_write_manifest_atomic(tmp_path: Path):
    target_manifest = tmp_path / "subdir" / "manifest.json"
    dummy_hash = "c" * 64
    data = create_manifest(
        dataset="test_atomic",
        source_url="http://example.com/test.zip",
        source_snapshot="2024-01",
        run_id="run_123",
        filename="test.zip",
        bytes_count=1024,
        sha256=dummy_hash,
        destination="data/raw/test.zip",
    )

    result_path = write_manifest_atomic(data, target_manifest)
    assert result_path.exists()
    assert not target_manifest.with_suffix(".tmp").exists()

    loaded = json.loads(result_path.read_text(encoding="utf-8"))
    assert loaded["dataset"] == "test_atomic"
    assert loaded["bytes"] == 1024
    assert loaded["sha256"] == dummy_hash
