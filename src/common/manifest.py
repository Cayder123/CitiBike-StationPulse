"""Manifest generation, validation, and serialization for StationPulse data artifacts.

Guarantees data provenance, auditability, and immutability across pipeline runs.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from src.common.logging_utils import sanitize_url

MANIFEST_SCHEMA_VERSION = "1.0.0"

REQUIRED_MANIFEST_FIELDS = {
    "schema_version",
    "dataset",
    "source_url",
    "source_snapshot",
    "ingested_at_utc",
    "run_id",
    "filename",
    "bytes",
    "record_count",
    "sha256",
    "destination",
    "status",
    "pipeline_version",
    "git_commit",
    "notes",
}

_HEX_64_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")


@dataclass
class ManifestRecord:
    """Dataclass holding artifact metadata for pipeline provenance."""

    schema_version: str
    dataset: str
    source_url: str
    source_snapshot: str
    ingested_at_utc: str
    run_id: str
    filename: str
    bytes: int
    record_count: Optional[int]
    sha256: str
    destination: str
    status: str
    pipeline_version: str
    git_commit: Optional[str]
    notes: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def get_git_commit_hash(repo_dir: Optional[Union[str, Path]] = None) -> Optional[str]:
    """Retrieve the current Git commit hash if running in a Git repository."""
    try:
        cmd = ["git", "rev-parse", "HEAD"]
        cwd = str(repo_dir) if repo_dir else None
        res = subprocess.run(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5,
            check=False,
        )
        if res.returncode == 0:
            commit = res.stdout.strip()
            return commit if commit else None
    except Exception:
        pass
    return None


def get_utc_now_iso() -> str:
    """Return current UTC time in ISO-8601 format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def validate_manifest_dict(data: Dict[str, Any]) -> bool:
    """Validate that the manifest dictionary contains all required fields and valid types.

    Args:
        data: Manifest dictionary to validate.

    Returns:
        True if valid.

    Raises:
        ValueError: If a required field is missing or invalid.
    """
    missing = REQUIRED_MANIFEST_FIELDS - set(data.keys())
    if missing:
        raise ValueError(f"Manifest missing required keys: {sorted(list(missing))}")

    if data["status"] not in ("success", "skipped", "failed"):
        raise ValueError(
            f"Invalid manifest status: {data['status']}. Must be 'success', 'skipped', or 'failed'."
        )

    if not isinstance(data["bytes"], int) or data["bytes"] < 0:
        raise ValueError(f"Manifest 'bytes' must be a non-negative integer, got {data['bytes']}")

    if data["status"] == "success":
        if data["bytes"] <= 0:
            raise ValueError(f"Successful manifest must have positive byte count, got {data['bytes']}")
        if not isinstance(data["sha256"], str) or not _HEX_64_PATTERN.match(data["sha256"]):
            raise ValueError(
                f"Successful manifest requires valid 64-character hex SHA-256 string, got: {data['sha256']!r}"
            )

    if data["record_count"] is not None:
        if not isinstance(data["record_count"], int) or data["record_count"] < 0:
            raise ValueError(
                f"Manifest 'record_count' must be a non-negative integer or None, got: {data['record_count']}"
            )

    return True


def create_manifest(
    dataset: str,
    source_url: str,
    source_snapshot: str,
    run_id: str,
    filename: str,
    bytes_count: int,
    sha256: str,
    destination: str,
    status: str = "success",
    record_count: Optional[int] = None,
    pipeline_version: str = "0.1.0",
    git_commit: Optional[str] = None,
    notes: Optional[str] = None,
    ingested_at_utc: Optional[str] = None,
) -> Dict[str, Any]:
    """Construct and validate a manifest dictionary."""
    if ingested_at_utc is None:
        ingested_at_utc = get_utc_now_iso()

    if git_commit is None:
        git_commit = get_git_commit_hash()

    sanitized_source = sanitize_url(source_url)

    record = ManifestRecord(
        schema_version=MANIFEST_SCHEMA_VERSION,
        dataset=dataset,
        source_url=sanitized_source,
        source_snapshot=source_snapshot,
        ingested_at_utc=ingested_at_utc,
        run_id=run_id,
        filename=filename,
        bytes=bytes_count,
        record_count=record_count,
        sha256=sha256,
        destination=str(destination),
        status=status,
        pipeline_version=pipeline_version,
        git_commit=git_commit,
        notes=notes,
    )
    manifest_dict = record.to_dict()
    validate_manifest_dict(manifest_dict)
    return manifest_dict


def write_manifest_atomic(
    manifest_data: Union[Dict[str, Any], List[Dict[str, Any]]],
    target_path: Union[str, Path],
) -> Path:
    """Atomically write manifest data to JSON file using a temporary file.

    Args:
        manifest_data: Manifest dict or list of manifest dicts.
        target_path: Path where manifest.json will be saved.

    Returns:
        Path to the saved manifest file.
    """
    path = Path(target_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(".tmp")

    # If list of dicts, validate all items
    if isinstance(manifest_data, list):
        for item in manifest_data:
            validate_manifest_dict(item)
    else:
        validate_manifest_dict(manifest_data)

    with temp_path.open("w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())

    # Atomic rename (on Windows os.replace is atomic)
    temp_path.replace(path)
    return path
