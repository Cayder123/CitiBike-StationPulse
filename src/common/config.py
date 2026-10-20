"""Configuration manager for CitiBike-StationPulse pipeline.

Resolves settings with precedence: CLI Arguments > Environment Variables > Config File > Built-in Defaults.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore


@dataclass
class GBFSConfig:
    root_url: str = "https://gbfs.citibikenyc.com/gbfs/gbfs.json"
    preferred_language: str = "en"
    required_feeds: List[str] = field(default_factory=lambda: ["station_information", "station_status"])


@dataclass
class TripConfig:
    base_url: str = "https://s3.amazonaws.com/tripdata"
    default_dataset: str = "jc"
    default_year: int = 2024
    default_month: int = 1


@dataclass
class PipelineConfig:
    raw_root: Path = Path("data/raw")
    snapshots_root: Path = Path("data/snapshots")
    logs_root: Path = Path("logs")
    timeout_seconds: int = 30
    max_download_mb: int = 500
    max_retries: int = 3
    retry_backoff_factor: float = 2.0
    gbfs: GBFSConfig = field(default_factory=GBFSConfig)
    trips: TripConfig = field(default_factory=TripConfig)

    @classmethod
    def load(cls, config_path: Optional[Path | str] = None) -> PipelineConfig:
        """Load configuration from file, falling back to env vars and defaults."""
        config = cls()

        # Try to load YAML file if provided or default exists
        candidate_paths = [
            Path(config_path) if config_path else None,
            Path("config/project.yaml"),
            Path("config/project.example.yaml"),
        ]

        found_yaml_path = None
        for p in candidate_paths:
            if p and p.is_file():
                found_yaml_path = p
                break

        if found_yaml_path and yaml:
            try:
                with open(found_yaml_path, "r", encoding="utf-8") as f:
                    raw_data = yaml.safe_load(f) or {}
                storage = raw_data.get("storage", {})
                if "raw_root" in storage:
                    config.raw_root = Path(storage["raw_root"])
                if "snapshots_root" in storage:
                    config.snapshots_root = Path(storage["snapshots_root"])
                if "logs_root" in storage:
                    config.logs_root = Path(storage["logs_root"])

                network = raw_data.get("network", {})
                if "timeout_seconds" in network:
                    config.timeout_seconds = int(network["timeout_seconds"])
                if "max_download_mb" in network:
                    config.max_download_mb = int(network["max_download_mb"])
                if "max_retries" in network:
                    config.max_retries = int(network["max_retries"])
                if "retry_backoff_factor" in network:
                    config.retry_backoff_factor = float(network["retry_backoff_factor"])

                sources = raw_data.get("data_sources", {})
                gbfs = sources.get("gbfs", {})
                if "root_url" in gbfs:
                    config.gbfs.root_url = str(gbfs["root_url"])
                if "preferred_language" in gbfs:
                    config.gbfs.preferred_language = str(gbfs["preferred_language"])

                trips = sources.get("trips", {})
                if "base_url" in trips:
                    config.trips.base_url = str(trips["base_url"])
                if "default_dataset" in trips:
                    config.trips.default_dataset = str(trips["default_dataset"])
                if "default_year" in trips:
                    config.trips.default_year = int(trips["default_year"])
                if "default_month" in trips:
                    config.trips.default_month = int(trips["default_month"])
            except Exception:
                pass

        # Environment variable overrides
        if raw_env := os.getenv("STATIONPULSE_RAW_ROOT"):
            config.raw_root = Path(raw_env)
        if snap_env := os.getenv("STATIONPULSE_SNAPSHOTS_ROOT"):
            config.snapshots_root = Path(snap_env)
        if logs_env := os.getenv("STATIONPULSE_LOGS_ROOT"):
            config.logs_root = Path(logs_env)
        if timeout_env := os.getenv("STATIONPULSE_DOWNLOAD_TIMEOUT"):
            config.timeout_seconds = int(timeout_env)
        if max_mb_env := os.getenv("STATIONPULSE_MAX_DOWNLOAD_MB"):
            config.max_download_mb = int(max_mb_env)
        if gbfs_url_env := os.getenv("STATIONPULSE_GBFS_ROOT_URL"):
            config.gbfs.root_url = gbfs_url_env
        if trips_url_env := os.getenv("STATIONPULSE_TRIPDATA_BASE_URL"):
            config.trips.base_url = trips_url_env

        return config
