# CitiBike-StationPulse Data Dictionary & Schema Reference

This document outlines the data schemas for both historical monthly trip records and live General Bikeshare Feed Specification (GBFS) snapshot feeds.

---

## 1. Historical Trip Data (CSV Schema)

Citi Bike publishes trip data in standardized CSV formats. Starting from February 2021, records conform to the following schema:

| Column Name | Data Type | Nullable | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `ride_id` | String | No | Unique 16-character identifier for each trip | `655CCEB034843684` |
| `rideable_type` | String | No | Type of bicycle used (`classic_bike`, `electric_bike`) | `classic_bike` |
| `started_at` | Timestamp | No | Trip departure timestamp (UTC/Local) `YYYY-MM-DD HH:MM:SS` | `2024-01-01 00:02:15` |
| `ended_at` | Timestamp | No | Trip completion timestamp `YYYY-MM-DD HH:MM:SS` | `2024-01-01 00:15:30` |
| `start_station_name` | String | Yes | Name of origin Citi Bike station | `Hoboken Terminal - River St & Hudson Pl` |
| `start_station_id` | String | Yes | System station identifier for origin | `HB102` |
| `end_station_name` | String | Yes | Name of destination Citi Bike station | `South Waterfront Walkway - Sinatra Dr & 1 St` |
| `end_station_id` | String | Yes | System station identifier for destination | `HB103` |
| `start_lat` | Double | Yes | Origin station latitude in decimal degrees | `40.736004` |
| `start_lng` | Double | Yes | Origin station longitude in decimal degrees | `-74.030865` |
| `end_lat` | Double | Yes | Destination station latitude in decimal degrees | `40.736982` |
| `end_lng` | Double | Yes | Destination station longitude in decimal degrees | `-74.027781` |
| `member_casual` | String | No | Rider subscription classification (`member` vs `casual`) | `member` |

> [!NOTE]
> For rides where users dock bikes outside geofenced stations or e-bike dropoffs, `start_station_name` / `end_station_name` may be null while coordinates remain populated.

---

## 2. GBFS Realtime Feeds (JSON Schema)

GBFS (General Bikeshare Feed Specification) feeds represent point-in-time snapshots of station locations and instantaneous dock availability.

### 2.1 `station_information.json`
Static metadata describing all active docking stations in the fleet.

```json
{
  "station_id": "HB102",
  "name": "Hoboken Terminal - River St & Hudson Pl",
  "short_name": "HB102",
  "lat": 40.736004,
  "lon": -74.030865,
  "region_id": "71",
  "capacity": 30,
  "rental_methods": ["KEY", "CREDITCARD"]
}
```

### 2.2 `station_status.json`
Dynamic snapshot representing current availability.

```json
{
  "station_id": "HB102",
  "num_bikes_available": 12,
  "num_ebikes_available": 4,
  "num_bikes_disabled": 0,
  "num_docks_available": 18,
  "num_docks_disabled": 0,
  "is_installed": 1,
  "is_renting": 1,
  "is_returning": 1,
  "last_reported": 1704067200
}
```

> [!IMPORTANT]
> **Station Availability Caveat**:
> GBFS captures current real-time status. A single GBFS snapshot CANNOT reconstruct historical bike shortages. To analyze historical imbalance, StationPulse computes station pick-up and drop-off net-flows from historical trip records as an analytical proxy.

---

## 3. Pipeline Ingestion Manifest Schema (`manifest.json`)

Stored alongside every raw partition for provenance tracking.

| Key | Type | Description |
| :--- | :--- | :--- |
| `schema_version` | String | Manifest schema version (`1.0.0`) |
| `dataset` | String | Dataset identifier (`citibike_trips`, `gbfs_station_information`) |
| `source_url` | String | Exact remote download endpoint |
| `source_snapshot` | String | Period tag (`2024-01`, `realtime_snapshot`) |
| `ingested_at_utc` | String | UTC ISO-8601 execution timestamp |
| `run_id` | String | Execution run identifier (`run_YYYYMMDDTHHMMSS_ffffffZ`; collision suffix may be added) |
| `filename` | String | Downloaded artifact basename |
| `bytes` | Integer | Exact byte count on disk |
| `record_count` | Integer \| null | Verified rows (CSV rows or station count) |
| `sha256` | String | Cryptographic 64-character SHA-256 hash |
| `destination` | String | Relative destination path |
| `status` | String | Outcome state: `success`, `skipped`, or `failed` |
| `pipeline_version` | String | Semantic version of the code (`0.1.0`) |
| `git_commit` | String \| null | Current git commit hash when ingested |
| `notes` | String \| null | Supplemental diagnostic context |
