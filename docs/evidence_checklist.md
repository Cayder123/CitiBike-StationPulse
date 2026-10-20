# Buổi 6 Milestone Evidence Checklist & Presentation Outline

This checklist outlines the concrete evidence required to prove successful completion of the **Buổi 6 Ingestion & Verification Milestone**, along with the 6-slide presentation script for the team pitch.

---

## 1. Buổi 6 Deliverables & Evidence Matrix

| Checkpoint Item | Requirement | Command / Artifact Evidence | Status |
| :--- | :--- | :--- | :--- |
| **1. Environment** | Python 3.10+, Java 17/21 LTS, PySpark smoke test (1M records) | Run `python src/env_check.py` -> Clean exit code 0 | Verified |
| **2. Source Validation** | Official Citi Bike data sources, dynamic GBFS discovery | Inspect `https://citibikenyc.com/system-data` & `gbfs.json` | Verified |
| **3. Ingestion Contract** | Streaming download, atomic write via `.tmp`, exponential retry | Run `python src/ingest_trips.py --dry-run` and live ingest | Verified |
| **4. Raw Storage** | Immutable partitions: `data/raw/.../ingest_date=*/run_*/` | Directory listing proving byte-exact raw storage | Verified |
| **5. Manifest Lineage** | Cryptographic SHA-256, byte count, row count, UTC ISO-8601 | Inspect `manifest.json` inside run partition folder | Verified |
| **6. Sanitized Logs** | Structured UTC logs, credential/header filtering | Check logs in console or redirected log files | Verified |
| **7. Rerun Safety** | Run pipeline 3 times; verify 3 distinct partitions, zero overwrites | Run unit test: `tests/test_rerun_policy.py` | Verified |
| **8. Spark Raw Read** | PySpark reads CSV/JSON, prints schema, row count, and show(5) | Run `python src/raw_verify.py --path <artifact>` | Verified |

---

## 2. Six-Slide Presentation Outline (≤ 4 Minute Pitch)

### Slide 1: Title & Academic Scope (30 seconds)
- **Title**: StationPulse — Urban Mobility Analysis & Station Imbalance with PySpark
- **Team**: 5 Members (Coordinator, Environment, Trips Ingestion, GBFS/Verify, Presentation/Architecture)
- **Problem Statement**: Understanding Citi Bike usage surges and dock starvation/overflow patterns across NYC/NJ stations.

### Slide 2: Pipeline Architecture & Ingestion Contract (45 seconds)
- Highlight the **Source -> Ingestion -> Raw Immutable Storage -> Manifest -> Verification** flow.
- Core reliability mechanisms: streaming chunks, exponential backoff, atomic `.tmp` rename, streaming SHA-256 calculation.
- Mention data policies: strictly abiding by Citi Bike Data Sharing Policy (no redistribution, non-commercial research attribution).

### Slide 3: Immutable Storage & Rerun Policy (45 seconds)
- Explain partition strategy: `ingest_date=YYYY-MM-DD/run_<UTC_TIMESTAMP>/`.
- Demonstrate rerun test: running the pipeline 3 times creates 3 isolated runs without clobbering historical artifacts.
- Manifest schema: every partition holds a cryptographically verifiable `manifest.json`.

### Slide 4: Real-time Extension: GBFS Discovery (40 seconds)
- Dynamic feed discovery via official `gbfs.json`.
- Automatic resolution of `station_information` and `station_status`.
- **Key Caveat**: Real-time snapshot is instantaneous and cannot replace historical trip logs.

### Slide 5: PySpark Raw Data Verification (45 seconds)
- Verification script `src/raw_verify.py`.
- Demonstrates PySpark reading raw files: `printSchema()`, record count validation, and `show(5)` inspection.
- Clean shutdown of local SparkSession.

### Slide 6: Buổi 7 Roadmap & Q&A (35 seconds)
- Phase 2 preview: Spark SQL aggregations, hourly origin/destination matrix, station net-flow ($Flow = \text{Dropoffs} - \text{Pickups}$) proxy imbalance metric.
- Open floor for questions.
