# Ingestion Metadata & Audit Trail

This directory can be used to store global pipeline metadata catalogs, audit indexes, or consolidated lineage summaries across runs.

Per-run manifests are stored directly with each ingested artifact partition:
- Trip data: `data/raw/citibike_trips/ingest_date=YYYY-MM-DD/run_<UTC_TIMESTAMP>/manifest.json`
- GBFS snapshots: `data/snapshots/gbfs/ingest_date=YYYY-MM-DD/run_<UTC_TIMESTAMP>/manifest.json`

Each manifest guarantees immutable data provenance, SHA-256 integrity, source URL attribution, and run lineage.
