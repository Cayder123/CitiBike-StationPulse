# StationPulse: System Architecture & Ingestion Pipeline

## 1. Overview & Project Goal

StationPulse is an academic big-data pipeline designed to analyze Citi Bike trip demand and evaluate station bike imbalance in the New York City / Jersey City metropolitan area using Apache Spark (PySpark).

The architecture is partitioned into two distinct phases according to course milestones:
- **Phase 1 (Buổi 6 - Current Focus)**: Environment setup, robust data ingestion, immutable raw storage, cryptographic audit manifests, log sanitization, safe rerun isolation, and PySpark raw data verification.
- **Phase 2 (Buổi 7 - Extension)**: DataFrame transformations, hourly pickup/dropoff aggregations, net-flow station imbalance estimation, and analytical modeling.

---

## 2. High-Level Architecture Diagram

```mermaid
flowchart TD
    subgraph Data_Sources["1. Official Data Sources"]
        S3["AWS S3 Tripdata Archive\n(Monthly CSV/ZIP)\nhttps://citibikenyc.com/system-data"]
        GBFS["Live GBFS Endpoint\ngbfs.json\n(station_information & station_status)"]
    end

    subgraph Ingestion_Engine["2. Ingestion Engine (Python 3.11)"]
        TripIngest["ingest_trips.py\n- Streaming Download\n- Exponential Backoff\n- Atomic .tmp Rename\n- SHA-256 Checksum"]
        GBFSIngest["ingest_gbfs.py\n- Dynamic Feed Discovery\n- Live Realtime Snapshots\n- Zero-Poll Immutable Capture"]
    end

    subgraph Raw_Storage["3. Immutable Raw Storage"]
        RawTrips["data/raw/citibike_trips/\ningest_date=YYYY-MM-DD/run_YYYYMMDDTHHMMSS_ffffffZ[_NN]/\n- <source_file>.zip\n- manifest.json"]
        RawGBFS["data/snapshots/gbfs/\ningest_date=YYYY-MM-DD/run_YYYYMMDDTHHMMSS_ffffffZ[_NN]/\n- station_information.json\n- station_status.json\n- manifest.json"]
    end

    subgraph Verification_Engine["4. Verification & Audit"]
        EnvCheck["env_check.py\n- Python & Java runtime\n- PySpark smoke test"]
        RawVerify["raw_verify.py\n- Archive checksum & integrity\n- PySpark DataFrame read\n- printSchema(), count(), show(5)"]
    end

    subgraph Analytics_Phase["5. Buổi 7 Spark Analytics (Future)"]
        SparkTransform["Spark SQL & DataFrame\nNet-Flow = Dropoffs - Pickups\nProxy Imbalance Modeling"]
    end

    S3 -->|HTTP GET Stream| TripIngest
    GBFS -->|HTTP JSON Fetch| GBFSIngest

    TripIngest -->|Atomic Write| RawTrips
    GBFSIngest -->|Atomic Write| RawGBFS

    RawTrips -->|Inspect| RawVerify
    RawGBFS -->|Inspect| RawVerify
    EnvCheck -->|Validates Cluster/Local| RawVerify

    RawTrips -.->|Phase 2 Ingestion| SparkTransform
```

---

## 3. Storage Hierarchy & Partitioning Layout

All downloaded raw artifacts and real-time snapshots are strictly immutable. Rerunning ingestion pipelines generates a new timestamped partition rather than overwriting historical files:

```
D:\CitiBike-StationPulse/
├── data/
│   ├── raw/
│   │   └── citibike_trips/
│   │       └── ingest_date=YYYY-MM-DD/
│   │           └── run_YYYYMMDDTHHMMSS_ffffffZ/
│   │               ├── JC-202401-citibike-tripdata.csv.zip
│   │               └── manifest.json
│   ├── snapshots/
│   │   └── gbfs/
│   │       └── ingest_date=YYYY-MM-DD/
│   │           └── run_YYYYMMDDTHHMMSS_ffffffZ/
│   │               ├── station_information.json
│   │               ├── station_status.json
│   │               └── manifest.json
│   └── sample/
│       └── README.md
└── logs/
    ├── env_check.txt
    └── raw_verify.txt
```

---

## 4. Key Architectural Guarantees

1. **Byte-Level Immutability**:
   Raw archives are saved exactly as received from official servers. No records are filtered, cleaned, re-encoded, or converted (e.g. Parquet) inside `data/raw/`.
2. **Atomic Writes via Temporary Files**:
   Downloads stream to `.tmp` files. The final destination file only appears once streaming completes and file size matches expectations, preventing partial or corrupt downloads.
3. **Cryptographic Lineage (SHA-256)**:
   Every artifact is hashed using streaming SHA-256. The hash is saved alongside metadata in `manifest.json`.
4. **Isolated Rerun Capability**:
    With the default policy, each ingestion run creates a distinct `run_YYYYMMDDTHHMMSS_ffffffZ` directory and preserves its artifact and manifest. The optional `--skip-existing` policy can skip a valid same-day monthly artifact.
5. **No Secret / Credential Leakage**:
   All loggers pass through a sanitization filter that intercepts auth tokens and sensitive headers. `.gitignore` strictly protects raw datasets and environment files from Git commits.
