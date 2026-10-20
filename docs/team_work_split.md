# Team Work Split & Collaboration Protocol (5 Members)

To ensure seamless parallel development without merge conflicts or overlapping responsibilities, the project is structured into 5 clear workstreams.

---

## 1. Workstream Matrix

| Member | Primary Role | Core Files Owned | Suggested Branch Name | Key Acceptance Criteria |
| :--- | :--- | :--- | :--- | :--- |
| **Member 1** | **Coordinator & Integration Lead** | `README.md`, `pyproject.toml`, `config/`, `src/common/` | `feat/coordinator-integration` | Validated repository structure, interface freeze, clean end-to-end integration, git tag `checkpoint-2`. |
| **Member 2** | **Spark & Environment Engineer** | `src/env_check.py`, `requirements.txt`, environment docs | `feat/spark-environment` | Reliable Python 3.10+ / Java / PySpark validation, smoke test `spark.range(1_000_000).count()`, clear error reporting. |
| **Member 3** | **Historical Ingestion Specialist** | `src/ingest_trips.py`, `tests/test_rerun_policy.py` | `feat/trips-ingestion` | Streaming download, atomic `.tmp` rename, exponential backoff, SHA-256 calculation, multi-CSV inside ZIP counting, manifest output. |
| **Member 4** | **GBFS Feeds & Verification Engineer** | `src/ingest_gbfs.py`, `src/raw_verify.py` | `feat/gbfs-raw-verify` | Dynamic feed discovery via `gbfs.json`, immutable snapshot capture, PySpark DataFrame verification (`printSchema`, `count`, `show(5)`). |
| **Member 5** | **Architecture, Docs & Presentation Lead** | `docs/architecture.md`, `docs/evidence_checklist.md`, Slides | `docs/slides-architecture` | Clear architecture diagram, verified evidence checklist, 6-slide presentation deck, rehearsed ≤4 minute pitch. |

---

## 2. Detailed Workstream Breakdown & Contracts

### Member 1: Coordinator & Integration Lead
- **Responsibilities**:
  - Maintain project configuration freeze (`config/project.example.yaml`).
  - Oversee shared manifest format (`src/common/manifest.py`).
  - Ensure all sub-modules adhere to `.gitignore` rules (no raw data or logs committed).
  - Perform integration testing and create Git release tag `checkpoint-2`.

### Member 2: Spark Environment Engineer
- **Responsibilities**:
  - Test PySpark compatibility across team workstations (Windows PowerShell, macOS, Linux).
  - Ensure `env_check.py` handles missing Java/PySpark gracefully without crash.
  - Document setup guides in `README.md` for local Java 17/21 and PySpark installation.

### Member 3: Historical Trip Ingestion Specialist
- **Responsibilities**:
  - Implement and maintain `src/ingest_trips.py`.
  - Guarantee non-destructive rerun policy (creates `run_<UTC_TIMESTAMP>` partitions).
  - Handle large multi-part CSV archives inside ZIP files without corrupting disk state.
  - Unit test edge cases: empty responses, download interruptions, checksum mismatches.

### Member 4: GBFS Feeds & Verification Engineer
- **Responsibilities**:
  - Implement and test `src/ingest_gbfs.py` and `src/raw_verify.py`.
  - Validate auto-discovery against official Citi Bike GBFS endpoints.
  - Support Spark schema printing and sample previewing for CSV, JSON, and Parquet.
  - Document real-time availability limitations (instant snapshot vs historical net-flow).

### Member 5: Architecture, Documentation & Presentation Lead
- **Responsibilities**:
  - Maintain system diagrams (`docs/architecture.md`) and data dictionaries (`docs/data_dictionary.md`).
  - Prepare the Buổi 6 milestone presentation (6 slides, ≤4 minutes duration).
  - Verify every item in `docs/evidence_checklist.md` before team demo.
  - Ensure team members follow attribution and Citi Bike data policy guidelines.

---

## 3. GitHub Collaboration & Review Protocol

### 3.1 Adding Collaborators (GitHub UI)
1. Repository Owner navigates to `https://github.com/<owner>/CitiBike-StationPulse/settings/access`.
2. Click **Collaborators** -> **Add people**.
3. Search for the GitHub handles of all 4 teammates and send invitations.
4. Teammates must accept via GitHub email or notification.

### 3.2 Branching & Pull Request (PR) Policy
- **Never push directly to `main`**: All features must be developed on dedicated branches:
  ```bash
  git checkout -b feat/trips-ingestion
  ```
- **Pull Request Protocol**:
  1. Open a PR against `main`.
  2. PR description must reference the workstream and include output logs or test results.
  3. Require **at least 1 peer approval** (Coordinator or affected module owner).
  4. Ensure all unit tests pass locally before merging:
     ```bash
     python -m pytest tests -v
     ```
  5. Merge using **Squash and Merge** or **Rebase and Merge** to keep git history clean.

### 3.3 Shared File Ownership Rule
Files that affect multiple members (`requirements.txt`, `pyproject.toml`, `src/common/manifest.py`, `config/project.example.yaml`) are designated **Shared Files**. Changes to these files must be coordinated with Member 1 (Coordinator) before submitting a PR.
