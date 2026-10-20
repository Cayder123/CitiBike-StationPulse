# Sample Data Directory

This folder is intended strictly for minimal, synthetic, or non-sensitive schema test samples.

## Rules:
1. **Never copy production raw data here.**
2. Only small, anonymized test fixtures (e.g. 5-10 rows of mock trip data) may reside here to facilitate unit tests.
3. All actual ingestion outputs belong in `data/raw/` or `data/snapshots/`, which are strictly ignored by `.gitignore`.
