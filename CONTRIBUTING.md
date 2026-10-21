# Contributing to CitiBike-StationPulse

Welcome to the team! To ensure a smooth, conflict-free development experience for all 4 team members, please adhere to the following workflow and guidelines.

---

## 1. Branch Naming Conventions

All development must occur on topic branches created from the latest `main` branch. Never commit directly to `main`.

Use the following prefixes:
- `feat/<feature-name>`: New pipeline functionality or scripts (e.g. `feat/trips-ingestion`)
- `fix/<bug-name>`: Bug fixes or error resolution (e.g. `fix/atomic-rename-windows`)
- `docs/<doc-topic>`: Documentation, architecture updates, slide preparations (e.g. `docs/work-split`)
- `test/<test-scope>`: Additional test suites or mocks (e.g. `test/manifest-validation`)

---

## 2. Commit Message Guidelines

Keep commit messages concise and descriptive:
```
<type>(<scope>): <short summary in present tense>

[optional body explaining context or rationale]
```

Examples:
- `feat(ingest): add streaming download with exponential backoff`
- `fix(manifest): correct timestamp format to UTC ISO-8601`
- `docs(team): assign workstream acceptance criteria`
- `test(rerun): verify run partition isolation`

---

## 3. Pull Request (PR) & Review Workflow

1. **Pull Latest Changes**: Before creating a branch or opening a PR, always synchronize with `main`:
   ```powershell
   git checkout main
   git pull origin main
   ```
2. **Run Tests Locally**: All unit tests must pass before opening a PR:
   ```powershell
   python -m pytest tests -v
   ```
3. **Open PR with Context**: Include a brief description of what was changed and attach relevant CLI execution outputs.
4. **Peer Review**: At least **one team member approval** is required prior to merging.
5. **Merge Strategy**: Use **Squash and Merge** or **Rebase and Merge** to maintain a clean linear history.

---

## 4. How to Prevent Merge Conflicts

- **Respect Module Ownership**: Follow the file assignments listed in `docs/team_work_split.md`. Do not modify other members' files without prior coordination.
- **Shared Files Policy**: Changes to `requirements.txt`, `pyproject.toml`, `config/project.example.yaml`, or `src/common/manifest.py` must be vetted by the Integration Coordinator.
- **Commit Granularity**: Make small, frequent PRs rather than massive single pull requests.

---

## 5. Absolute Prohibitions

1. **NEVER commit raw data or snapshots**: `data/raw/**` and `data/snapshots/**` are ignored by `.gitignore`. Do not bypass this rule with `git add -f`.
2. **NEVER commit secrets or credentials**: Keep any tokens or environment variables in `.env` (which is gitignored).
3. **NEVER commit runtime logs**: Execution logs in `logs/*.txt` must remain local.
