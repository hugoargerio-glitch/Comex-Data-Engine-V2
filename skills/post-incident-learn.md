---
name: post-incident-learn
description: Structured post-incident learning workflow (Self-Healing Governance) that converts production failures into stricter ODCS v3.2.0 contracts, Pydantic/SQL tests, and runbooks in ./knowledge/runbooks/.
---

# Post-Incident Self-Healing & Institutional Learning Protocol

## Purpose
This procedure transforms resolved operational incidents into executable institutional memory auditable via Git Pull Request. No `P0`, `P1`, or `P2` incident is considered closed until all 5 steps below are completed.

## Core Rule
- **MANDATE**: Convert every identified root cause into an automated deterministic gate (Pydantic V2 validator in `./src/models/`, PostgreSQL trigger/constraint in `./sql/ddl/` + `./alembic/`, audit query in `./sql/queries/`, and unit/integration test in `./tests/`) bound to `./contracts/comex_fiduciary_engine_v2.odcs.yaml`.
- **!ANTI_PATTERN**: Closing an incident after a manual SQL hotfix or quarantine override without codifying regression tests and semantic rules to block recurrence.

## Mandatory 5-Step Closure Workflow

### Step 1: Extract Structured RCA Metadata
Collect from the incident ticket in `./knowledge/runbooks/INC-*.md`:
1. **Affected Asset & Contract**: ODCS v3.2.0 contract (`urn:datacontract:comex:fiduciary_engine_v2`) and target MongoDB/PostgreSQL/Parquet tables.
2. **Failure Signature**: Exact exception class, validation error, HTTP status code, or SQL trigger violation message.
3. **Detection Gap (Why the Gate Missed It)**: Why existing Pydantic validators, `datacontract lint`, SQL audit queries (`01..07`), or `pytest` suites failed to block or quarantine the anomaly earlier.

### Step 2: Reinforce Contract & Semantic Domain Rules
1. Update `./knowledge/domain/semantic_rules.md` with an explicit paired `MANDATE` / `!ANTI_PATTERN` invariant if a new business/fiscal rule was uncovered.
2. Tighten schema/quality definitions in `./contracts/comex_fiduciary_engine_v2.odcs.yaml` and add matching `Alembic` migration revisions if PostgreSQL DDL constraints changed.

### Step 3: Codify Automated Regression Checks
1. Add `@pytest.mark.unit` tests in `./tests/` covering the exact malformed payload, float/overflow vector, or tax/FX edge case without hitting live databases.
2. Add `@pytest.mark.integration` assertions (inside rolled-back transactions/savepoints) or SQL audit checks in `./sql/queries/` when database constraints or triggers are involved.

### Step 4: Full Verification Gate
Confirm zero failures and zero skips across:
1. `make contract-lint` (`uv run datacontract lint ./contracts/*.odcs.yaml`)
2. `uv run ruff check .`
3. `uv run alembic upgrade head`
4. `uv run pytest tests/ -v` (with `0 skipped`)

### Step 5: Finalize Incident Documentation
Update `./knowledge/runbooks/INC-*.md` with:
- Root cause summary and financial blast radius.
- Commit hashes implementing code, DDL/migration, contract, and test hardening.
- Verified evidence table (`PASS` across contract lint, ruff, SQL checks, and pytest).
