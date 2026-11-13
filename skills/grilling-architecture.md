---
name: grilling-architecture
description: Structured architectural interview protocol (Grill-Me) to align grain, fiduciary math, HTAP schemas, and ODCS v3.2.0 contracts before modifying pipelines or DDL.
---

# Architectural Alignment Protocol ("Grill-Me")

## Purpose
This skill defines the deterministic design review protocol ("Grill-Me") executed before implementing new ingestion pipelines, schema migrations, or analytical marts whenever requirements contain semantic or technical gaps.

## Core Rule
- **MANDATE**: Walk down the decision tree resolving one dependency at a time, always providing a **recommended option (`(Recommended)`)** grounded in `./knowledge/domain/semantic_rules.md`, `./knowledge/domain/architecture_raw.md`, and `./contracts/comex_fiduciary_engine_v2.odcs.yaml`.
- **!ANTI_PATTERN**: Never guess missing financial grain, rounding behavior, tax regime semantics, or storage layout without clarifying them explicitly against the contract and DDL.

## Phase 1: Context Discovery (Mandatory Pre-Flight)
Before posing questions, silently inspect:
1. Domain rules in `./knowledge/domain/semantic_rules.md` and physical schemas in `./sql/ddl/` + `./alembic/versions/`.
2. Executable data contract `./contracts/comex_fiduciary_engine_v2.odcs.yaml` (`ODCS v3.2.0`).
3. Existing Pydantic V2 validators in `./src/models/` (`Decimal` quantization, Modulo-11 CNPJ, corporate tax IDs, `QuarantineRecordInput` states).

## Phase 2: Sequential Architectural Decision Tree

### 1. Granularity & Natural Keys
- **Key Question**: What is the exact atomic grain of the target entity (e.g., 1 row per `invoice_id` / `line_id`, 1 row per ledger `entry_id`, or 1 row per `(ano|mes|tipo_op|co_ncm|co_pais|sg_uf|co_via|co_urf)` SHA-256 `operation_hash_id`), and how are replays idempotently deduplicated?
- **Default Recommendation**: `(Recommended)` Preserve triple idempotency: MongoDB `payload_hash`/`idempotency_key` landing (`comex_raw_lake` on port `27018`), PostgreSQL `ingestion_idempotency_registry` + natural key `UNIQUE` constraints (port `5434`), and Parquet Hive deduplication on natural keys.

### 2. Monetary Precision & Fiduciary Invariants
- **Key Question**: For monetary amounts and FX rates, are strict `Decimal("0.0001")` (`NUMERIC(19,4)` + `*_cents` BIGINT) and `Decimal("0.000001")` (`NUMERIC(19,6)`) enforced via `parse_strict_decimal`, rejecting IEEE 754 `float`, `bool`, `NaN`/`Infinity`, and `None`?
- **Default Recommendation**: `(Recommended)` Reject all `float` inputs with Pydantic V2 `ValidationError` -> deflect persistent mismatches (> `0.0001`) to `comex_quarantine_hitl` (`PENDING_REVIEW` -> `APPROVED_OVERRIDE` | `REJECTED_HITL`), and require zero-sum (`SUM(debit) - SUM(credit) == 0.0000`) on `journal_entries` / `journal_lines`.

### 3. Temporal Semantics, Storage Partitioning & Governance
- **Key Question**: Which date column governs Hive partitioning in `./data/parquet_lakehouse` (`year=YYYY/month=MM`) for DuckDB queries, how is zero-lookahead PTAX (`date_id <= target_date_id`) preserved, and how do Alembic migrations stay aligned with `sql/ddl/`?
- **Default Recommendation**: `(Recommended)` Partition fact tables in Parquet Hive by competence/statistical `year`/`month` (`DECIMAL(19,4)` PyArrow schemas), apply LOCF forward-fill only (`date_id <= target_date_id`), sync schema via `uv run alembic upgrade head`, and certify with `make contract-lint`, `uv run ruff check .`, and `uv run pytest tests/ -v`.
