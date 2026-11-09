---
name: grilling-architecture
description: Structured pre-code technical interview protocol to eliminate ambiguities in grain, ODCS v3.2.0 contracts, PII governance, SLAs, and financial reconciliation before implementation.
version: 1.0.0
owner: data-architecture-team
invoked_by: ["data-architecture-planner", "main-orchestrator"]
audit_trail: git-pr
---

# Skill: Grilling Architecture Interview (`grilling-architecture.md`)

This skill defines the architectural alignment protocol ("Grill-Me") executed by the main orchestrator or the `planner` subagent whenever a pipeline request contains semantic or technical gaps.

- **MANDATE**: Walk down the decision tree resolving one dependency at a time, always providing a **recommended option (`(Recommended)`)** grounded in `./knowledge/domain/semantic_rules.md` and the `datahub` catalog.
- **!ANTI_PATTERN**: Never dump a 10-question questionnaire all at once or guess financial/tax rules by inference when they are not documented in `./knowledge/`.

---

## Step 1 — Silent Pre-Inspection (Zero-Redundancy Check)
Before asking the human engineer any question, strictly execute:
1. Local search via `rg` across `./knowledge/domain/semantic_rules.md` and `./contracts/*.odcs.yaml` to check if the rule is already specified.
2. Live metadata lookup via `datahub` MCP to inspect existing schemas, primary keys, and lineage for referenced tables.
3. **Golden Rule**: If the answer already exists in the codebase, ODCS contract, or MCP catalog, **do not ask the user** — use the verified evidence directly.

## Step 2 — Interview Decision Tree (5 Critical Branches)
Walk through the branches below sequentially (one question per turn) only for dimensions that remain unresolved after Step 1:

### Branch A: Target Grain & Primary Identity
- **Key Question**: What is the exact atomic grain of the target table (e.g., 1 row per `order_id`, 1 row per `order_item_id`, or daily summary by `order_date`), and how should duplicate or late-arriving CDC events be handled?
- **Default Recommendation**: Propose deterministic deduplication via `ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY <updated_at> DESC) = 1` with the natural key documented in the ODCS contract.

### Branch B: JOIN Cardinality & Fan-Out Prevention
- **Key Question**: When joining the anchor entity with auxiliary tables, what is the expected cardinality (`1:1`, `N:1`, `1:N`) and what pre-aggregation rule must be applied to `1:N` child tables?
- **Default Recommendation**: Propose explicit pre-aggregating CTEs grouped by the join key prior to the main `LEFT JOIN`.

### Branch C: Financial Semantics & Domain Rules
- **Key Question**: For `measure` columns (`DECIMAL(18,4)`), which transaction statuses qualify for official metric recognition, and how should `NULL` values or partial refunds be handled?
- **Default Recommendation**: Propose strict adherence to the canonical accounting identity in `./knowledge/domain/semantic_rules.md` with `COALESCE(val, 0.0000)`.

### Branch D: PII Classification & Security
- **Key Question**: Are there any new columns containing Personally Identifiable Information (tax ID, email, phone, IP address) requiring salted `SHA-256` hashing or partial masking from Bronze $\rightarrow$ Silver?
- **Default Recommendation**: Propose salted `SHA-256` hashing and `classification: confidential | restricted` metadata in the ODCS contract.

### Branch E: Incremental Strategy, Partitioning & SLA
- **Key Question**: Which column defines physical partitioning in Apache Iceberg, what is the lookback window for late-arriving facts, and what is the maximum acceptable freshness SLA?
- **Default Recommendation**: Propose `incremental_strategy='merge'` partitioned by the accounting date (`order_date`) with a 3-day lookback window and automated Elementary monitors.

## Step 3 — Consolidation & Auditable Materialization via Git PR
Once all branches are resolved:
1. Update (or generate) `./contracts/<asset>_<layer>_v<version>.odcs.yaml` reflecting every decision.
2. If a new reusable domain rule was established, append it to `./knowledge/domain/semantic_rules.md` using the paired `MANDATE:` / `!ANTI_PATTERN:` syntax.
3. Hand off the signed blueprint for implementation by the `worker` subagent.
