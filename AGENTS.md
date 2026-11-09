# AGENTS.md — Canonical Agentic Harness (`de-agentic-boilerplate-olap`)

## 1. Project Identity & Context Architecture
- **Project**: `de-agentic-boilerplate-olap` (Medallion OLAP Lakehouse Profile)
- **Primary Domain**: Enterprise Data Engineering (Multi-Domain Template — E-commerce & FinOps Canonical Reference)
- **Data Stack**: Medallion Lakehouse (Bronze/Silver/Gold), PySpark, dbt Core, Bruin, DuckDB (local dev/CI) & Apache Iceberg (production storage).
- **Catalog & Lineage MCP**: `datahub` (DataHub MCP Server), `elementary-observability` (Elementary MCP), `bruin` (Bruin MCP), `dbt` (dbt Core MCP).
- **Single Source of Truth**: This file (`AGENTS.md`) is the sole canonical runtime specification. Client files (`.claude/CLAUDE.md`, `.gemini/GEMINI.md`) act strictly as lightweight shims importing `@AGENTS.md`.
- **Institutional Memory (OKF)**: All domain semantics, metric formulas, and operational runbooks live in Git-versioned `./knowledge/`. Never rely on unversioned external chat histories or external notebook tools.

## 2. Layer Ownership & Orchestration Scoping
- **MANDATE**: Use **Bruin** (`bruin`) to govern Bronze raw ingestion, cross-technology asset orchestration, Python/PySpark execution, and end-to-end DAG scheduling.
- **!ANTI_PATTERN**: Never use dbt to ingest raw external APIs or manage cross-runtime DAG scheduling outside analytical transformations.
- **MANDATE**: Use **dbt Core** (`dbt`) strictly for Silver and Gold analytical transformations, SQL/PySpark model contracts, and semantic layer definitions using `--select state:modified+`.
- **!ANTI_PATTERN**: Never execute unscoped `dbt run` or `dbt build` across the entire warehouse without state selection (`state:modified+`) or explicit partition predicates.

## 3. Executable Data Contracts (ODCS v3.2.0)
- **MANDATE**: Every Silver and Gold table must have a validated Open Data Contract Standard (ODCS v3.2.0) specification in `./contracts/<asset>_<layer>_v<version>.odcs.yaml` before writing any SQL or PySpark code.
- **!ANTI_PATTERN**: Never create or alter a pipeline schema in dbt or Bruin without first updating and linting its corresponding `./contracts/*.odcs.yaml` file.
- **MANDATE**: Include both the native `context` block (`instructions`, `verifiedStatements`, `constraints`, and property-level `semanticType: measure | dimension`) and `customProperties` for DataHub and Elementary integration.
- **!ANTI_PATTERN**: Never use floating-point types (`FLOAT`, `DOUBLE`, `REAL`) for monetary values or financial metrics; strictly enforce `DECIMAL(18,4)` or integer cents (`BIGINT`).

## 4. Exact Validation & Execution Commands
Run these deterministic checks locally (against DuckDB/Iceberg local catalog) before proposing any PR or completing a task:
1. **Contract Linting**: `datacontract lint ./contracts/orders_silver_v1.odcs.yaml`
2. **Contract Verification**: `datacontract test ./contracts/orders_silver_v1.odcs.yaml --server local_duckdb`
3. **Bruin DAG & Asset Validation**: `bruin validate ./pipelines/`
4. **Bruin Lineage Inspection**: `bruin lineage ./pipelines/orders_silver/asset.py --full`
5. **dbt Compilation & Contract Check**: `dbt compile --select state:modified+ --state ./target/prod_manifest`
6. **dbt Selective Build & Quality Test**: `dbt build --select state:modified+ --defer --state ./target/prod_manifest`
7. **Elementary Anomaly & Schema Drift Check**: `edr monitor --profiles-dir ~/.dbt --project-dir ./dbt_project`
8. **Knowledge Base Search**: `rg -i "<term>" ./knowledge/ ./contracts/`

## 5. Subagent Routing & Native Multi-Agent Orchestration
- **MANDATE**: In Google Antigravity, the main chat agent acts strictly as the **Orchestrator (Maestro)**. Never roleplay a subagent in the main chat thread; always register and spawn isolated subagent instances using the native `define_subagent` and `invoke_subagent` tools loaded from `.claude/agents/*.md`.
- **!ANTI_PATTERN**: Never execute `"Act as..."` persona roleplay in the main context window when `invoke_subagent` is available, as doing so defeats context isolation and least-privilege scoping.
- **`planner` (`.claude/agents/planner.md`)**: Read-only Architect (`EnableWriteTools: false` for production code). Runs on Flash 3.8 (`model: flash`). Produces technical specs and ODCS drafts. Pauses at the Human Specification Gate (`🛑`).
- **`worker` (`.claude/agents/worker.md`)**: Pipeline/Application Developer (`EnableWriteTools: true`). Runs on Flash 3.8 (`model: flash`) by default. Spawned automatically by the Orchestrator as soon as the human replies `APPROVED` to the planner's spec.
- **`auditor` (`.claude/agents/auditor.md`)**: Data Quality & Contract Auditor (`EnableWriteTools: false`). Runs on Flash 3.8 (`model: flash`). Spawned automatically by the Orchestrator right after `worker` finishes to execute all quality gates and issue `PASS` or `BLOCK`.
- **`dataops` (`.claude/agents/dataops.md`)**: Incident & Self-Healing Triage. Runs on Flash 3.8 (`model: flash`). Follows `./knowledge/runbooks/` decision trees to diagnose failures and codify post-mortems via `./skills/post-incident-learn.md`.

### 5.1 Weekly Pro Quota Governance & Surgical Elevation Protocol (Flash-First)
- **Flash-First Foundation**: The Main Chat (Maestro/Orchestrator), `planner`, `worker` (by default), `auditor`, and `dataops` run strictly on **Gemini 3.8 Flash** (`model: flash`, `thinkingEffort: low`). This handles large context exploration, boilerplate code generation, schemas, and test loops with zero threat to the user's weekly Pro plan quota (168h window).
- **Surgical Escalation to Argon 4 Low**: The Maestro ONLY elevates `worker` to **Argon 4 Low** (`Model: 'pro'`, `thinkingEffort: low`) when:
  1. **The 2-Failure Fallback Rule**: Worker in Flash failed 2 consecutive build/test loops on the exact same compiler or contract test error.
  2. **Critical Algorithmic Logic**: Complex mathematical reconciliation formulas, recursive CTE graph traversals, or bitwise state-hash diff engines.
- **!ANTI_PATTERN**: Never route routine file scaffolding, directory creation, or standard CRUD/SQL modeling to Argon 4, preventing premature weekly Pro plan quota exhaustion.

## 6. Domain & Engineering Paired Constraints
- **MANDATE**: Write all pipelines to be strictly idempotent using `MERGE INTO` on deterministic natural/surrogate keys or `INSERT OVERWRITE` scoped by partition (`order_date`).
- **!ANTI_PATTERN**: Never use blind `INSERT INTO` without partition cleanup or deduplication (`QUALIFY ROW_NUMBER() OVER (...) = 1`), as retries will silently duplicate rows.
- **MANDATE**: Explicitly verify JOIN cardinality (`1:1`, `N:1`) before joining tables in Silver/Gold; pre-aggregate child tables to the target grain prior to joining.
- **!ANTI_PATTERN**: Never join header tables (`orders`) directly with unaggregated line-item or payment-attempt logs (`order_items`, `payment_attempts`), which causes silent metric fan-out.
- **MANDATE**: Mask, hash (SHA-256 with salt), or tokenize all PII fields (`customer_email`, `tax_id`, `ip_address`) upon transition from Bronze to Silver.
- **!ANTI_PATTERN**: Never expose cleartext PII in Silver/Gold tables, logs, exception tracebacks, or LLM prompts.
- **MANDATE**: Consult `./knowledge/runbooks/INCIDENT_TEMPLATE.md` deterministically before invoking open-ended generative troubleshooting during pipeline incidents.
- **!ANTI_PATTERN**: Never apply ad-hoc hotfixes (`ALTER TABLE`, manual `DELETE`) directly in production catalogs without backporting to `./contracts/` and git history.

## 7. Three-Tier Operational Boundaries

### ALWAYS DO (Autonomous Execution)
- Read `./knowledge/domain/semantic_rules.md` and target `./contracts/*.odcs.yaml` before proposing or modifying any SQL/PySpark transformation.
- Run `datacontract lint`, `bruin validate`, and `dbt compile` after every code modification.
- Scope all dbt executions with `--select state:modified+` and `--defer` pointing to the canonical manifest.
- Enforce partition pruning predicates on partitioned Iceberg tables in every analytical query.
- Invoke `./skills/grilling-architecture.md` when business grain, SLA, or PII classification is ambiguous.

### ASK FIRST (Human-in-the-Loop Gate)
- Introducing breaking schema changes (dropping columns, renaming fields, or narrowing data types) in Silver or Gold contracts.
- Executing historical backfills or full-refresh operations (`dbt run --full-refresh` or `bruin run --full-refresh`) on tables exceeding 10M rows or 50 GB.
- Modifying canonical financial metric formulas (`net_revenue_amount`, `gross_merchandise_value`) in `./knowledge/domain/semantic_rules.md`.
- Promoting quarantined rows (`_quarantine` tables) back into production Silver tables after an incident.

### NEVER DO (Hard Architectural Guardrails)
- **NEVER** execute `DROP TABLE`, `TRUNCATE TABLE`, or unpartitioned `DELETE FROM` against production or staging Iceberg catalogs.
- **NEVER** bypass failing `datacontract test` or `elementary` quality gates by disabling tests or lowering severity thresholds without a signed Architecture Decision Record (ADR).
- **NEVER** store credentials, API tokens, or raw connection strings in `.agents/mcp_config.json`, `AGENTS.md`, or pipeline files (use environment variables exclusively).
- **NEVER** create JSON subagent definitions (`.claude/agents/*.json`) or duplicate instructions across `.claude/CLAUDE.md` and `.gemini/GEMINI.md`.

## 8. Multi-Agent Handoff Protocol (Deterministic Workflow)
1. **Phase A — Discovery & Spec (`planner`)**: Read `./knowledge/domain/semantic_rules.md` and `datahub` MCP lineage. If requirements are ambiguous, execute `./skills/grilling-architecture.md`. Output a verified ODCS v3.2.0 contract draft.
2. **Phase B — Implementation (`worker`)**: Operate in an isolated git worktree (`isolation: worktree`). Implement Bronze ingestion in Bruin and Silver/Gold transformations in dbt Core (`--select state:modified+`). Validate locally against DuckDB (`local_duckdb`).
3. **Phase C — Verification Gate (`auditor`)**: Execute `datacontract lint`, `datacontract test`, `bruin validate`, `bruin lineage`, `dbt compile`, and `dbt test`. Cross-check `elementary-observability` and `datahub` blast radius. Issue a deterministic `PASS` or `BLOCK` verdict.
4. **Phase D — Triage & Self-Healing (`dataops`)**: On production failure or SLA breach, follow the 6-step decision tree in `./knowledge/runbooks/INCIDENT_TEMPLATE.md` and codify permanent contract/runbook guardrails via `./skills/post-incident-learn.md`.

## 9. Canonical Repository Pointers
- **Data Contracts (ODCS v3.2.0)**: `./contracts/orders_silver_v1.odcs.yaml`
- **Domain Semantics (OKF)**: `./knowledge/domain/semantic_rules.md`
- **Operational Runbooks (OKF)**: `./knowledge/runbooks/INCIDENT_TEMPLATE.md`
- **Architecture Interview Skill**: `./skills/grilling-architecture.md`
- **Post-Incident Learning Skill**: `./skills/post-incident-learn.md`
- **MCP Server Registry**: `.agents/mcp_config.json`
- **Subagent Definitions**: `.claude/agents/{planner,worker,auditor,dataops}.md`
- **Client Shims**: `.claude/CLAUDE.md` and `.gemini/GEMINI.md`

