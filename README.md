# Comex Data Engine V2 — Multi-Model HTAP Fiduciary Ledger, Cross-Border FX & Brazilian Customs Cascading Tax Engine

[![ODCS v3.2.0](https://img.shields.io/badge/Data_Contracts-ODCS_v3.2.0-0A66C2?style=flat-square)](./contracts/comex_fiduciary_engine_v2.odcs.yaml)
[![Python 3.12](https://img.shields.io/badge/Python-3.12+-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688?style=flat-square&logo=fastapi&logoColor=white)](./src/api/main.py)
[![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL_16-ACID_Fiduciary_Ledger-336791?style=flat-square&logo=postgresql&logoColor=white)](./sql/ddl/01_schema_fiduciary_ledger.sql)
[![MongoDB 7](https://img.shields.io/badge/MongoDB_7-NoSQL_Landing_Zone-47A248?style=flat-square&logo=mongodb&logoColor=white)](./playgrounds/quickstart_comex.mongodb.js)
[![DuckDB | Parquet](https://img.shields.io/badge/OLAP_Lakehouse-DuckDB_%7C_Parquet_Hive-FFF000?style=flat-square&logo=duckdb&logoColor=black)](./src/connectors/lakehouse.py)

**Comex Data Engine V2** is an enterprise-grade **Hybrid Transactional/Analytical Processing (HTAP)** and **Fiduciary Accounting Engine** purpose-built for international trade (**COMEX**), multi-currency foreign exchange reconciliation (**CHF, EUR, USD, BRL**), **ISO 20022 SWIFT (`camt.053`)** wire settlement, and **Brazilian Customs Nationalization (*Cálculo de Impostos Aduaneiros em Cascata & CPC 16 / IAS 2*)**.

Engineered under a **Spec-Driven Data Engineering** paradigm, every ingestion pipeline, accounting entry, and analytical transformation is governed by executable **Open Data Contract Standard (ODCS v3.2.0)** specifications, paired domain mandates, **Alembic** versioned migrations, strict **No-Float (`Decimal`)** arithmetic, database-level **PL/pgSQL immutability & zero-sum constraint triggers**, authenticated **FastAPI V2** security controls, and a closed-loop **Human-in-the-Loop (HITL) Quarantine** workflow.

---

## 1. Executive Summary & Architectural Paradigm Shift

Traditional foreign trade and cross-border bookkeeping architectures fail across two opposing fault lines:
1. **Purely Analytical Batch Pipelines (Legacy ETL)**: Ingest raw foreign trade CSVs (such as Brazilian **MDIC Comex Stat**) using IEEE 754 binary floating-point types (`float` / `DOUBLE PRECISION`) that silently corrupt multi-currency reconciliations by fractions of a cent, while ignoring the transactional lifecycle of **Commercial Invoices**, **Incoterms 2020**, and **SWIFT** bank settlements.
2. **Purely Transactional CRUD Systems (Isolated ERPs)**: Mutate account balances destructively in place (`UPDATE accounts SET balance = ...`), erasing statutory audit trails, failing to segregate competence vs. cash foreign exchange variance, and lacking columnar OLAP throughput to analyze millions of Harmonized System (**HS / NCM**) customs declarations.

**Comex Data Engine V2** bridges both worlds via a **Multi-Model HTAP & Fiduciary Medallion Architecture**:

| Architectural Dimension | Conventional Trade Pipeline (Legacy ETL) | Comex Resilient Data & Fiduciary Engine V2 (HTAP) |
| :--- | :--- | :--- |
| **Workload Topology** | Isolated CSV batch scripts on disk | **Tri-Store HTAP**: **MongoDB 7 (Authenticated)** Raw BSON Landing + **PostgreSQL 16** ACID Double-Entry Ledger + **DuckDB / Parquet Hive** Vectorized OLAP Star Schema |
| **Monetary & FX Arithmetic** | Binary floating-point (`float` IEEE 754) | **Strict Anti-Float Shield**: `Decimal("0.0001")` (`NUMERIC(19,4)`) for currency, `Decimal("0.000001")` (`NUMERIC(19,6)`) for FX rates, and `BIGINT` `*_cents` `GENERATED ALWAYS` columns |
| **Idempotency Guarantee** | Blind file overwrite or duplicate inserts | **Triple-Barrier Idempotency `[f(f(x)) = f(x)]`**: Edge `SHA-256` payload registry + PostgreSQL composite natural-key `ON CONFLICT` upsert + deterministic `uuid5` / `operation_hash_id` |
| **Bookkeeping & Audit Trail** | Destructive `UPDATE` / `DELETE` on balances | **Append-Only Double-Entry Ledger**: Zero-sum deferred constraint triggers (`SUM(Debit) - SUM(Credit) == 0.0000`), `TRUNCATE` block triggers, and statutory **Reversal Entries** |
| **Cross-Border Tax & FX Rules** | Hardcoded flat percentages, missing weekend rates | **BACEN Olinda PTAX** with **LOCF Forward-Fill** (`date_id <= target_date_id`), **Swiss UID (`CHE-MWST`)** & **Brazilian CNPJ (Modulo-11)** validation, and **Cascading Customs Taxes (`ICMS "Por Dentro"` & `CPC 16 / IAS 2`)** |
| **Network & Extraction Resilience** | Naive fixed-interval retries or silent stale fallbacks | **Tenacity Exponential Backoff + Full Jitter** (4xx fail-fast), strict `offline_fallback=False` default, + **Bounded 3-Iteration Agentic Self-Correction Loop** |
| **Exception & Anomaly Handling** | Silent data truncation or pipeline crash (`exit 1`) | **Closed-Loop HITL Quarantine (`comex_quarantine_hitl`)**: Immutable finite state machine (`PENDING_REVIEW` to `APPROVED_OVERRIDE` or `REJECTED_HITL`) with `X-API-Key` auditor auth and hashed `reviewed_by` |

---

## 2. End-to-End 5-Layer HTAP Architecture

```mermaid
flowchart TD
    subgraph Sources["1. External Cross-Border Sources"]
        S1["MDIC Comex Stat Open Data"]
        S2["BACEN Olinda Official PTAX API"]
        S3["Commercial Invoices and SWIFT camt.053"]
    end

    subgraph Ingestion["2. Resilient Ingestion and Landing (MongoDB 7 Auth)"]
        ING["Resilient Extractors<br/>Tenacity Jitter and Strict Offline Guard"]
        API["FastAPI V2 Gateway<br/>X-API-Key + slowapi Rate Limit"]
        SHA["SHA-256 Edge Idempotency Registry"]
        MGO["MongoDB 7 comex_raw_lake<br/>Raw Invoices, SWIFT and PTAX BSONs"]
        LOOP["Agentic Self-Correction Loop<br/>Up to 3x Pydantic Coercion Retries"]
    end

    subgraph Validation["3. Shift-Left Contracts (ODCS v3.2.0)"]
        PYD["Pydantic V2 Contract Guards<br/>Zero-Float, Modulo-11 CNPJ and Incoterms 2020"]
        HITL["HITL Quarantine Queue<br/>comex_quarantine_hitl (REJECTED_HITL / APPROVED_OVERRIDE)"]
    end

    subgraph Fiduciary["4. Fiduciary OLTP Engine (PostgreSQL 16 ACID)"]
        UPS["Atomic Upsert Engine<br/>Composite Natural Key + Competence PTAX"]
        TAX["Customs and Cascading Tax Engine<br/>ICMS Por Dentro and CPC 16"]
        LED["Immutable Double-Entry Ledger<br/>Triggers: Append-Only, Zero-Sum and No TRUNCATE"]
    end

    subgraph Analytics["5. Vectorized OLAP Lakehouse (DuckDB + Parquet)"]
        PARQ["Parquet Hive Lakehouse<br/>Partitioned by year and month (Atomic Merge)"]
        DUCK["DuckDB Star Schema<br/>fact_comex_operations and dim_currency_ptax"]
    end

    S1 --> ING
    S2 --> ING
    S3 --> API
    API --> MGO
    MGO --> SHA
    SHA -->|New Payload| LOOP
    SHA -->|REJECTED_HITL Replay| HITL
    LOOP --> PYD
    ING --> PYD
    PYD -->|Valid Payload| UPS
    PYD -->|Math Drift or Schema Error| HITL
    HITL -->|APPROVED_OVERRIDE + AUDITOR_API_KEY| UPS
    UPS --> TAX
    TAX --> LED
    PYD --> PARQ
    TAX --> PARQ
    PARQ --> DUCK
    LED --> DUCK
```

---

## 3. Deep-Dive into Core Engineering Pillars

### 3.1 Strict Anti-Float Monetary Precision & Cross-Border Fiscal Validation
In international trade and fiduciary accounting, binary floating-point numbers (`float` IEEE 754, where `0.1 + 0.2 == 0.30000000000000004`) are strictly forbidden because they introduce penny drift across multi-million-dollar currency conversions and cascading tax allocations.

- **Anti-Float Guardrail ([`src/models/_validators.py`](./src/models/_validators.py), [`src/models/invoices.py`](./src/models/invoices.py), [`src/models/customs.py`](./src/models/customs.py), [`src/models/quarantine.py`](./src/models/quarantine.py))**: Rejects `None` on required fields, `bool`, `float`, `NaN`, `Infinity`, malformed formatted strings (e.g., `"1,250"`), and PostgreSQL `NUMERIC(19,4)` / `NUMERIC(19,6)` / `BIGINT` scaled-cents overflow bounds. Monetary values quantize to `Decimal("0.0001")` (`ROUND_HALF_UP`) and mirror as PostgreSQL `BIGINT` `*_cents` `GENERATED ALWAYS AS (...) STORED` columns. Exchange rates quantize to `Decimal("0.000001")`.
- **Horizontal & Vertical Invoice Math Closure**:
  ```text
  Horizontal Line Math:    ROUND(quantity * unit_price, 4) == total_price
  Line Aggregation:        SUM(line.total_price) == subtotal_amount
  Vertical Header Closure: subtotal_amount + tax_amount + freight_amount + insurance_amount == total_amount
  ```
- **Incoterms 2020 & CIF Protection**: Under `CIF` (*Cost, Insurance, and Freight*), explicit strictly positive `freight_amount > 0` AND `insurance_amount > 0` are mandatory across both `CommercialInvoiceInput` and `CustomsNationalizationCalcInput` as well as PostgreSQL `chk_invoice_cif_freight_insurance` to prevent customs under-valuation (*Subfaturamento de Valor Aduaneiro*).
- **Transnational Tax ID & PII Governance**:
  - **Cleartext B2B Corporate Tax IDs**: Enforces full **Modulo-11 check digits** on 14-digit **Brazilian CNPJ** and strict structural validation on **Swiss Corporate UID** (e.g., `CHE-116.281.710 MWST`), keeping corporate IDs in cleartext for customs traceability.
  - **Salted SHA-256 Natural-Person PII**: Hashes personal contact fields in MongoDB raw landing (`contact_email`, `cpf`, etc.) and auditor identities (`reviewed_by` in `comex_quarantine_hitl`) using `hash_natural_person_pii` with mandatory `PII_HASH_SALT` before persistence or API response.

---

### 3.2 Append-Only Double-Entry Fiduciary Ledger & Database Triggers
All validated financial events (invoice recognition using competence-date PTAX, customs nationalization, and SWIFT bank settlement) are recorded in PostgreSQL 16 ([`sql/ddl/01_schema_fiduciary_ledger.sql`](./sql/ddl/01_schema_fiduciary_ledger.sql)):

- **Frozen Ledger DTOs**: `JournalLineInput` and `JournalEntryInput` are `frozen=True` Pydantic models with strict balance and reversal guards.
- **Database-Enforced Immutability & Deferred Zero-Sum Triggers**:
  - **`fn_enforce_append_only_ledger()`**: Bound via `trg_enforce_append_only_lines` (`journal_lines` `UPDATE`/`DELETE`), `trg_enforce_append_only_entries` (`journal_entries` `UPDATE`/`DELETE` — allowing strictly the single-field transition `POSTED -> REVERSED` with valid non-self `reversed_from_entry_id`, while locking `REVERSED` rows permanently), `trg_block_truncate_journal_lines`, and `trg_block_truncate_journal_entries`.
  - **`fn_enforce_double_entry_zero_sum()`**: Deferred constraint triggers (`trg_check_journal_entry_balance`, `trg_check_journal_lines_balance` `DEFERRABLE INITIALLY DEFERRED`) enforcing `SUM(debit_amount) - SUM(credit_amount) == 0.0000` and `>= 2` lines per entry at transaction commit.
  - **`fn_enforce_quarantine_hitl_immutability()`**: Bound via `trg_enforce_quarantine_hitl_immutability` and `trg_block_truncate_quarantine_hitl`, making finalized HITL resolutions (`APPROVED_OVERRIDE` and `REJECTED_HITL`) permanently immutable at the database level.
- **Statutory Reversals (*Estorno Contábil*)**: Corrections occur exclusively via `create_reversal_entry()`, posting a new compensating `journal_entry` with inverted debit/credit legs.

---

### 3.3 Live BACEN Olinda PTAX Connector, LOCF Forward-Fill & SWIFT `camt.053` Reconciliation
Implemented in [`src/extractors/bacen_ptax_client.py`](./src/extractors/bacen_ptax_client.py), [`src/extractors/http_client.py`](./src/extractors/http_client.py), and [`src/models/customs.py`](./src/models/customs.py):

- **Resilient Central Bank Extraction & Strict Offline Guard**: Queries the **BACEN Olinda OData API** (`CotacaoMoedaPeriodo`) using `ResilientAPIExtractor` (`tenacity` exponential backoff with jitter on transient HTTP 429/5xx/network faults; immediate fail-fast on non-retryable HTTP 4xx such as 400/404). `BacenPTAXClient` defaults to `offline_fallback=False` (fails loudly when live API is unreachable); `offline_fallback=True` serves [`samples/bacen_ptax_sample.json`](./samples/bacen_ptax_sample.json) strictly within temporal sample bounds.
- **Zero-Lookahead LOCF Forward-Fill & Competence FX**: Weekend and holiday gaps are filled strictly backward-only (`apply_locf_forward_fill`, `is_interpolated_locf = TRUE`), and runtime invoice/MDIC PTAX resolution executes `WHERE date_id <= target_date_id ORDER BY date_id DESC LIMIT 1`. Non-BRL invoices without a prior PTAX rate fail explicitly with `MissingPTAXRateError` (`HTTP 422`).
- **Persisted & Idempotent SWIFT Fee Split (`build_swift_fee_split_entry` / `reconcile_swift_settlement`)**:
  - Persists raw statements into MongoDB `raw_swift_statements` + balanced PostgreSQL `journal_entries` & `journal_lines` + `ingestion_idempotency_registry` keyed by deterministic `uuid5`. Replays return `200 DUPLICATE_IGNORED` (`idempotent_replay: true`) with zero extra ledger rows.
  - Supports `fee_amount > 0` (**3-legged** split: Debit `1.1.1.01` Cash Net, Debit `4.1.2.01` SWIFT Fee, Credit `1.1.2.01` AR Gross) and zero-fee `OUR` wires (`fee_amount == 0`, **2-legged** balanced entry).

---

### 3.4 Brazilian Customs Cascading Tax Engine & CPC 16 (IAS 2) Inventory Valuation
Implemented in [`src/models/customs.py`](./src/models/customs.py) (`compute_brazilian_landed_cost` & `build_import_nationalization_ledger_entry`):

```text
1. Customs Value (VA):   VA_BRL     = ROUND((FOB + Freight + Insurance) * PTAX_sell, 4)
2. Import Duty (II):      II_BRL     = ROUND(VA_BRL * Aliq_II, 4)
3. Industrial Tax (IPI):  IPI_BRL    = ROUND((VA_BRL + II_BRL) * Aliq_IPI, 4)
4. PIS Importação:        PIS_BRL    = ROUND(VA_BRL * Aliq_PIS, 4)
5. COFINS Importação:     COFINS_BRL = ROUND(VA_BRL * Aliq_COFINS, 4)
6. ICMS Base (Gross-Up):  Base_ICMS  = ROUND((VA_BRL + II_BRL + IPI_BRL + PIS_BRL + COFINS_BRL + Siscomex + AFRMM) / (1 - Aliq_ICMS), 4)
7. ICMS Importação:       ICMS_BRL   = ROUND(Base_ICMS * Aliq_ICMS, 4)
8. Total Taxes:           Taxes_BRL  = II_BRL + IPI_BRL + PIS_BRL + COFINS_BRL + Siscomex + AFRMM + ICMS_BRL
9. Total Landed Cost:     Landed_BRL = VA_BRL + Taxes_BRL
```

- **Tax Regime Segregation (`VALID_TAX_REGIMES`)**:
  - **`LUCRO_REAL` (Non-Cumulative Regime)**: 4-legged CPC 16 journal entry capitalizing non-recoverable duties (`VA + II + Siscomex + AFRMM`) into inventory (`1.1.3.02`) and segregating recoverable taxes (`IPI + PIS + COFINS + ICMS`) into `1.1.4.01` (*Tributos a Recuperar*).
  - **`LUCRO_PRESUMIDO`, `SIMPLES_NACIONAL`, and `CUSTO_INTEGRAL`**: Non-crediting regimes that capitalize 100% of cascading taxes directly into inventory (`1.1.3.02`) via a 3-legged balanced journal entry (`recoverable_taxes_brl = 0.0000`).

---

### 3.5 Bulk MDIC Comex Stat Streaming & Partition-Safe Parquet Lakehouse
Implemented in [`src/extractors/mdic_bulk_loader.py`](./src/extractors/mdic_bulk_loader.py) and [`src/connectors/lakehouse.py`](./src/connectors/lakehouse.py):
- Streams official MDIC CSVs (`IMP_YYYY.csv` / `EXP_YYYY.csv`, 11-column and 12-column `CO_UNID` layouts), pre-aggregates by the 8-key natural tuple `(ano|mes|tipo_op|co_ncm|co_pais|sg_uf|co_via|co_urf)`, computes pipe-delimited `SHA-256` `operation_hash_id`, and resolves PTAX from DuckDB without lookahead bias (`date_id <= target_date_id`).
- **Atomic Partition Merge & DuckDB Scoping**: `write_records_to_parquet_hive` merges/deduplicates existing partition Parquet data via file-locked atomic replacement so sequential `FLOW=IMPORT` and `FLOW=EXPORT` loads preserve both flows without clobbering partitions; `duckdb_connection()` guarantees closure on exit.

---

### 3.6 Closed-Loop HITL Quarantine & Audited Governance
Implemented in [`src/pipeline/comex_processor.py`](./src/pipeline/comex_processor.py):
- **Finite State Machine (`comex_quarantine_hitl`)**: Invoices failing math closure, Incoterm CIF rules, or PTAX lookup after the bounded self-correction loop enter `PENDING_REVIEW`.
- **Privileged Resolution (`POST /api/v2/quarantine/{id}/resolve`)**: Protected by `AUDITOR_API_KEY` (`HTTP 401/403` otherwise) and locked via `SELECT ... FOR UPDATE`.
  - **`APPROVED_OVERRIDE`**: Re-validates `corrected_payload`, resolves competence PTAX, atomically posts to `commercial_invoices` + double-entry ledger, hashes `reviewed_by` with `PII_HASH_SALT`, and promotes `ingestion_idempotency_registry` to `RESOLVED_OVERRIDE`.
  - **`REJECTED_HITL`** (input alias `REJECTED` normalized to `REJECTED_HITL`): Permanently rejects the quarantine record and marks `ingestion_idempotency_registry` as `REJECTED_HITL`; subsequent ingestion replays return `HTTP 409 Conflict`.

---

## 4. Repository Structure

```text
comex-data-engine-v2/
├── .env.example                                     # Environment template (PG 5434, Mongo 27018 auth, API Keys, PII Salt)
├── .github/workflows/ci.yml                         # CI: healthy Postgres+Mongo, uv sync --frozen, Ruff, ODCS lint, Alembic, Seed x2, SQL, Pytest (0 skipped)
├── alembic.ini                                      # Alembic configuration reading DATABASE_URL
├── alembic/
│   ├── env.py                                       # Alembic migration runner environment
│   ├── script.py.mako
│   └── versions/
│       └── 0001_initial_schema.py                   # Initial revision applying sql/ddl/01 & sql/ddl/02
├── contracts/
│   └── comex_fiduciary_engine_v2.odcs.yaml          # Executable ODCS v3.2.0 contract
├── Dockerfile                                       # Python 3.12-slim non-root container image
├── docker-compose.yml                               # PostgreSQL 16 (5434) + MongoDB 7 auth (27018) + Comex API (8000)
├── knowledge/
│   ├── domain/
│   │   ├── architecture_raw.md                      # HTAP architectural report
│   │   └── semantic_rules.md                        # Paired MANDATE / !ANTI_PATTERN domain invariants
│   └── runbooks/
│       ├── INCIDENT_TEMPLATE.md                     # Deterministic P0-P3 triage template
│       └── INC-2026-11-10-001_quarantine_triage.md  # Quarantine HITL post-incident RCA
├── Makefile                                         # CLI targets (setup, up, migrate, lint, seed, test, contract-lint, verify, run-api)
├── playgrounds/
│   └── quickstart_comex.mongodb.js                  # Authenticated MongoDB Playground script (port 27018)
├── pyproject.toml                                   # uv dependencies, Ruff & Pytest config
├── samples/                                         # Deterministic offline seed datasets
│   ├── bacen_ptax_sample.json
│   ├── comex_mdic_sample.csv
│   ├── commercial_invoices_batch.json
│   ├── customs_nationalizations_sample.json
│   ├── mdic_exp_sample.csv
│   ├── mdic_imp_sample.csv
│   ├── mdic_ncm_sample.csv
│   ├── mdic_pais_sample.csv
│   └── swift_camt053_statements.json
├── skills/
│   ├── grilling-architecture.md
│   └── post-incident-learn.md
├── sql/
│   ├── ddl/
│   │   ├── 01_schema_fiduciary_ledger.sql           # ACID Ledger, Triggers, *_cents GENERATED ALWAYS, customs_nationalizations
│   │   └── 02_schema_comex_star.sql                 # Star Schema: fact_comex_operations & conformed dimensions
│   └── queries/
│       ├── 00_init_duckdb_views.sql                 # DuckDB views over Parquet Hive partitions
│       ├── 01_audit_trial_balance_zero_sum.sql      # General Ledger zero-sum trial balance
│       ├── 02_sabotage_test_append_only_trigger.sql # Rolled-back PL/pgSQL trigger sabotage test
│       ├── 03_fx_and_swift_reconciliation_audit.sql # PTAX LOCF & SWIFT fee-split inspection
│       ├── 04_analytical_landed_cost_by_ncm.sql     # Star Schema OLAP query by NCM & modal
│       ├── 05_quarantine_hitl_forensics.sql         # Quarantine HITL error forensics
│       ├── 06_brazilian_cascading_taxes_simulation.sql # Cascading tax & ICMS por dentro audit
│       └── 07_hitl_quarantine_resolution_audit.sql  # End-to-end HITL promotion audit
├── src/
│   ├── __init__.py
│   ├── api/
│   │   ├── __init__.py
│   │   └── main.py                                  # Hardened FastAPI V2 Gateway (X-API-Key, slowapi, /health, /metrics)
│   ├── connectors/
│   │   ├── __init__.py
│   │   ├── lakehouse.py                             # Thread/file-safe Parquet Hive merge writer & DuckDB context manager
│   │   ├── mongo.py                                 # Authenticated PyMongo client (port 27018, authSource=admin)
│   │   └── postgres.py                              # SQLAlchemy 2.0 pool for PostgreSQL 16 (port 5434)
│   ├── extractors/
│   │   ├── __init__.py
│   │   ├── bacen_ptax_client.py                     # Olinda PTAX client + LOCF + explicit offline guard
│   │   ├── http_client.py                           # ResilientAPIExtractor (4xx fail-fast) & canonical SHA-256 hash
│   │   └── mdic_bulk_loader.py                      # MDIC bulk CSV loader + 8-key hash + DuckDB PTAX no-lookahead
│   ├── models/
│   │   ├── __init__.py
│   │   ├── _validators.py                           # Shared anti-float, NaN/Inf/bool/overflow validators
│   │   ├── canonical.py                             # Base canonical V2 schema
│   │   ├── customs.py                               # Frozen Ledger DTOs, SWIFT split, LOCF, Tax Cascade & CPC 16
│   │   ├── invoices.py                              # CommercialInvoiceInput (CNPJ Mod-11, Swiss UID, CIF, PII hashing)
│   │   └── quarantine.py                            # QuarantineRecordInput & REJECTED_HITL enum governance
│   ├── pipeline/
│   │   ├── __init__.py
│   │   └── comex_processor.py                       # ComexHTAPProcessor (Mongo landing, self-correction, ACID Ledger, SWIFT, HITL)
│   └── seed.py                                      # Idempotent Alembic + multi-model seed runner
└── tests/
    ├── conftest.py                                  # Isolated FakeHTAPBackend fixtures & env bootstrap
    ├── test_canonical_htap.py                       # V2 unit suite (8 tests): V1 retirement, mocked /health, retries
    ├── test_comex_fiduciary_v2.py                   # Fiduciary unit suite (29 tests): contracts, PTAX, MDIC, tax, API, HITL
    ├── test_live_htap.py                            # Live integration suite (7 tests, 0 skips): PG/Mongo/DuckDB, triggers, API, Seed x2
    ├── test_wave1_models_connectors.py              # Security & precision hardening suite (7 tests)
    └── test_wave2_api_pipeline_seed.py              # API auth/rate-limit/metrics/SWIFT/no-lookahead/seed x2 suite (9 tests)
```

---

## 5. Quickstart & Deterministic CLI Execution

### Prerequisites
- Linux / macOS with **Python 3.12+**, [`uv`](https://docs.astral.sh/uv/), and **Docker Compose**.
- Copy `.env.example` to `.env` (rotate `API_KEY`, `AUDITOR_API_KEY`, and `PII_HASH_SALT` before any shared/staging/production deployment).

### Step 1: Install Dependencies, Start Containers & Run Migrations
```bash
# Synchronize locked virtual environment (.venv)
make setup

# Launch PostgreSQL 16 (port 5434), MongoDB 7 auth (port 27018), and API container
make up
make ps

# Apply versioned PostgreSQL schema & PL/pgSQL triggers via Alembic
make migrate
```

### Step 2: Seed All 3 Storage Engines (Idempotent x2) & Inspect Live Telemetry
```bash
make seed
```
**Verified Telemetry Output (`make seed`)**:
```text
======================================================================
 📊 RESUMO DE TELEMETRIA DO BANCO (DBeaver / MongoDB / DuckDB)
======================================================================
 🐘 PostgreSQL commercial_invoices:         6 registros (Validados & Desembaraçados)
 🐘 PostgreSQL journal_entries:            11 lançamentos contábeis
 🐘 PostgreSQL journal_lines:              29 partidas dobradas
 🐘 PostgreSQL Soma Zero Invariante:    0.0000 (Zero Absoluto)
 🏛️ PostgreSQL customs_nationalizations:    3 declarações (Cascata & CPC 16)
 🚨 PostgreSQL comex_quarantine_hitl:       2 divergências (1 PENDING, 1 APPROVED_OVERRIDE)
 💱 PostgreSQL dim_currency_ptax:          18 cotações diárias (com LOCF)
 🚢 PostgreSQL fact_comex_operations:       4 células aduaneiras
 🍃 MongoDB raw_commercial_invoices:        7 BSONs brutos
 🦆 DuckDB fact_comex_operations:           4 linhas Parquet Hive
======================================================================
```
*(Re-running `make seed` adds `0` duplicate rows across every table, collection, and Parquet partition.)*

### Step 3: Run Extractors (BACEN PTAX & MDIC Comex Stat)
```bash
# Synchronize USD, EUR, and CHF exchange rates with LOCF weekend forward-fill (or add --offline)
make fetch-ptax START=2026-11-05 END=2026-11-10 CURRENCIES=USD,EUR,CHF

# Stream and pre-aggregate MDIC trade microdata into Parquet Hive & PostgreSQL
make fetch-mdic YEAR=2024 MONTH=1 FLOW=BOTH OFFLINE=true
```

### Step 4: Execute Lint, Contracts & Pytest Verification
```bash
# Run full HTAP verification (lint, ODCS contract-lint, full 60-test pytest suite)
make verify

# Or run unit (53 tests) and live integration (7 tests) targets separately:
make test-unit
make test-integration
```

---

## 6. FastAPI V2 REST API Reference (`http://localhost:8000/docs`)

Start the API server locally with `make run-api` (`uvicorn src.api.main:app`). Legacy V1 routes (`/api/v1/ingest`, `/api/v1/ingest/pipeline`) and `HTAPProcessor` are permanently retired (`HTTP 404`).

### Authentication, Rate Limiting & Observability
- **API Key Authorization (`X-API-Key`)**:
  - Standard ingestion and read routes require a valid `API_KEY` or `AUDITOR_API_KEY` (`HTTP 401` if missing/invalid/unconfigured).
  - Privileged auditor operations (`POST /api/v2/quarantine/{quarantine_id}/resolve` and `POST /api/v2/analytics/landed-cost-sim?persist=true`) strictly require `AUDITOR_API_KEY` (a valid standard `API_KEY` receives `HTTP 403 Forbidden`).
- **Rate Limiting & Tracing**: Protected endpoints enforce per-IP throttling via `slowapi` (`RATE_LIMIT_PER_MINUTE`, returning `HTTP 429`). Every response includes an `X-Correlation-ID` header paired with structured JSON `structlog` logs and sanitized error payloads (zero raw credentials or stack traces).
- **Exempt Operational Probes**: `GET /health` and `GET /metrics` are unauthenticated and `@limiter.exempt`.

| Method & Route | Auth Role | Purpose & Contract Guarantees |
| :--- | :--- | :--- |
| `GET /health` | None (`@limiter.exempt`) | Live probes PostgreSQL (`SELECT 1`), MongoDB (`admin.command("ping")`), and DuckDB/Parquet R/W; returns `200 healthy` or `503 degraded`. |
| `GET /metrics` | None (`@limiter.exempt`) | Exposes request/error totals, status counts, and latency metrics (`?format=json` default or `?format=prometheus`). |
| `POST /api/v2/invoices/ingest` | `API_KEY` / `AUDITOR_API_KEY` | Lands raw BSON in MongoDB (natural-person PII hashed), checks edge SHA-256 registry (`200 DUPLICATE_IGNORED`, `409` on `REJECTED_HITL` replay), runs 3x self-correction loop, upserts PG invoice + competence-PTAX double-entry ledger (`201 COMMITTED_ACID`), or routes to `comex_quarantine_hitl` (`422`). |
| `POST /api/v2/ledger/reconcile-swift` | `API_KEY` / `AUDITOR_API_KEY` | Persists ISO 20022 `camt.053` wire into MongoDB `raw_swift_statements` + balanced 2- or 3-leg PG `journal_entries`/`lines` (`200 RECONCILED`); replays deterministically via `uuid5` (`200 DUPLICATE_IGNORED`, +0 rows). |
| `POST /api/v2/analytics/landed-cost-sim` | `API_KEY` (`persist=false`) / `AUDITOR_API_KEY` (`persist=true`) | Computes cascading import taxes (`VA`, `II`, `IPI`, `PIS`, `COFINS`, `Siscomex`, `AFRMM`, `ICMS por dentro`) and CPC 16 ledger entry across all 4 `VALID_TAX_REGIMES`. |
| `GET /api/v2/quarantine/pending` | `API_KEY` / `AUDITOR_API_KEY` | Lists quarantine records filtered by `review_status` (`PENDING_REVIEW`, `APPROVED_OVERRIDE`, `REJECTED_HITL`, alias `REJECTED` -> `REJECTED_HITL`, or `ALL`) with `limit`/`offset` pagination. |
| `POST /api/v2/quarantine/{quarantine_id}/resolve` | `AUDITOR_API_KEY` only (`403` on standard key) | Enforces HITL state lock (`409` if already resolved, `404` if missing), hashes `reviewed_by` via `PII_HASH_SALT`, and promotes `APPROVED_OVERRIDE` atomically to `commercial_invoices` + ledger or records `REJECTED_HITL`. |

---

## 7. Visual Inspection Guide (DBeaver & MongoDB Compass)

### Connecting Your Database Clients
1. **PostgreSQL 16 (DBeaver)**:
   - **Host**: `localhost` | **Port**: `5434` | **Database**: `comex_fiduciary_db` | **User**: `comex_user` | **Password**: `comex_pass`
2. **DuckDB OLAP Lakehouse (DBeaver)**:
   - Open `./data/comex_lakehouse.duckdb` and execute [`sql/queries/00_init_duckdb_views.sql`](./sql/queries/00_init_duckdb_views.sql) once to register views over `./data/parquet_lakehouse/*`.
3. **MongoDB 7 Authenticated (MongoDB Compass)**:
   - **URI**: `mongodb://comex_mongo_user:comex_mongo_pass@localhost:27018/comex_raw_lake?authSource=admin` -> Database `comex_raw_lake` -> Collections `raw_commercial_invoices`, `raw_bacen_ptax`, `raw_swift_statements`, `raw_mdic_declarations` (or run [`playgrounds/quickstart_comex.mongodb.js`](./playgrounds/quickstart_comex.mongodb.js)).

### Pre-Built SQL Audit & Forensics Suite ([`sql/queries/`](./sql/queries/))

| SQL Script | Audit / Analytical Objective | Verified Invariant |
| :--- | :--- | :--- |
| [`00_init_duckdb_views.sql`](./sql/queries/00_init_duckdb_views.sql) | Binds DuckDB views (`dim_currency_ptax`, `fact_comex_operations`) to Hive Parquet partitions | Columnar `DECIMAL(19,4)` / `DECIMAL(19,6)` schema fidelity |
| [`01_audit_trial_balance_zero_sum.sql`](./sql/queries/01_audit_trial_balance_zero_sum.sql) | Audits General Ledger Trial Balance (*Balancete de Verificação*) across all entries | `SUM(Debit) - SUM(Credit) == 0.0000` (`PASS — ZERO SUM`) |
| [`02_sabotage_test_append_only_trigger.sql`](./sql/queries/02_sabotage_test_append_only_trigger.sql) | Tests unauthorized `UPDATE` and `DELETE` on `journal_lines` inside a rolled-back transaction | `fn_enforce_append_only_ledger()` blocks mutation cleanly |
| [`03_fx_and_swift_reconciliation_audit.sql`](./sql/queries/03_fx_and_swift_reconciliation_audit.sql) | Inspects weekend LOCF forward-fill on `CHF`/`EUR`/`USD` and 3-legged SWIFT fee split | Zero lookahead bias & balanced wire fee deduction |
| [`04_analytical_landed_cost_by_ncm.sql`](./sql/queries/04_analytical_landed_cost_by_ncm.sql) | Star Schema OLAP query joining facts with NCM and Partner Country dimensions | Freight/insurance ratios & BRL landed cost by NCM |
| [`05_quarantine_hitl_forensics.sql`](./sql/queries/05_quarantine_hitl_forensics.sql) | Inspects DLQ payloads caught by shift-left validation with `mongo_raw_id` links | Vertical math mismatch & missing CIF freight/insurance detection |
| [`06_brazilian_cascading_taxes_simulation.sql`](./sql/queries/06_brazilian_cascading_taxes_simulation.sql) | Side-by-side comparison of `LUCRO_REAL` vs. non-crediting nationalization | Exact gross-up `ICMS por dentro` & CPC 16 asset split |
| [`07_hitl_quarantine_resolution_audit.sql`](./sql/queries/07_hitl_quarantine_resolution_audit.sql) | Audits the full lifecycle of `INV-2026-ERR-CIF` from quarantine to ledger promotion | Atomic promotion, hashed `reviewed_by` attribution & `RESOLVED_OVERRIDE` lock |
