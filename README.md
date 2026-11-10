# Comex Data Engine V2 — Multi-Model HTAP Fiduciary Ledger, Cross-Border FX & Brazilian Customs Cascading Tax Engine

[![ODCS v3.2.0](https://img.shields.io/badge/Data_Contracts-ODCS_v3.2.0-0A66C2?style=flat-square)](./contracts/comex_fiduciary_engine_v2.odcs.yaml)
[![Python 3.12](https://img.shields.io/badge/Python-3.12+-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688?style=flat-square&logo=fastapi&logoColor=white)](./src/api/main.py)
[![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL_16-ACID_Fiduciary_Ledger-336791?style=flat-square&logo=postgresql&logoColor=white)](./sql/ddl/01_schema_fiduciary_ledger.sql)
[![MongoDB 7](https://img.shields.io/badge/MongoDB_7-NoSQL_Landing_Zone-47A248?style=flat-square&logo=mongodb&logoColor=white)](./playgrounds/quickstart_comex.mongodb.js)
[![DuckDB | Parquet](https://img.shields.io/badge/OLAP_Lakehouse-DuckDB_%7C_Parquet_Hive-FFF000?style=flat-square&logo=duckdb&logoColor=black)](./src/connectors/lakehouse.py)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg?style=flat-square)](LICENSE)

**Comex Data Engine V2** is an enterprise-grade **Hybrid Transactional/Analytical Processing (HTAP)** and **Fiduciary Accounting Engine** purpose-built for international trade (**COMEX**), multi-currency foreign exchange reconciliation (**CHF, EUR, USD, BRL**), **ISO 20022 SWIFT (`camt.053`)** wire settlement, and **Brazilian Customs Nationalization (*Cálculo de Impostos Aduaneiros em Cascata & CPC 16 / IAS 2*)**.

Engineered under a **Spec-Driven Data Engineering** paradigm, every ingestion pipeline, accounting entry, and analytical transformation is governed by executable **Open Data Contract Standard (ODCS v3.2.0)** specifications, paired **Open Knowledge Format (OKF)** domain mandates, strict **No-Float (`Decimal`)** arithmetic, database-level **PL/pgSQL immutability triggers**, and a closed-loop **Human-in-the-Loop (HITL) Quarantine** workflow.

---

## 1. Executive Summary & Architectural Paradigm Shift

Traditional foreign trade and cross-border bookkeeping architectures fail across two opposing fault lines:
1. **Purely Analytical Batch Pipelines (Legacy ETL)**: Ingest raw foreign trade CSVs (such as Brazilian **MDIC Comex Stat**) using IEEE 754 binary floating-point types (`float` / `DOUBLE PRECISION`) that silently corrupt multi-currency reconciliations by fractions of a cent, while ignoring the transactional lifecycle of **Commercial Invoices**, **Incoterms 2020**, and **SWIFT** bank settlements.
2. **Purely Transactional CRUD Systems (Isolated ERPs)**: Mutate account balances destructively in place (`UPDATE accounts SET balance = ...`), erasing statutory audit trails, failing to segregate competence vs. cash foreign exchange variance, and lacking columnar OLAP throughput to analyze millions of Harmonized System (**HS / NCM**) customs declarations.

**Comex Data Engine V2** bridges both worlds via a **Multi-Model HTAP & Fiduciary Medallion Architecture**:

| Architectural Dimension | Conventional Trade Pipeline (Legacy ETL) | Comex Resilient Data & Fiduciary Engine V2 (HTAP) |
| :--- | :--- | :--- |
| **Workload Topology** | Isolated CSV batch scripts on disk | **Tri-Store HTAP**: **MongoDB 7** (Raw BSON Landing) + **PostgreSQL 16** (ACID Double-Entry Ledger) + **DuckDB / Parquet Hive** (Vectorized OLAP Star Schema) |
| **Monetary & FX Arithmetic** | Binary floating-point (`float` IEEE 754) | **Strict Anti-Float Shield**: `Decimal("0.0001")` (`NUMERIC(19,4)`) for currency, `Decimal("0.000001")` (`NUMERIC(19,6)`) for FX rates, and `BIGINT` scaled cents |
| **Idempotency Guarantee** | Blind file overwrite or duplicate inserts | **Triple-Barrier Idempotency [f(f(x)) = f(x)]**: Edge `SHA-256` payload registry + PostgreSQL composite natural-key `ON CONFLICT` upsert + deterministic `operation_hash_id` |
| **Bookkeeping & Audit Trail** | Destructive `UPDATE` / `DELETE` on balances | **Append-Only Double-Entry Ledger**: Zero-sum invariant (`SUM(Debit) - SUM(Credit) == 0.0000`), PL/pgSQL immutability trigger, and statutory **Reversal Entries** |
| **Cross-Border Tax & FX Rules** | Hardcoded flat percentages, missing weekend rates | **Live BACEN Olinda PTAX** with **LOCF Forward-Fill** (`USD`, `EUR`, `CHF`), **Swiss UID (CHE-MWST)** & **Brazilian CNPJ (Mod-11)** validation, and **Brazilian Cascading Customs Taxes (ICMS "Por Dentro" & CPC 16 / IAS 2)** |
| **Network & Extraction Resilience** | Naive fixed-interval retries or hard crashes | **Tenacity Exponential Backoff + Full Jitter** for external APIs + **3-Iteration Agentic Self-Correction Loop** reinjecting structured Pydantic V2 tracebacks |
| **Exception & Anomaly Handling** | Silent data truncation or pipeline crash (`exit 1`) | **Closed-Loop HITL Quarantine (`comex_quarantine_hitl`)**: Finite state machine (`PENDING_REVIEW` to `APPROVED_OVERRIDE` or `REJECTED`) with atomic ledger promotion |

---

## 2. End-to-End 5-Layer HTAP Architecture

```mermaid
flowchart TD
    subgraph Sources["1. External Cross-Border Sources"]
        S1["MDIC Comex Stat Open Data"]
        S2["BACEN Olinda Official PTAX API"]
        S3["Commercial Invoices and SWIFT camt.053"]
    end

    subgraph Ingestion["2. Resilient Ingestion and Landing (MongoDB 7)"]
        ING["Resilient Extractors<br/>Tenacity Jitter and Offline Fallback"]
        API["FastAPI Edge API<br/>POST /api/v2/invoices/ingest"]
        SHA["SHA-256 Idempotency Filter"]
        MGO["MongoDB 7 comex_raw_lake<br/>Raw Invoices, SWIFT and MDIC BSONs"]
        LOOP["Agentic Self-Correction Loop<br/>3x Pydantic Traceback Retries"]
    end

    subgraph Validation["3. Shift-Left Contracts (ODCS v3.2.0)"]
        PYD["Pydantic V2 Contract Guards<br/>Zero-Float and Incoterms 2020"]
        HITL["HITL Quarantine Queue<br/>comex_quarantine_hitl"]
    end

    subgraph Fiduciary["4. Fiduciary OLTP Engine (PostgreSQL 16 ACID)"]
        UPS["Atomic Upsert Engine<br/>Composite Natural Key"]
        TAX["Customs and Cascading Tax Engine<br/>ICMS Por Dentro and CPC 16"]
        LED["Immutable Double-Entry Ledger<br/>PL/pgSQL Trigger: Block UPDATE/DELETE"]
    end

    subgraph Analytics["5. Vectorized OLAP Lakehouse (DuckDB + Parquet)"]
        PARQ["Parquet Hive Lakehouse<br/>Partitioned by year and month"]
        DUCK["DuckDB Star Schema<br/>fact_comex_operations and dim_currency_ptax"]
    end

    S1 --> ING
    S2 --> ING
    S3 --> API
    API --> SHA
    SHA --> MGO
    MGO --> LOOP
    LOOP --> PYD
    ING --> PYD
    PYD -->|Valid Payload| UPS
    PYD -->|Math Drift or Schema Error| HITL
    HITL -->|Approved Override| UPS
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

- **Anti-Float Guardrail ([`src/models/invoices.py`](./src/models/invoices.py), [`src/models/customs.py`](./src/models/customs.py))**: Any `float` instance passed to a monetary or exchange-rate field raises an immediate `ValueError("IEEE 754 float is strictly prohibited...")`. All monetary measures are quantized to `Decimal("0.0001")` (`NUMERIC(19,4)`) using `ROUND_HALF_UP` and mirrored as integer scaled cents (`BIGINT = amount * 10000`). Exchange rates are quantized to `Decimal("0.000001")` (`NUMERIC(19,6)`).

- **Horizontal & Vertical Invoice Math Closure**:
  ```text
  Horizontal Line Math:    ROUND(quantity * unit_price, 4) == total_price
  Line Aggregation:        SUM(line.total_price) == subtotal_amount
  Vertical Header Closure: subtotal_amount + tax_amount + freight_amount + insurance_amount == total_amount
  ```
  - **Incoterms 2020 Semantics**: Under `CIF` (*Cost, Insurance, and Freight*), explicit non-zero `freight_amount` is mandatory to prevent customs under-valuation (*Subfaturamento de Valor Aduaneiro*).

- **Transnational Tax ID & PII Governance**:
  - **Cleartext B2B Corporate Tax IDs**: Validates 14-digit **Brazilian CNPJ** (Modulo-11 check digits) and official **Swiss Corporate UID** (e.g. `CHE-116.281.710 MWST`), preserving them in cleartext for statutory customs traceability and natural-key deduplication.
  - **Salted SHA-256 Natural-Person PII**: Hashes individual personal data (`contact_email` -> `contact_email_hash`) with a cryptographic salt before persistence.

---

### 3.2 Append-Only Double-Entry Fiduciary Ledger & Database Triggers
All validated financial events (invoice recognition, customs nationalization, and SWIFT bank settlement) are recorded in PostgreSQL 16 ([`sql/ddl/01_schema_fiduciary_ledger.sql`](./sql/ddl/01_schema_fiduciary_ledger.sql)):

- **Zero-Sum Accounting Identity**: Every `journal_entry` requires at least two lines (one debit leg and one credit leg) satisfying the fundamental zero-sum invariant:
  ```text
  SUM(debit_amount) - SUM(credit_amount) == 0.0000  (max tolerance ±0.0001)
  ```
- **Database-Enforced Immutability**: A PostgreSQL PL/pgSQL trigger (`trg_journal_lines_immutable` / `prevent_journal_mutation()`) intercepts and aborts any `UPDATE` or `DELETE` statement against `POSTED` ledger records:
  ```sql
  RAISE EXCEPTION 'Append-Only Violation: journal_lines cannot be updated or deleted. Use statutory reversal entries (create_reversal_entry).';
  ```
- **Statutory Reversals (*Estorno Contábil*)**: Operational corrections are executed exclusively via `create_reversal_entry()`, which posts a new compensating `journal_entry` with inverted debit/credit legs referencing `reversed_from_entry_id`.

---

### 3.3 Live BACEN Olinda PTAX Connector, LOCF Forward-Fill & SWIFT `camt.053` Reconciliation
Implemented in [`src/extractors/bacen_ptax_client.py`](./src/extractors/bacen_ptax_client.py) and [`src/models/customs.py`](./src/models/customs.py):

- **Resilient Central Bank Extraction**: Queries the official **Banco Central do Brasil (BACEN) Olinda OData API** (`CotacaoMoedaPeriodo`) for `USD`, `EUR`, and `CHF` using `tenacity` exponential backoff with full jitter (randomized sleep up to 60s), with deterministic offline fallback to [`samples/bacen_ptax_sample.json`](./samples/bacen_ptax_sample.json).
- **Zero-Lookahead LOCF Forward-Fill (`apply_locf_forward_fill`)**: Because central banks do not publish exchange rates on weekends or national holidays while customs shipments occur 365 days a year, calendar gaps are filled strictly using **Last Observation Carried Forward (LOCF)** from the immediately preceding business day (`is_interpolated_locf = TRUE`), preventing lookahead bias.
- **Competence vs. Cash Settlement & SWIFT Fee Split (`build_swift_fee_split_entry`)**:
  - Reconciles ISO 20022 `camt.053` international wire statements where intermediary correspondent banks deduct wire fees (typically between 10.00 and 50.00 units under `SHA` or `BEN` clauses).
  - Generates a balanced **3-Legged Split Journal Entry** in functional currency (`BRL`):
    1. **Debit**: `1.1.1.01` (*Banco Conta Movimento — Câmbio*) for Net Cash Received (`Net * Rate`).
    2. **Debit**: `4.1.2.01` (*Despesas Bancárias e Tarifas SWIFT*) for Deducted Wire Fee (`Fee * Rate`).
    3. **Credit**: `1.1.2.01` (*Clientes no Exterior / Contas a Receber*) for Gross Invoice Nominal Amount (`Gross * Rate`).

---

### 3.4 Brazilian Customs Cascading Tax Engine & CPC 16 (IAS 2) Inventory Valuation
Implemented in [`src/models/customs.py`](./src/models/customs.py) (`compute_brazilian_landed_cost` & `build_import_nationalization_ledger_entry`):

#### Statutory Brazilian Import Tax Cascade (Sequential Execution)
```text
1. Customs Value (VA):   VA_BRL     = ROUND((FOB_USD + Freight_USD + Insurance_USD) * PTAX_sell, 4)
2. Import Duty (II):      II_BRL     = ROUND(VA_BRL * Aliq_II, 4)
3. Industrial Tax (IPI):  IPI_BRL    = ROUND((VA_BRL + II_BRL) * Aliq_IPI, 4)
4. PIS Importação:        PIS_BRL    = ROUND(VA_BRL * Aliq_PIS, 4)
5. COFINS Importação:     COFINS_BRL = ROUND(VA_BRL * Aliq_COFINS, 4)
6. ICMS Base (Gross-Up):  Base_ICMS  = ROUND((VA_BRL + II_BRL + IPI_BRL + PIS_BRL + COFINS_BRL + Siscomex + AFRMM) / (1 - Aliq_ICMS), 4)
7. ICMS Importação:       ICMS_BRL   = ROUND(Base_ICMS * Aliq_ICMS, 4)
8. Total Taxes:           Taxes_BRL  = II_BRL + IPI_BRL + PIS_BRL + COFINS_BRL + Siscomex + AFRMM + ICMS_BRL
9. Total Landed Cost:     Landed_BRL = VA_BRL + Taxes_BRL
```

- **ICMS "Cálculo por Dentro"**: In Brazil, ICMS is statutory grossed-up by dividing by `(1 - Aliq_ICMS)` so that ICMS itself is included in its own taxable base alongside all federal duties and customs fees (*Taxa Siscomex* + *AFRMM*).
- **CPC 16 (R1) / IAS 2 Inventory Cost Segregation**:
  - **`LUCRO_REAL` (Non-Cumulative Regime)**: Capitalizes only non-recoverable duties into merchandise inventory (`1.1.3.02`: `VA + II + Siscomex + AFRMM`) and segregates recoverable taxes (`IPI + PIS + COFINS + ICMS`) into `1.1.4.01` (*Tributos a Recuperar*).
  - **`CUSTO_INTEGRAL` (Cumulative Regime)**: Capitalizes 100% of cascading taxes directly into inventory cost (`1.1.3.02`).

---

### 3.5 Bulk MDIC Comex Stat Streaming & Canonical 8-Key Pre-Aggregation
Implemented in [`src/extractors/mdic_bulk_loader.py`](./src/extractors/mdic_bulk_loader.py):
- Streams official federal trade CSVs (`IMP_YYYY.csv` / `EXP_YYYY.csv`), supporting both 11-column and 12-column (`CO_UNID`) layouts.
- **Canonical 8-Key Pre-Aggregation**: Aggregates rows sharing the natural tuple `(year, month, operation_type, ncm_code, country_code, uf_code, modal_code, urf_code)` across differing statistical units (`CO_UNID`) before computing the deterministic 64-char SHA-256 `operation_hash_id`.
- **Early-Arriving Dimensions Defense**: Automatically upserts missing keys into `dim_date`, `dim_merchandise_ncm`, and `dim_partner_country` prior to fact table insertion, preventing foreign-key rollbacks during asynchronous bulk loads.

---

### 3.6 Closed-Loop HITL Quarantine & Autonomous Incident Triage
Implemented in [`src/pipeline/comex_processor.py`](./src/pipeline/comex_processor.py):
- **Finite State Machine (`comex_quarantine_hitl`)**: Invoices failing mathematical closure or Incoterm rules after 3 self-correction attempts are routed to `PENDING_REVIEW` with their `mongo_raw_id` and structured Pydantic error tree.
- **Audited Resolution (`resolve_quarantined_invoice`)**: Human auditors inspect the raw payload in MongoDB Compass and submit a resolution via `POST /api/v2/quarantine/{id}/resolve`. Under `APPROVED_OVERRIDE`, the corrected payload is re-validated against `CommercialInvoiceInput`, atomically upserted into `commercial_invoices`, booked into the double-entry ledger, and stamped with `reviewed_by` and `reviewed_at` (`HTTP 409 Conflict` blocks any subsequent mutation).
- **Automated Incident Post-Mortem & RCA**: The engine generates deterministic RCA post-mortems ([`INC-2026-11-10-001_quarantine_triage.md`](./knowledge/runbooks/INC-2026-11-10-001_quarantine_triage.md)) following structured decision trees for compliance auditability.

---

## 4. Repository Structure

```text
comex-data-engine-v2/
├── Makefile                                       # Deterministic CLI targets (setup, up, seed, fetch-ptax, fetch-mdic, verify)
├── docker-compose.yml                             # Container orchestration: PostgreSQL 16 (5434) & MongoDB 7 (27018)
├── pyproject.toml                                 # Python 3.12+ dependency lock (FastAPI, Pydantic V2, DuckDB, PyArrow, Tenacity)
├── .env.example                                   # Environment configuration template
├── contracts/
│   ├── canonical_document_v1.odcs.yaml            # Base HTAP ODCS v3.2.0 contract
│   └── comex_fiduciary_engine_v2.odcs.yaml        # Executable Comex V2 ODCS v3.2.0 contract (Invoices, Ledger, Taxes, Star Schema)
├── knowledge/
│   ├── domain/
│   │   ├── architecture_raw.md                    # Deep-dive HTAP architectural blueprint & ADRs
│   │   └── semantic_rules.md                      # 11 sections of paired MANDATE / !ANTI_PATTERN domain invariants
│   └── runbooks/
│       ├── INCIDENT_TEMPLATE.md                   # 6-step deterministic incident decision tree (P0-P3)
│       └── INC-2026-11-10-001_quarantine_triage.md # Post-incident RCA & contract immunization report
├── samples/                                       # Deterministic offline seed datasets
│   ├── bacen_ptax_sample.json                     # Multi-currency PTAX series (USD, EUR, CHF) with weekend gaps
│   ├── commercial_invoices_batch.json             # Valid & corrupted cross-border invoices (Swiss UID & Brazilian CNPJ)
│   ├── comex_mdic_sample.csv                      # Pre-valued customs operations sample
│   ├── customs_nationalizations_sample.json       # Import declarations (DI/Duimp) for LUCRO_REAL & CUSTO_INTEGRAL
│   ├── mdic_imp_sample.csv / mdic_exp_sample.csv  # Raw MDIC microdata with multi-CO_UNID rows
│   └── swift_camt053_statements.json              # ISO 20022 camt.053 wire settlements with SWIFT fee deductions
├── sql/
│   ├── ddl/
│   │   └── 01_schema_fiduciary_ledger.sql         # DDL: ACID tables, Star Schema, indexes & PL/pgSQL immutability triggers
│   └── queries/                                   # 8 ready-to-run DBeaver SQL audit & analytical scripts
│       ├── 00_init_duckdb_views.sql               # Materializes DuckDB views over Hive-partitioned Parquet files
│       ├── 01_audit_trial_balance_zero_sum.sql    # General Ledger trial balance & zero-sum verification
│       ├── 02_sabotage_test_append_only_trigger.sql # Live proof of PL/pgSQL trigger blocking UPDATE/DELETE fraud
│       ├── 03_fx_and_swift_reconciliation_audit.sql # PTAX LOCF & 3-legged SWIFT fee split inspection
│       ├── 04_analytical_landed_cost_by_ncm.sql   # Star Schema OLAP aggregation by NCM, country & transport modal
│       ├── 05_quarantine_hitl_forensics.sql       # Forensics on quarantined invoices & Pydantic error trees
│       ├── 06_brazilian_cascading_taxes_simulation.sql # Cascading tax verification (ICMS por dentro & CPC 16)
│       └── 07_hitl_quarantine_resolution_audit.sql # End-to-end audit of HITL promotion & ledger booking
├── src/
│   ├── api/
│   │   └── main.py                                # FastAPI REST server (v1 & v2 endpoints)
│   ├── connectors/
│   │   ├── postgres.py                            # SQLAlchemy 2.0 connection pool for PostgreSQL 16
│   │   ├── mongo.py                               # PyMongo client for MongoDB 7 raw landing zone
│   │   └── lakehouse.py                           # PyArrow Hive Parquet writer & DuckDB OLAP connector
│   ├── extractors/
│   │   ├── bacen_ptax_client.py                   # Live BACEN Olinda PTAX client + LOCF forward-fill CLI
│   │   └── mdic_bulk_loader.py                    # Streaming MDIC Comex Stat loader + 8-key canonical aggregation
│   ├── models/
│   │   ├── canonical.py                           # Base shift-left schema
│   │   ├── invoices.py                            # CommercialInvoiceInput (Zero-Float, CNPJ/Swiss UID, Incoterms)
│   │   └── customs.py                             # Ledger models, SWIFT split, Cascading Tax Engine & CPC 16 builder
│   ├── pipeline/
│   │   ├── processor.py                           # Base HTAP processor
│   │   └── comex_processor.py                     # ComexHTAPProcessor (Self-correction, ACID upsert, HITL resolution)
│   └── seed.py                                    # End-to-end physical seed runner with telemetry summary
└── tests/
    ├── test_comex_fiduciary_v2.py                 # 31 unit, mathematical, HTTP-mocked & integration tests
    └── test_live_htap.py                          # Multi-model HTAP connectivity & contract tests
```

---

## 5. Quickstart & Deterministic CLI Execution

### Prerequisites
- Linux / macOS with **Python 3.12+**, [`uv`](https://docs.astral.sh/uv/), and **Docker Compose**.

### Step 1: Install Dependencies & Start Multi-Model Containers
```bash
# Synchronize locked virtual environment (.venv)
make setup

# Launch PostgreSQL 16 (port 5434) and MongoDB 7 (port 27018)
make up
make ps
```

### Step 2: Seed All 3 Storage Engines & Inspect Live Telemetry
```bash
make seed
```
**Verified Telemetry Output (`make seed`)**:
```text
======================================================================
 📊 RESUMO DE TELEMETRIA DO BANCO (DBeaver / MongoDB / DuckDB)
======================================================================
 🐘 PostgreSQL commercial_invoices:         5 registros (Validados & Desembaraçados)
 🐘 PostgreSQL journal_entries:             9 lançamentos contábeis
 🐘 PostgreSQL journal_lines:              24 partidas dobradas
 🐘 PostgreSQL Soma Zero Invariante:   0.0000 (Zero Absoluto)
 🏛️ PostgreSQL customs_nationalizations:    2 declarações (Cascata & CPC 16)
 🚨 PostgreSQL comex_quarantine_hitl:       2 divergências (1 PENDING, 1 APPROVED_OVERRIDE)
 💱 PostgreSQL dim_currency_ptax:          18 cotações diárias (com LOCF)
 🚢 PostgreSQL fact_comex_operations:       5 células aduaneiras
 🍃 MongoDB raw_commercial_invoices:        6 BSONs brutos
 🦆 DuckDB fact_comex_operations:           5 linhas Parquet Hive
======================================================================
```

### Step 3: Run Live Extractors (BACEN PTAX & MDIC Comex Stat)
```bash
# Synchronize USD, EUR, and CHF exchange rates with LOCF weekend forward-fill
make fetch-ptax START=2026-11-05 END=2026-11-10 CURRENCIES=USD,EUR,CHF

# Stream and pre-aggregate MDIC trade microdata into Parquet Hive & PostgreSQL
make fetch-mdic YEAR=2024 MONTH=1 FLOW=BOTH OFFLINE=true
```

### Step 4: Execute the 5-Gate Certification Suite
```bash
# Lint ODCS v3.2.0 contracts and run all 34 automated pytest assertions
make verify
```

---

## 6. FastAPI REST API Reference (`http://localhost:8000/docs`)

Start the server via `make run-api`:

| Method & Route | Purpose | Key Contract Guarantees |
| :--- | :--- | :--- |
| `GET /health` | Multi-model architecture readiness check | Confirms HTAP runtime status |
| `POST /api/v2/invoices/ingest` | Ingests cross-border Commercial Invoices | Computes `SHA-256` hash, lands raw BSON in MongoDB, runs 3x self-correction loop, upserts into PostgreSQL + Ledger or routes to `comex_quarantine_hitl` (`HTTP 422`) |
| `POST /api/v2/ledger/reconcile-swift` | Reconciles ISO 20022 `camt.053` wire statements | Rejects `float`, computes 3-legged SWIFT fee split entry (Debit == Credit) |
| `POST /api/v2/analytics/landed-cost-sim` | Simulates or persists Brazilian Import Nationalization | Computes `VA`, `II`, `IPI`, `PIS`, `COFINS`, grossed-up `ICMS "por dentro"`, and books 4-legged `CPC 16` journal entry (`persist=true/false`) |
| `GET /api/v2/quarantine/pending` | Lists quarantined payloads by review status | Returns `quarantine_id`, `mongo_raw_id`, and structured validation errors |
| `POST /api/v2/quarantine/{id}/resolve` | Closed-loop HITL quarantine resolution | Enforces finite state machine (`HTTP 409` if already resolved); promotes `APPROVED_OVERRIDE` atomically to `commercial_invoices` + `journal_lines` |

---

## 7. Visual Inspection Guide (DBeaver & MongoDB Compass)

### Connecting Your Database Clients
1. **PostgreSQL 16 (DBeaver)**:
   - **Host**: `localhost` | **Port**: `5434` | **Database**: `comex_fiduciary_db` | **User**: `comex_user` | **Password**: `comex_pass`
2. **DuckDB OLAP Lakehouse (DBeaver)**:
   - Open `./data/comex_lakehouse.duckdb` and execute [`sql/queries/00_init_duckdb_views.sql`](./sql/queries/00_init_duckdb_views.sql) once to register views over `./data/parquet_lakehouse/*`.
3. **MongoDB 7 (MongoDB Compass)**:
   - **URI**: `mongodb://localhost:27018` -> Database `comex_raw_lake` -> Collection `raw_commercial_invoices` (or run [`playgrounds/quickstart_comex.mongodb.js`](./playgrounds/quickstart_comex.mongodb.js)).

### Pre-Built SQL Audit & Forensics Suite ([`sql/queries/`](./sql/queries/))

| SQL Script | Audit / Analytical Objective | Verified Invariant |
| :--- | :--- | :--- |
| [`00_init_duckdb_views.sql`](./sql/queries/00_init_duckdb_views.sql) | Binds DuckDB views (`dim_currency_ptax`, `fact_comex_operations`) to Hive Parquet partitions | Columnar `DECIMAL(19,4)` / `DECIMAL(19,6)` schema fidelity |
| [`01_audit_trial_balance_zero_sum.sql`](./sql/queries/01_audit_trial_balance_zero_sum.sql) | Audits General Ledger Trial Balance (*Balancete de Verificação*) across all entries | `SUM(Debit) - SUM(Credit) == 0.0000` (`PASS — ZERO SUM`) |
| [`02_sabotage_test_append_only_trigger.sql`](./sql/queries/02_sabotage_test_append_only_trigger.sql) | Attempts unauthorized `UPDATE` and `DELETE` on posted `journal_lines` | PL/pgSQL trigger aborts mutation with statutory error |
| [`03_fx_and_swift_reconciliation_audit.sql`](./sql/queries/03_fx_and_swift_reconciliation_audit.sql) | Inspects weekend LOCF forward-fill on `CHF`/`EUR`/`USD` and 3-legged SWIFT fee split | Zero lookahead bias & balanced wire fee deduction |
| [`04_analytical_landed_cost_by_ncm.sql`](./sql/queries/04_analytical_landed_cost_by_ncm.sql) | Star Schema OLAP query joining facts with NCM and Partner Country dimensions | Freight/insurance ratios & BRL landed cost by NCM |
| [`05_quarantine_hitl_forensics.sql`](./sql/queries/05_quarantine_hitl_forensics.sql) | Inspects DLQ payloads caught by shift-left validation with `mongo_raw_id` links | Vertical math mismatch & missing CIF freight detection |
| [`06_brazilian_cascading_taxes_simulation.sql`](./sql/queries/06_brazilian_cascading_taxes_simulation.sql) | Side-by-side comparison of `LUCRO_REAL` vs. `CUSTO_INTEGRAL` nationalization | Exact gross-up `ICMS por dentro` & CPC 16 asset split |
| [`07_hitl_quarantine_resolution_audit.sql`](./sql/queries/07_hitl_quarantine_resolution_audit.sql) | Audits the full lifecycle of `INV-2026-ERR-CIF` from quarantine to ledger promotion | Atomic promotion, reviewer attribution & idempotency lock |

---

## 8. License

Distributed under the MIT License.
