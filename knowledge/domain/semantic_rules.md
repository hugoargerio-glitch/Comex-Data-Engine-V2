---
okf_version: "1.0"
document_id: "domain-semantic-rules-comex-fiduciary-v2"
domain: "comex_fiduciary_analytics"
owners: ["data-architecture-team", "fiduciary-accounting-team", "customs-compliance"]
linked_contracts:
  - "./contracts/comex_fiduciary_engine_v2.odcs.yaml"
last_verified: "2026-11-09"
---

# Canonical Domain Semantic Rules — Comex Resilient Data & Fiduciary Engine V2 (`semantic_rules.md`)

This **Open Knowledge Format (OKF)** document defines the institutional single source of truth for international trade (COMEX), foreign exchange (FX) reconciliation, double-entry fiduciary accounting, and data governance in `comex-data-engine-v2`. Every pipeline module, API handler, and contributor must strictly adhere to these paired constraints before designing, implementing, or auditing workloads.

---

## 1. Entity Definitions & Granularity Matrix (HTAP Ledger + OLAP Star Schema)

| Entity / Collection / Table | Storage Engine | Layer | Atomic Grain | Primary Key / Natural Key | Partition Key | Invariant / Cardinality |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `raw_commercial_invoices` | MongoDB 7 | Landing | 1 BSON doc per incoming invoice payload | `_id` (ObjectId) / `payload_hash` | N/A | Append-only raw landing zone |
| `raw_bacen_ptax` | MongoDB 7 | Landing | 1 BSON doc per BACEN Olinda API page | `_id` (ObjectId) / `idempotency_key` | N/A | Raw Olinda OData responses |
| `raw_swift_statements` | MongoDB 7 | Landing | 1 BSON doc per bank statement message | `_id` (ObjectId) / `idempotency_key` | N/A | ISO 20022 `camt.053` or MT103 |
| `raw_mdic_declarations` | MongoDB 7 | Landing | 1 BSON doc per raw MDIC declaration batch | `_id` (ObjectId) / `batch_id` | N/A | Raw customs batch metadata |
| `ingestion_idempotency_registry` | PostgreSQL 16 | Edge Governance | 1 row per ingested payload fingerprint | `payload_hash` (SHA-256) | N/A | Edge dedup & `REJECTED_HITL` guard |
| `commercial_invoices` | PostgreSQL 16 | Operational Core | **1 row per validated commercial invoice** | `invoice_id` (UUID) / Composite Natural Key | `issue_date` | `1:N` to `journal_entries` & lines; `*_cents` `GENERATED ALWAYS` |
| `commercial_invoice_lines` | PostgreSQL 16 | Operational Core | 1 row per itemized invoice product line | `line_id` (UUID) / `(invoice_id, line_number)` | N/A | `quantity * unit_price == total_price` |
| `accounts` | PostgreSQL 16 | Operational Core | 1 row per Chart of Accounts code | `account_code` | N/A | Master Chart of Accounts |
| `journal_entries` | PostgreSQL 16 | Fiduciary Ledger | 1 row per accounting transaction event | `entry_id` (UUID) | `competence_date` | `1:N` to `journal_lines` ($N \ge 2$); append-only |
| `journal_lines` | PostgreSQL 16 | Fiduciary Ledger | 1 row per debit or credit entry leg | `line_id` (UUID) | N/A | $\sum \text{Debit} - \sum \text{Credit} = 0$ |
| `comex_quarantine_hitl` | PostgreSQL 16 | Operational DLQ | 1 row per rejected ingestion payload | `quarantine_id` (UUID) | N/A | `PENDING_REVIEW` -> `APPROVED_OVERRIDE` \| `REJECTED_HITL` |
| `customs_nationalizations` | PostgreSQL 16 | Operational Core | 1 row per DI / Duimp tax liquidation | `nationalization_id` (UUID) / `declaration_number` | `clearance_date` | Cascading taxes + ICMS por dentro |
| `dim_date` | DuckDB / PG | Analytical Dim | 1 row per calendar day | `date_id` (YYYYMMDD) | N/A | Canonical calendar |
| `dim_merchandise_ncm` | DuckDB / PG | Analytical Dim | 1 row per 8-digit NCM code | `ncm_code` (8 digits) | N/A | Conforms to HS 6-digit prefix |
| `dim_partner_country` | DuckDB / PG | Analytical Dim | 1 row per trading partner country | `country_code` (MDIC ID) | N/A | Conforms to ISO 3166-1 alpha-3 |
| `dim_currency_ptax` | DuckDB / PG | Analytical Dim | 1 row per `(date_id, currency_code)` | `(date_id, currency_code)` | `year=YYYY/month=MM` | LOCF forward-filled (`date_id <= target`) |
| `fact_comex_operations` | Parquet Hive / DuckDB | Analytical Fact | 1 row per monthly customs declaration cell | `operation_hash_id` (SHA-256) | `year=YYYY/month=MM` | `1:1` per statistical cell |

### Paired Granularity & Fan-Out Prevention Constraints (Comex)
- **MANDATE**: Before joining `commercial_invoices` with `journal_lines` or invoice line items, pre-aggregate child tables within a CTE grouped by the foreign key (`GROUP BY entry_id` or `GROUP BY invoice_id`) and verify line uniqueness.
- **!ANTI_PATTERN**: Never execute direct `LEFT JOIN journal_lines jl ON je.entry_id = jl.entry_id` inside queries that sum header-level monetary metrics across entries, preventing silent metric fan-out by a factor of $N$ lines.
- **MANDATE**: Compute analytical MDIC grain using a deterministic 64-character SHA-256 hash:
  $$\text{operation\_hash\_id} = \text{SHA-256}(\text{ano} \parallel \text{mes} \parallel \text{tipo\_op} \parallel \text{co\_ncm} \parallel \text{co\_pais} \parallel \text{sg\_uf} \parallel \text{co\_via} \parallel \text{co\_urf})$$
- **!ANTI_PATTERN**: Never rely on autoincrementing integer sequences or random UUIDv4 keys for statistical operations facts, which corrupt idempotency during analytical re-runs.

---

## 2. Monetary Precision (`DECIMAL(19,4)`), FX Precision (`DECIMAL(19,6)`) & Zero-Float Prohibition

### Paired Typing & Monetary Arithmetic Constraints
- **MANDATE**: Store and compute all Comex monetary values (`subtotal_amount`, `tax_amount`, `freight_amount`, `insurance_amount`, `total_amount`, `landed_cost_brl`, `fob_amount_usd`, `cif_amount_usd`) strictly as `DECIMAL(19,4)` / `NUMERIC(19,4)` alongside their integer representation in scaled cents (`BIGINT = amount * 10000`).
- **!ANTI_PATTERN**: Never use IEEE 754 binary floating-point types (`float`, `double`, `real`) in Python, SQL, or Pydantic V2. Any `float` instance passed to a monetary or FX validator must raise an immediate `ValueError`.
- **MANDATE**: Store and compute all foreign exchange rates (`ptax_buy_rate`, `ptax_sell_rate`, `exchange_rate_applied`) strictly as `DECIMAL(19,6)` / `NUMERIC(19,6)`.
- **!ANTI_PATTERN**: Never truncate or round foreign exchange rates to 2 or 4 decimal places, as a $0.0001$ drift on multi-million-dollar customs contracts produces material accounting discrepancies.
- **MANDATE**: Wrap all optional discount, tax, freight, and insurance columns in `COALESCE(col, CAST(0.0000 AS DECIMAL(19,4)))` prior to mathematical summation.
- **!ANTI_PATTERN**: Never perform direct addition or subtraction on nullable monetary columns without explicit `COALESCE`, preventing silent `NULL` propagation in invoice balances.

---

## 3. Double-Entry Bookkeeping & Zero-Sum ACID Invariants

### Paired Fiduciary Ledger Constraints
- **MANDATE**: Every accounting entry in `journal_entries` must contain at least two rows in `journal_lines` (minimum 1 debit and 1 credit) and must strictly satisfy the zero-sum invariant in functional currency:
  $$\sum_{i=1}^{k} \text{debit\_amount}_i - \sum_{j=1}^{m} \text{credit\_amount}_j = 0.0000 \quad (\pm 0.0001 \text{ max tolerance})$$
- **!ANTI_PATTERN**: Never commit a single-legged journal entry, an unbalanced entry, or a journal line where both `debit_amount > 0` and `credit_amount > 0` simultaneously.
- **MANDATE**: Maintain the fiduciary ledger as strictly **Append-Only**. Once an entry has `status = 'POSTED'`, PostgreSQL triggers and application services must reject any `UPDATE` or `DELETE` operations.
- **!ANTI_PATTERN**: Never execute `UPDATE journal_lines SET debit_amount = ...` or `DELETE FROM journal_entries WHERE ...` to fix bookkeeping mistakes, which destroys the statutory audit trail.
- **MANDATE**: Execute accounting corrections exclusively via statutory **Reversal Entries (`create_reversal_entry`)**, generating a new `journal_entry` with inverted line amounts (`debit_amount <-> credit_amount`) referencing `reversed_from_entry_id`.
- **!ANTI_PATTERN**: Never overwrite historical ledger balances in place; every financial correction must leave an auditable compensating entry.

---

## 4. FX Reconciliation (PTAX Competence vs. Spot Liquidation) & LOCF Forward-Fill

### Paired Foreign Exchange Constraints
- **MANDATE**: In `dim_currency_ptax`, populate weekends and non-business holidays strictly using **Last Observation Carried Forward (LOCF / Forward-Fill)** from the immediately preceding business day, setting `is_interpolated_locf = TRUE`.
- **!ANTI_PATTERN**: Never backfill weekend or holiday exchange rates using future quotations (Lookahead Bias), which introduces temporal leakage and fiscal non-compliance.
- **MANDATE**: Segregate foreign currency invoice lifecycle into Competence ($t_1$ at $\text{PTAX}(t_1)$) and Liquidation ($t_2$ at $\text{Spot}(t_2)$), booking the realized variance $\Delta_{\text{Cambial}} = V_{\text{estrang}} \times (\text{Taxa}_{\text{Spot}}(t_2) - \text{Taxa}_{\text{PTAX}}(t_1))$ into Active FX Gain (`REALIZED_FX_GAIN`) or Passive FX Loss (`REALIZED_FX_LOSS`).
- **!ANTI_PATTERN**: Never settle foreign currency invoices at the original competence PTAX rate or fail reconciliation when $\text{Spot}(t_2) \neq \text{PTAX}(t_1)$.

---

## 5. Incoterms, Landed Cost & SWIFT Fee Split Entries

### Paired Customs & Banking Constraints
- **MANDATE**: Enforce vertical invoice math closure (`subtotal_amount + tax_amount + freight_amount + insurance_amount == total_amount` within $\pm 0.0001$) and validate Incoterms (`EXW`, `FOB`, `CIF`, `DDP`), requiring explicit separation of `freight_amount` and `insurance_amount` under `CIF` for customs valuation (*Valor Aduaneiro*).
- **!ANTI_PATTERN**: Never compute Landed Cost or Brazilian import duties on `FOB` merchandise value alone without incorporating international freight and insurance.
- **MANDATE**: When reconciling SWIFT `camt.053` / `MT103` bank statements under `SHA`, `OUR`, or `BEN` fee clauses with intermediary bank deductions ($10.00–$50.00 within $\pm 5$ business days), automatically generate a **Split Journal Entry** debiting Cash (net received) and SWIFT Bank Fees (deducted fee) against Accounts Receivable (gross invoice amount).
- **!ANTI_PATTERN**: Never reject a valid international wire settlement or write down gross export revenue when the variance matches a standard intermediary SWIFT fee deduction.

---

## 6. Corporate Tax IDs vs. Natural-Person PII Governance

### Paired Privacy & Data Masking Constraints
- **MANDATE**: Retain B2B corporate tax identifiers (`issuer_tax_id`: 14-digit Modulo 11 Brazilian CNPJ and Swiss UID `CHE-XXX.XXX.XXX`) in **cleartext** across MongoDB, PostgreSQL, and DuckDB to support statutory fiscal auditing and the composite natural key upsert.
- **!ANTI_PATTERN**: Never hash or mask public corporate tax IDs (CNPJ / Swiss UID), which breaks statutory customs traceability and natural-key deduplication.
- **MANDATE**: Apply salted cryptographic `SHA-256` hashing (`pii_hash_salt`) strictly to natural-person PII (individual CPF, personal contact emails, individual names, IP addresses) before persisting to operational or analytical tables.
- **!ANTI_PATTERN**: Never store cleartext natural-person PII outside the restricted landing zone or expose it in logs, tracebacks, or analytical tables.

---

## 7. Multi-Level Idempotency ($f(f(x)) = f(x)$)

### Paired Idempotency Constraints
- **MANDATE**: Enforce three idempotency barriers:
  1. **Edge Barrier**: Payload `SHA-256` fingerprint check in `ingestion_idempotency_registry`.
  2. **Relational Barrier**: PostgreSQL atomic upsert `ON CONFLICT (issuer_tax_id, document_number, issue_date, currency_code, total_amount) DO UPDATE SET extraction_confidence = GREATEST(...)`.
  3. **Analytical Barrier**: `operation_hash_id` deduplication in DuckDB/Parquet Lakehouse.
- **!ANTI_PATTERN**: Never execute blind `INSERT INTO` without conflict resolution or partition deduplication.

---

## 8. Shift-Left Quarantine & Agentic Self-Correction Loop

### Paired Error Handling & HITL Constraints
- **MANDATE**: On Pydantic V2 `ValidationError` during document extraction, run at most 3 deterministic **Self-Correction Loop** iterations; if still invalid, route the payload to `comex_quarantine_hitl` (`review_status = 'PENDING_REVIEW'`) preserving `mongo_raw_id` for visual inspection in MongoDB Compass.
- **!ANTI_PATTERN**: Never perform blind LLM retries with the identical prompt, and never silently drop or coerce arithmetic mismatches into production tables.
- **MANDATE**: All arithmetic mismatches exceeding $0.0001$ (`ABS(total - (subtotal + tax + freight + insurance)) > 0.0001`) must fail validation immediately and be quarantined for human auditor review via MongoDB Compass.
- **!ANTI_PATTERN**: Never coerce or silently round arithmetic discrepancies into an arbitrary balancing line without human review.

---

## 9. Bulk MDIC Comex Stat Ingestion & Dimensional Integrity

### Paired Bulk Ingestion & Referential Integrity Constraints
- **MANDATE**: When processing raw MDIC microdata (`IMP_YYYY.csv` / `EXP_YYYY.csv`), stream-parse and pre-aggregate records sharing the identical canonical 8-attribute natural tuple `(ano, mes, tipo_op, co_ncm, co_pais, sg_uf, co_via, co_urf)`, summing `statistical_quantity`, `net_weight_kg`, `fob_amount_usd`, `freight_amount_usd`, and `insurance_amount_usd` across different measurement units (`CO_UNID`) using strict `Decimal("0.0001")` (`ROUND_HALF_UP`) prior to calculating delimiter-safe `operation_hash_id = SHA-256(ano|mes|tipo_op|co_ncm|co_pais|sg_uf|co_via|co_urf)`.
- **!ANTI_PATTERN**: Never insert un-aggregated raw MDIC rows with identical 8-key attributes into `fact_comex_operations`, as differing `CO_UNID` lines will trigger primary key collisions on `operation_hash_id` or cause non-deterministic upsert overwrites.
- **MANDATE**: Prior to persisting batches into `fact_comex_operations`, defensively ensure dimensional referential integrity via Early-Arriving Dimensions auto-upsert (`INSERT INTO dim_date ... ON CONFLICT (date_id) DO NOTHING`, `INSERT INTO dim_merchandise_ncm ... ON CONFLICT (ncm_code) DO NOTHING`, and `INSERT INTO dim_partner_country ... ON CONFLICT (country_code) DO NOTHING`) to guarantee zero foreign key constraint violations during asynchronous bulk ingestion.
- **!ANTI_PATTERN**: Never execute blind batch inserts into fact tables without verifying that parent foreign keys exist in `dim_date`, `dim_merchandise_ncm`, and `dim_partner_country`, which leads to pipeline halting and hard foreign key rollbacks.
- **MANDATE**: For export microdata declarations (`EXP_YYYY.csv`) where MDIC does not publish international freight and insurance (`VL_FRETE`, `VL_SEGURO`), default both amounts strictly to `Decimal("0.0000")` and set `cif_amount_usd = fob_amount_usd` to satisfy customs valuation consistency.
- **!ANTI_PATTERN**: Never impute non-zero or arbitrary estimates for export freight and insurance, and never leave `cif_amount_usd` null or lower than `fob_amount_usd`.
- **MANDATE**: Automatically value the internalized inventory cost `landed_cost_brl` by multiplying `cif_amount_usd` by the latest USD selling rate on or before the statistical reference month (`WHERE currency_code = 'USD' AND date_id <= target_date_id ORDER BY date_id DESC LIMIT 1`) obtained from PostgreSQL `dim_currency_ptax` or DuckDB Parquet (falling back to the statutory default only when no prior observation exists, never looking ahead into future dates), rounded to `Decimal("0.0001")` using `ROUND_HALF_UP`.
- **!ANTI_PATTERN**: Never persist unvalued or zero-valued `landed_cost_brl` for valid import customs transactions, and never use float arithmetic to compute BRL landed cost from USD CIF.

---

## 10. Brazilian Cascading Tax Engine, ICMS "Cálculo por Dentro" & CPC 16 Inventory Valuation

### Paired Customs Tax Calculation & Accounting Segregation Constraints
- **MANDATE**: Compute Brazilian import cascading taxes sequentially in strict statutory order using `Decimal("0.0001")` and `ROUND_HALF_UP`:
  1. $\text{Customs Value (Valor Aduaneiro, } \text{VA}_{\text{BRL}}\text{)} = \text{ROUND}(\text{CIF}_{\text{USD}} \times \text{PTAX}_{\text{rate}}, 4)$
  2. $\text{II}_{\text{BRL}} = \text{ROUND}(\text{VA}_{\text{BRL}} \times \text{ii\_rate}, 4)$
  3. $\text{IPI}_{\text{BRL}} = \text{ROUND}((\text{VA}_{\text{BRL}} + \text{II}_{\text{BRL}}) \times \text{ipi\_rate}, 4)$
  4. $\text{PIS}_{\text{BRL}} = \text{ROUND}(\text{VA}_{\text{BRL}} \times \text{pis\_rate}, 4)$
  5. $\text{COFINS}_{\text{BRL}} = \text{ROUND}(\text{VA}_{\text{BRL}} \times \text{cofins\_rate}, 4)$
  6. $\text{ICMS Base Grossed-up ("Cálculo por Dentro")} = \text{ROUND}\left(\frac{\text{VA}_{\text{BRL}} + \text{II}_{\text{BRL}} + \text{IPI}_{\text{BRL}} + \text{PIS}_{\text{BRL}} + \text{COFINS}_{\text{BRL}} + \text{Siscomex}_{\text{BRL}} + \text{AFRMM}_{\text{BRL}}}{1 - \text{icms\_rate}}, 4\right)$
  7. $\text{ICMS}_{\text{BRL}} = \text{ROUND}(\text{ICMS Base}_{\text{BRL}} \times \text{icms\_rate}, 4)$
  8. $\text{Total Taxes}_{\text{BRL}} = \text{II}_{\text{BRL}} + \text{IPI}_{\text{BRL}} + \text{PIS}_{\text{BRL}} + \text{COFINS}_{\text{BRL}} + \text{Siscomex}_{\text{BRL}} + \text{AFRMM}_{\text{BRL}} + \text{ICMS}_{\text{BRL}}$
  9. $\text{Landed Cost}_{\text{BRL}} = \text{VA}_{\text{BRL}} + \text{Total Taxes}_{\text{BRL}}$
- **!ANTI_PATTERN**: Never calculate ICMS "por fora" (direct multiplication without grossing up the divisor $(1 - \text{icms\_rate})$) and never omit IPI, PIS, COFINS, Siscomex, or AFRMM from the ICMS taxable base, which constitutes a severe fiscal under-recollection violation.
- **MANDATE**: In compliance with CPC 16 (R1) / IAS 2 (*Estoques*), segregate nationalized import costs by corporate tax regime:
  - Under non-cumulative regime (`tax_regime = 'LUCRO_REAL'`):
    - Capitalize non-recoverable customs duties into inventory asset: $\text{net\_inventory\_cost\_brl} = \text{VA}_{\text{BRL}} + \text{II}_{\text{BRL}} + \text{Siscomex}_{\text{BRL}} + \text{AFRMM}_{\text{BRL}}$ booked to `1.1.3.02` (*Estoques de Mercadorias Nacionalizadas - Disponível*).
    - Segregate recoverable tax credits into recoverable tax asset: $\text{recoverable\_taxes\_brl} = \text{IPI}_{\text{BRL}} + \text{PIS}_{\text{BRL}} + \text{COFINS}_{\text{BRL}} + \text{ICMS}_{\text{BRL}}$ booked to `1.1.4.01` (*Tributos a Recuperar - IPI/PIS/COFINS/ICMS sobre Importação*).
    - Clear inventory in transit by crediting $\text{VA}_{\text{BRL}}$ to `1.1.3.01` (*Estoques em Trânsito*), and recognize tax liability by crediting $\text{Total Taxes}_{\text{BRL}}$ to `2.1.2.01` (*Obrigações Tributárias e Aduaneiras a Recolher - DARF/GNRE*).
  - Under cumulative / non-credit regimes (`tax_regime = 'CUSTO_INTEGRAL'`):
    - Fully capitalize all taxes: $\text{net\_inventory\_cost\_brl} = \text{landed\_cost\_brl}$ and $\text{recoverable\_taxes\_brl} = 0.0000$.
- **!ANTI_PATTERN**: Never capitalize recoverable taxes (IPI, PIS, COFINS, ICMS) into inventory under `LUCRO_REAL` (violating CPC 16 Item 11), and never post unsegregated journal entries that blend recoverable tax credits into merchandise inventory accounts.

---

## 11. Closed-Loop HITL Quarantine Governance & Promotion Invariants

### Paired HITL Quarantine Resolution & Promotion Constraints
- **MANDATE**: Enforce a strict finite state machine for the Human-in-the-Loop quarantine queue (`comex_quarantine_hitl`): an item enters as `PENDING_REVIEW` and transitions monotonically to either `APPROVED_OVERRIDE` or `REJECTED_HITL`. Once finalized (`review_status != 'PENDING_REVIEW'`), the review decision is strictly immutable via both application validation and PostgreSQL trigger `trg_enforce_quarantine_hitl_immutability`; any subsequent modification or re-resolution attempt must be rejected with an immediate conflict exception (`HTTP 409 Conflict` / `ValueError`).
- **!ANTI_PATTERN**: Never permit re-opening, modifying, or re-resolving an already finalized quarantine record in place, which violates the statutory customs audit trail and breaks deterministic downstream ledger reconciliation.
- **MANDATE**: When resolving an invoice quarantine record via `APPROVED_OVERRIDE`, the reviewer must provide a corrected payload that strictly passes `CommercialInvoiceInput` contract validation (including the Anti-Float rule, B2B corporate tax ID verification, Incoterm explicit separation, horizontal line math, and vertical closure within $\pm 0.0001$). If validation fails, the transaction must abort immediately without altering the `PENDING_REVIEW` quarantine state.
- **!ANTI_PATTERN**: Never promote or force-commit an unvalidated or mathematically broken payload into `commercial_invoices` during human override, and never bypass Pydantic V2 shift-left contract guards under the guise of manual review.
- **MANDATE**: Execute invoice quarantine promotion within an atomic ACID transaction that simultaneously: (1) upserts the corrected invoice into `commercial_invoices`, (2) generates and posts the balanced double-entry accounting entry into `journal_entries` and `journal_lines` ($\sum \text{Debit} - \sum \text{Credit} = 0.0000$), (3) updates `comex_quarantine_hitl` with `review_status = 'APPROVED_OVERRIDE'`, non-empty auditor identity (`reviewed_by`), and UTC timestamp (`reviewed_at`), and (4) updates `ingestion_idempotency_registry` to `status = 'RESOLVED_OVERRIDE'` for both the original rejected hash and the corrected payload hash.
- **!ANTI_PATTERN**: Never write partial promotion state where an invoice is inserted into `commercial_invoices` without corresponding balanced double-entry ledger lines, or where the quarantine status is updated without updating the edge idempotency registry.
- **MANDATE**: When resolving an invoice quarantine record via `REJECTED_HITL`, update `comex_quarantine_hitl` with `review_status = 'REJECTED_HITL'`, mandatory non-empty auditor ID (`reviewed_by`, hashed via salted SHA-256 with `PII_HASH_SALT` before persistence), and UTC timestamp (`reviewed_at`), while recording `status = 'REJECTED_HITL'` in `ingestion_idempotency_registry` to reject duplicate ingestion re-attempts with HTTP `409 Conflict`.
- **!ANTI_PATTERN**: Never silently drop or leave rejected records without auditor attribution, never persist raw unhashed auditor email/name PII in `reviewed_by`, and never permit unrecorded rejected payload hashes to repeatedly enter the ingestion pipeline.
- **MANDATE**: Protect all operational V2 API endpoints with fail-closed `X-API-Key` authentication (`API_KEY` for standard ingest/read; `AUDITOR_API_KEY` for privileged `POST /api/v2/quarantine/{id}/resolve` and `?persist=true` nationalizations) and dynamic per-IP rate limiting (`RATE_LIMIT_PER_MINUTE`), returning sanitized JSON errors without raw tracebacks or database credentials.
- **!ANTI_PATTERN**: Never expose mutating or quarantine endpoints without `X-API-Key` authorization checks, and never serialize Python stack traces, Mongo URIs, or PostgreSQL DSNs in API error payloads or `/health` responses.


