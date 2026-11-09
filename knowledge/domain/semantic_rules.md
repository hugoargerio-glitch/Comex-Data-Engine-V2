---
okf_version: "1.0"
document_id: "domain-semantic-rules-ecommerce-finops-v1"
domain: "ecommerce_finops"
owners: ["data-architecture-team", "finops-governance"]
linked_contracts:
  - "./contracts/orders_silver_v1.odcs.yaml"
last_verified: "2026-11-04"
---

# Canonical Domain Semantic Rules — E-commerce & FinOps (`semantic_rules.md`)

This **Open Knowledge Format (OKF)** document defines the institutional single source of truth for financial calculations, entity granularity, fan-out prevention, and PII anonymization in `de-agentic-boilerplate`. Every subagent (`planner`, `worker`, `auditor`, `dataops`) must consult this file before proposing, implementing, or auditing SQL/PySpark models.

---

## 1. Entity Definitions & Granularity (Grain Matrix)

| Entity / Table | Layer | Atomic Grain | Primary Key | Partition Key | Relationship to `orders_silver` |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `orders_raw` | Bronze | 1 row per raw CDC event received | `event_id` | `ingestion_date` | `N:1` (multiple CDC versions per order) |
| `orders_silver` | Silver | **1 unique row per order** | `order_id` | `order_date` | `1:1` (Anchor entity) |
| `order_items_silver` | Silver | 1 row per line item / SKU within an order | `order_item_id` | `order_date` | `N:1` (`1:N` from `orders_silver`) |
| `payment_attempts_silver` | Silver | 1 row per payment gateway charge attempt | `attempt_id` | `attempt_date` | `N:1` (`1:N` from `orders_silver`) |
| `fct_daily_revenue` | Gold | 1 row per `order_date` + `currency_code` + `payment_method_type` | `revenue_grain_sk` | `order_date` | Aggregated from `orders_silver` |

### Paired Granularity & JOIN Constraints
- **MANDATE**: Before joining `orders_silver` with `order_items_silver` or `payment_attempts_silver`, pre-aggregate the child table inside a CTE grouped by `order_id` (`GROUP BY order_id`) and verify uniqueness.
- **!ANTI_PATTERN**: Never execute `LEFT JOIN order_items_silver USING (order_id)` directly in a query that sums header-level `gross_order_amount` or `net_order_amount`, as an order with $k$ line items will silently multiply revenue by $k$ (Silent JOIN Fan-Out).
- **MANDATE**: Deduplicate Bronze CDC events using `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_updated_at DESC, event_id DESC) = 1`.
- **!ANTI_PATTERN**: Never use `SELECT DISTINCT` as a substitute for deterministic window-based CDC deduplication (`ROW_NUMBER`).

---

## 2. Financial Precision & Canonical Formulas (FinOps)

### Paired Typing & Monetary Arithmetic Constraints
- **MANDATE**: Store and compute all monetary values as `DECIMAL(18,4)` alongside their integer cents representation (`BIGINT`) when provided upstream.
- **!ANTI_PATTERN**: Never cast monetary columns to `FLOAT`, `DOUBLE`, or `REAL` in PySpark or SQL, preventing IEEE 754 binary floating-point rounding drift during accounting reconciliation.
- **MANDATE**: Wrap all optional discount, tax, and shipping columns in `COALESCE(col, CAST(0.0000 AS DECIMAL(18,4)))` prior to arithmetic operations.
- **!ANTI_PATTERN**: Never perform direct addition or subtraction on nullable financial columns without explicit `COALESCE`, preventing silent `NULL` propagation in `net_order_amount`.

### Verified Accounting Equations
1. **Order Accounting Identity (`net_order_amount`)**:
   $$\text{net\_order\_amount} = \text{gross\_order\_amount} - \text{discount\_amount} + \text{tax\_amount} + \text{shipping\_amount}$$
   - Maximum permitted reconciliation delta in contract tests: $0.0000$ (`ABS(...) <= 0.0001`).

2. **Gross Merchandise Value (`gmv_amount`)**:
   - Sum of `gross_order_amount` across all valid non-test orders submitted in the period, regardless of subsequent cancellation status.

3. **Official Recognized Net Revenue (`recognized_net_revenue`)**:
   - Includes only orders whose payment has been captured and that have not been cancelled or refunded:
   ```sql
   SUM(
     CASE
       WHEN order_status IN ('PAID', 'SHIPPED', 'DELIVERED', 'COMPLETED')
        AND payment_status = 'CAPTURED'
       THEN net_order_amount
       ELSE CAST(0.0000 AS DECIMAL(18,4))
     END
   ) AS recognized_net_revenue
   ```

---

## 3. Temporal Normalization & Iceberg Partitioning

- **MANDATE**: Store all event timestamps (`order_created_at`, `order_updated_at`, `_ingested_at`) strictly in `UTC` (`TIMESTAMP WITH TIME ZONE` / `TIMESTAMPTZ`).
- **!ANTI_PATTERN**: Never store timezone-naive local timestamps or mix timezones across temporal audit columns.
- **MANDATE**: Derive the accounting partition column `order_date` by converting `order_created_at` to the official financial closing timezone (`America/Sao_Paulo`):
  ```sql
  CAST(order_created_at AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS order_date
  ```
- **!ANTI_PATTERN**: Never extract `CAST(order_created_at AS DATE)` directly in UTC for Brazilian financial closing, as orders placed between `21:00` and `23:59 BRT` would be misallocated to the next accounting day.

---

## 4. PII Governance & Anonymization (LGPD / GDPR)

- **MANDATE**: Apply salted cryptographic `SHA-256` hashing (`pii_hash_salt`) to all direct personal identifiers (`customer_email`, `customer_tax_id`) when transitioning from Bronze to Silver:
  ```sql
  CAST(sha256(lower(trim(customer_email)) || '{{ var("pii_hash_salt") }}') AS CHAR(64)) AS customer_email_hash
  ```
- **!ANTI_PATTERN**: Never hash PII without prior normalization (`lower(trim(...))`) or without a corporate salt (which leaves CPF/CNPJ and emails vulnerable to rainbow table attacks).
