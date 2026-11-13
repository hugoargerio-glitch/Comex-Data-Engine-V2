-- ==============================================================================
-- Query 07: Auditoria de Quarentena e Resolução Human-in-the-Loop (HITL)
-- Sprint Real 4 — Comex Fiduciary Engine V2 (ODCS v3.2.0)
-- ==============================================================================

-- 1. Status Completo da Fila de Quarentena HITL com Trilha de Auditoria do Revisor
-- Retorna todos os registros em comex_quarantine_hitl com status, erros e dados do revisor
SELECT 
    q.quarantine_id,
    q.source_type,
    q.raw_payload_hash,
    q.mongo_raw_id,
    q.self_correction_attempts,
    q.review_status,
    q.reviewed_by,
    q.reviewed_at,
    q.created_at AS quarantined_at,
    q.validation_errors
FROM comex_quarantine_hitl q
ORDER BY q.created_at DESC;

-- 2. Métricas de Resumo da Governança de Quarentena HITL
SELECT 
    review_status,
    COUNT(*) AS total_records,
    COUNT(reviewed_by) AS total_reviewed,
    MIN(created_at) AS earliest_quarantine,
    MAX(reviewed_at) AS latest_review
FROM comex_quarantine_hitl
GROUP BY review_status
ORDER BY review_status;

-- 3. Reconciliação de Faturas Promovidas (APPROVED_OVERRIDE) no Core Operacional
-- Usa DISTINCT ON determinístico priorizando registry -> hash -> mongo_raw_id para eliminar fan-out cartesiano
WITH resolved_quarantine_invoices AS (
    SELECT DISTINCT ON (q.quarantine_id)
        q.quarantine_id,
        q.review_status,
        q.reviewed_by,
        q.reviewed_at,
        ci.invoice_id,
        ci.document_number,
        ci.issuer_tax_id,
        ci.issue_date,
        ci.currency_code,
        ci.subtotal_amount,
        ci.tax_amount,
        ci.freight_amount,
        ci.insurance_amount,
        ci.total_amount,
        ci.incoterm,
        iir.status AS edge_idempotency_status
    FROM comex_quarantine_hitl q
    LEFT JOIN ingestion_idempotency_registry iir
        ON iir.payload_hash = q.raw_payload_hash
    JOIN commercial_invoices ci
        ON ci.invoice_id::TEXT = iir.target_entity_id
        OR ci.raw_payload_hash = q.raw_payload_hash
        OR (q.mongo_raw_id IS NOT NULL AND ci.mongo_raw_id = q.mongo_raw_id)
    WHERE q.review_status = 'APPROVED_OVERRIDE'
    ORDER BY
        q.quarantine_id,
        CASE
            WHEN ci.invoice_id::TEXT = iir.target_entity_id THEN 1
            WHEN ci.raw_payload_hash = q.raw_payload_hash THEN 2
            ELSE 3
        END,
        ci.created_at DESC,
        ci.invoice_id
)
SELECT
    quarantine_id,
    review_status,
    reviewed_by,
    reviewed_at,
    invoice_id,
    document_number,
    issuer_tax_id,
    issue_date,
    currency_code,
    subtotal_amount,
    tax_amount,
    freight_amount,
    insurance_amount,
    total_amount,
    incoterm,
    edge_idempotency_status
FROM resolved_quarantine_invoices
ORDER BY reviewed_at DESC;

-- 4. Reconciliação Contábil Fiduciária (Partidas Dobradas) de Faturas Promovidas via HITL
-- Elimina fan-out cartesiano combinando chaves determinísticas (1 fatura e 1 lançamento HITL por quarentena) e CTE pré-agregada de journal_lines
WITH resolved_quarantine_invoices AS (
    SELECT DISTINCT ON (q.quarantine_id)
        q.quarantine_id,
        q.reviewed_by,
        q.reviewed_at,
        ci.invoice_id,
        ci.document_number,
        ci.total_amount AS invoice_total
    FROM comex_quarantine_hitl q
    LEFT JOIN ingestion_idempotency_registry iir
        ON iir.payload_hash = q.raw_payload_hash
    JOIN commercial_invoices ci
        ON ci.invoice_id::TEXT = iir.target_entity_id
        OR ci.raw_payload_hash = q.raw_payload_hash
        OR (q.mongo_raw_id IS NOT NULL AND ci.mongo_raw_id = q.mongo_raw_id)
    WHERE q.review_status = 'APPROVED_OVERRIDE'
    ORDER BY
        q.quarantine_id,
        CASE
            WHEN ci.invoice_id::TEXT = iir.target_entity_id THEN 1
            WHEN ci.raw_payload_hash = q.raw_payload_hash THEN 2
            ELSE 3
        END,
        ci.created_at DESC,
        ci.invoice_id
),
matched_hitl_entries AS (
    SELECT DISTINCT ON (rqi.quarantine_id)
        rqi.quarantine_id,
        rqi.document_number,
        rqi.reviewed_by,
        rqi.reviewed_at,
        rqi.invoice_total,
        je.entry_id AS journal_entry_id,
        je.transaction_date,
        je.status AS journal_status
    FROM resolved_quarantine_invoices rqi
    JOIN journal_entries je
        ON je.reference_document_id = rqi.document_number
       AND je.description LIKE 'HITL Override Ingestion%'
    ORDER BY
        rqi.quarantine_id,
        ABS(EXTRACT(EPOCH FROM (je.created_at - rqi.reviewed_at))) ASC,
        je.created_at DESC,
        je.entry_id
),
ledger_entry_totals AS (
    SELECT
        jl.entry_id,
        COUNT(jl.line_id) AS line_count,
        COALESCE(SUM(jl.debit_amount), 0.0000) AS total_debits,
        COALESCE(SUM(jl.credit_amount), 0.0000) AS total_credits,
        ABS(COALESCE(SUM(jl.debit_amount), 0.0000) - COALESCE(SUM(jl.credit_amount), 0.0000)) AS zero_sum_delta,
        STRING_AGG(DISTINCT jl.account_code, ', ' ORDER BY jl.account_code) AS account_codes
    FROM journal_lines jl
    GROUP BY jl.entry_id
)
SELECT
    mhe.quarantine_id,
    mhe.document_number,
    mhe.reviewed_by,
    mhe.reviewed_at,
    mhe.invoice_total,
    mhe.journal_entry_id,
    mhe.transaction_date,
    mhe.journal_status,
    COALESCE(let.line_count, 0) AS line_count,
    COALESCE(let.total_debits, 0.0000) AS total_debits,
    COALESCE(let.total_credits, 0.0000) AS total_credits,
    COALESCE(let.zero_sum_delta, 0.0000) AS zero_sum_delta,
    let.account_codes
FROM matched_hitl_entries mhe
LEFT JOIN ledger_entry_totals let
    ON mhe.journal_entry_id = let.entry_id
ORDER BY mhe.quarantine_id;

-- 5. Invariante de Soma Zero Fiduciária dos Lançamentos Gerados por HITL Override
WITH ledger_entry_totals AS (
    SELECT
        jl.entry_id,
        COUNT(jl.line_id) AS line_count,
        COALESCE(SUM(jl.debit_amount), 0.0000) AS total_debits,
        COALESCE(SUM(jl.credit_amount), 0.0000) AS total_credits
    FROM journal_lines jl
    GROUP BY jl.entry_id
)
SELECT
    je.entry_id,
    je.reference_document_id,
    COALESCE(let.line_count, 0) AS line_count,
    COALESCE(let.total_debits, 0.0000) AS total_debits,
    COALESCE(let.total_credits, 0.0000) AS total_credits,
    (COALESCE(let.total_debits, 0.0000) - COALESCE(let.total_credits, 0.0000)) AS zero_sum_delta,
    CASE
        WHEN COALESCE(let.line_count, 0) >= 2
         AND ABS(COALESCE(let.total_debits, 0.0000) - COALESCE(let.total_credits, 0.0000)) <= 0.0001 THEN 'BALANCED_ACID'
        ELSE 'UNBALANCED_ERROR'
    END AS fiduciary_status
FROM journal_entries je
LEFT JOIN ledger_entry_totals let ON je.entry_id = let.entry_id
WHERE je.description LIKE 'HITL Override Ingestion%';
