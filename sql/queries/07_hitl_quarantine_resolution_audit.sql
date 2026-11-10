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
-- Cruza a quarentena resolvida com a fatura persistida em commercial_invoices e registro de idempotência
SELECT 
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
JOIN commercial_invoices ci 
    ON q.raw_payload_hash = ci.raw_payload_hash 
    OR q.mongo_raw_id = ci.mongo_raw_id
LEFT JOIN ingestion_idempotency_registry iir 
    ON q.raw_payload_hash = iir.payload_hash
WHERE q.review_status = 'APPROVED_OVERRIDE'
ORDER BY q.reviewed_at DESC;

-- 4. Reconciliação Contábil Fiduciária (Partidas Dobradas) de Faturas Promovidas via HITL
-- Verifica o balanceamento de soma zero nos lançamentos das faturas sobrepostas
WITH hitl_invoices AS (
    SELECT 
        q.quarantine_id,
        q.reviewed_by,
        q.reviewed_at,
        ci.invoice_id,
        ci.document_number,
        ci.total_amount AS invoice_total
    FROM comex_quarantine_hitl q
    JOIN commercial_invoices ci 
        ON q.raw_payload_hash = ci.raw_payload_hash 
        OR q.mongo_raw_id = ci.mongo_raw_id
    WHERE q.review_status = 'APPROVED_OVERRIDE'
)
SELECT 
    hi.quarantine_id,
    hi.document_number,
    hi.reviewed_by,
    hi.reviewed_at,
    je.entry_id AS journal_entry_id,
    je.transaction_date,
    je.status AS journal_status,
    jl.line_order,
    jl.account_code,
    a.account_name,
    jl.debit_amount,
    jl.credit_amount,
    jl.currency_code,
    jl.exchange_rate_applied
FROM hitl_invoices hi
JOIN journal_entries je 
    ON je.reference_document_id = hi.document_number
JOIN journal_lines jl 
    ON je.entry_id = jl.entry_id
LEFT JOIN accounts a 
    ON jl.account_code = a.account_code
ORDER BY hi.quarantine_id, jl.line_order;

-- 5. Invariante de Soma Zero Fiduciária dos Lançamentos Gerados por HITL Override
SELECT 
    je.entry_id,
    je.reference_document_id,
    SUM(jl.debit_amount) AS total_debits,
    SUM(jl.credit_amount) AS total_credits,
    (SUM(jl.debit_amount) - SUM(jl.credit_amount)) AS zero_sum_delta,
    CASE 
        WHEN ABS(SUM(jl.debit_amount) - SUM(jl.credit_amount)) <= 0.0001 THEN 'BALANCED_ACID'
        ELSE 'UNBALANCED_ERROR'
    END AS fiduciary_status
FROM journal_entries je
JOIN journal_lines jl ON je.entry_id = jl.entry_id
WHERE je.description LIKE 'HITL Override Ingestion%'
GROUP BY je.entry_id, je.reference_document_id;
