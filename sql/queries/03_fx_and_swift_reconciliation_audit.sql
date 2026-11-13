-- ==============================================================================
-- 03: Auditoria de Variação Cambial Realizada (PTAX t1 vs Spot t2) & Tarifas SWIFT
-- Ferramenta: DBeaver (PostgreSQL 16 - comex_fiduciary_db)
-- Contas de Interesse:
--   - 3.1.2.01: Variação Cambial Ativa (Realized FX Gain)
--   - 4.1.3.01: Variação Cambial Passiva (Realized FX Loss)
--   - 4.1.2.01: Despesas Bancárias e Tarifas SWIFT
-- ==============================================================================

-- 1. Apuração de Ganhos e Perdas Cambiais Efetivados nas Liquidações
SELECT 
    je.entry_id,
    je.transaction_date AS data_liquidacao,
    je.competence_date AS data_competencia,
    je.reference_document_id AS documento_referencia,
    je.description AS historico_bancario,
    jl.account_code,
    a.account_name,
    jl.debit_amount,
    jl.credit_amount,
    jl.currency_code,
    jl.original_currency_amount,
    jl.exchange_rate_applied
FROM journal_entries je
JOIN journal_lines jl ON je.entry_id = jl.entry_id
JOIN accounts a ON jl.account_code = a.account_code
WHERE jl.account_code IN ('3.1.2.01', '4.1.3.01', '4.1.2.01')
ORDER BY je.transaction_date DESC, jl.account_code;

-- 2. Resumo Consolidado do Resultado Financeiro Cambial & Tarifário (líquido de eventuais estornos)
SELECT 
    COALESCE(SUM(CASE WHEN jl.account_code = '3.1.2.01' THEN (jl.credit_amount - jl.debit_amount) ELSE 0.0000 END), 0.0000) AS total_ganho_cambial_brl,
    COALESCE(SUM(CASE WHEN jl.account_code = '4.1.3.01' THEN (jl.debit_amount - jl.credit_amount) ELSE 0.0000 END), 0.0000) AS total_perda_cambial_brl,
    COALESCE(SUM(CASE WHEN jl.account_code = '4.1.2.01' THEN (jl.debit_amount - jl.credit_amount) ELSE 0.0000 END), 0.0000) AS total_tarifas_swift_brl,
    (
        COALESCE(SUM(CASE WHEN jl.account_code = '3.1.2.01' THEN (jl.credit_amount - jl.debit_amount) ELSE 0.0000 END), 0.0000)
        - COALESCE(SUM(CASE WHEN jl.account_code = '4.1.3.01' THEN (jl.debit_amount - jl.credit_amount) ELSE 0.0000 END), 0.0000)
        - COALESCE(SUM(CASE WHEN jl.account_code = '4.1.2.01' THEN (jl.debit_amount - jl.credit_amount) ELSE 0.0000 END), 0.0000)
    ) AS resultado_liquido_operacoes_cambiais_brl
FROM journal_lines jl;
