-- ==============================================================================
-- 01: Auditoria do Balancete Contábil & Soma Zero do Razão (General Ledger)
-- Ferramenta: DBeaver (PostgreSQL 16 - comex_fiduciary_db)
-- Invariante: SUM(debit_amount) - SUM(credit_amount) == 0.0000
-- ==============================================================================

-- 1. Balancete de Verificação por Conta Contábil (Trial Balance)
SELECT 
    a.account_code,
    a.account_name,
    a.account_type,
    a.normal_balance,
    COUNT(jl.line_id) AS total_lancamentos,
    COALESCE(SUM(jl.debit_amount), 0.0000) AS total_debitos,
    COALESCE(SUM(jl.credit_amount), 0.0000) AS total_creditos,
    CASE 
        WHEN a.normal_balance = 'DEBIT' 
            THEN COALESCE(SUM(jl.debit_amount - jl.credit_amount), 0.0000)
        ELSE 
            COALESCE(SUM(jl.credit_amount - jl.debit_amount), 0.0000)
    END AS saldo_atual
FROM accounts a
LEFT JOIN journal_lines jl ON a.account_code = jl.account_code
GROUP BY a.account_code, a.account_name, a.account_type, a.normal_balance
ORDER BY a.account_code;

-- 2. Prova de Fogo: Auditoria de Soma Zero Transação por Transação (Zero-Sum Invariant)
-- EXPECTATIVA: ZERO linhas retornadas! Usa LEFT JOIN e verifica status = 'POSTED' para detectar cabeçalhos com 0 ou 1 linha e desequilíbrios.
SELECT 
    je.entry_id,
    je.competence_date,
    je.description,
    je.status,
    COUNT(jl.line_id) AS qtd_linhas,
    COALESCE(SUM(jl.debit_amount), 0.0000) AS soma_debitos,
    COALESCE(SUM(jl.credit_amount), 0.0000) AS soma_creditos,
    ABS(COALESCE(SUM(jl.debit_amount), 0.0000) - COALESCE(SUM(jl.credit_amount), 0.0000)) AS delta_divergencia
FROM journal_entries je
LEFT JOIN journal_lines jl ON je.entry_id = jl.entry_id
WHERE je.status = 'POSTED'
GROUP BY je.entry_id, je.competence_date, je.description, je.status
HAVING COUNT(jl.line_id) < 2
    OR ABS(COALESCE(SUM(jl.debit_amount), 0.0000) - COALESCE(SUM(jl.credit_amount), 0.0000)) > 0.0001;

-- 3. Fechamento Global do Razão
SELECT 
    COUNT(DISTINCT je.entry_id) FILTER (WHERE je.status = 'POSTED') AS total_transacoes_posted,
    COUNT(DISTINCT je.entry_id) FILTER (WHERE je.status = 'REVERSED') AS total_transacoes_reversed,
    COUNT(jl.line_id) AS total_linhas_escrituradas,
    COALESCE(SUM(jl.debit_amount), 0.0000) AS soma_global_debitos,
    COALESCE(SUM(jl.credit_amount), 0.0000) AS soma_global_creditos,
    COALESCE(SUM(jl.debit_amount) - SUM(jl.credit_amount), 0.0000) AS diferenca_final_zero_absoluto
FROM journal_entries je
LEFT JOIN journal_lines jl ON je.entry_id = jl.entry_id;
