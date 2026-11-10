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
-- EXPECTATIVA: ZERO linhas retornadas! Se retornar qualquer linha, houve quebra do razão.
SELECT 
    je.entry_id,
    je.competence_date,
    je.description,
    je.status,
    COUNT(jl.line_id) AS qtd_linhas,
    SUM(jl.debit_amount) AS soma_debitos,
    SUM(jl.credit_amount) AS soma_creditos,
    ABS(SUM(jl.debit_amount) - SUM(jl.credit_amount)) AS delta_divergencia
FROM journal_entries je
JOIN journal_lines jl ON je.entry_id = jl.entry_id
GROUP BY je.entry_id, je.competence_date, je.description, je.status
HAVING ABS(SUM(jl.debit_amount) - SUM(jl.credit_amount)) > 0.0001;

-- 3. Fechamento Global do Razão
SELECT 
    COUNT(DISTINCT entry_id) AS total_transacoes_posted,
    COUNT(line_id) AS total_linhas_escrituradas,
    SUM(debit_amount) AS soma_global_debitos,
    SUM(credit_amount) AS soma_global_creditos,
    (SUM(debit_amount) - SUM(credit_amount)) AS diferenca_final_zero_absoluto
FROM journal_lines;
