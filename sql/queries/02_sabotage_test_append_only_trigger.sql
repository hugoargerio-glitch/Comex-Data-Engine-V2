-- ==============================================================================
-- 02: Teste de Sabotagem Controlada da Trigger de Imutabilidade (Append-Only)
-- Ferramenta: DBeaver (PostgreSQL 16 - comex_fiduciary_db)
-- Objetivo: Demonstrar que NINGUÉM (nem mesmo via SQL direto) consegue adulterar
--           lançamentos contábeis já postados. A correção exige Estorno (Reversal Entry).
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- TESTE DE SABOTAGEM 1: Tentar alterar um valor de débito em journal_lines
-- EXPECTATIVA: O PostgreSQL deve abortar com ERRO:
-- "ERROR: Fiduciary Ledger Violation: journal_lines is strictly append-only."
-- ------------------------------------------------------------------------------
-- EXECUTE A LINHA ABAIXO PARA VER A TRIGGER BARRAR:
/*
UPDATE journal_lines 
SET debit_amount = 999999.0000 
WHERE line_id = (SELECT line_id FROM journal_lines LIMIT 1);
*/

-- ------------------------------------------------------------------------------
-- TESTE DE SABOTAGEM 2: Tentar deletar uma linha do razão
-- EXPECTATIVA: O PostgreSQL deve abortar com ERRO:
-- "ERROR: Fiduciary Ledger Violation: journal_lines is strictly append-only."
-- ------------------------------------------------------------------------------
/*
DELETE FROM journal_lines 
WHERE line_id = (SELECT line_id FROM journal_lines LIMIT 1);
*/

-- ------------------------------------------------------------------------------
-- TESTE DE SABOTAGEM 3: Tentar deletar um cabeçalho de lançamento postado
-- EXPECTATIVA: O PostgreSQL deve abortar com ERRO:
-- "ERROR: Fiduciary Ledger Violation: Deleting journal entries is strictly prohibited."
-- ------------------------------------------------------------------------------
/*
DELETE FROM journal_entries 
WHERE entry_id = (SELECT entry_id FROM journal_entries WHERE status = 'POSTED' LIMIT 1);
*/

-- ------------------------------------------------------------------------------
-- MODO CORRETO (ESTORNO FIDUCIÁRIO / REVERSAL ENTRY):
-- A única forma permitida é criar um novo lançamento invertendo Débito <-> Crédito
-- e referenciando o entry_id original.
-- ------------------------------------------------------------------------------
SELECT 
    je.entry_id AS lancamento_id,
    je.status,
    je.description,
    je.reversed_from_entry_id,
    jl.account_code,
    jl.debit_amount,
    jl.credit_amount
FROM journal_entries je
JOIN journal_lines jl ON je.entry_id = jl.entry_id
ORDER BY je.created_at DESC, jl.line_order ASC;
