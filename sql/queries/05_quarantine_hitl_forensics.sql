-- ==============================================================================
-- 05: Análise Forense da Fila de Quarentena & Human-in-the-Loop (HITL)
-- Ferramenta: DBeaver (PostgreSQL 16 - comex_fiduciary_db)
-- Cruzamento: Obtenha o mongo_raw_id aqui e inspecione o JSON bruto no VS Code!
-- ==============================================================================

-- 1. Faturas Barradas na Borda por Quebra de Contrato ou Divergência Aritmética
SELECT 
    quarantine_id,
    source_type,
    raw_payload_hash,
    mongo_raw_id,
    self_correction_attempts,
    review_status,
    created_at,
    validation_errors
FROM comex_quarantine_hitl
WHERE review_status = 'PENDING_REVIEW'
ORDER BY created_at DESC;

-- 2. Diagnóstico Detalhado do Erro Reportado pelo Pydantic V2 (JSONB)
SELECT 
    quarantine_id,
    mongo_raw_id,
    err->>'type' AS tipo_erro_pydantic,
    err->>'loc' AS campo_divergente,
    err->>'msg' AS mensagem_violacao
FROM comex_quarantine_hitl,
LATERAL jsonb_array_elements(validation_errors) AS err
ORDER BY quarantine_id;
