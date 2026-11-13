-- ==============================================================================
-- 06: Auditoria da Memória de Cálculo de Tributos em Cascata e CPC 16 (DBeaver)
-- Banco: comex_fiduciary_db (PostgreSQL 16)
-- Escopo: II, IPI, PIS/COFINS, Siscomex, AFRMM, ICMS "Cálculo por Dentro" e Segregação CPC 16
-- ==============================================================================

-- 1. Visão Geral das Nacionalizações Aduaneiras e Desdobramento de Tributos
SELECT 
    cn.declaration_number,
    cn.clearance_date,
    cn.importer_tax_id,
    cn.tax_regime,
    cn.currency_code,
    cn.cif_amount AS cif_estrangeiro,
    cn.ptax_exchange_rate AS ptax_desembaraco,
    cn.customs_value_brl AS valor_aduaneiro_brl,
    cn.ii_amount_brl AS ii_brl,
    cn.ipi_amount_brl AS ipi_brl,
    cn.pis_amount_brl AS pis_brl,
    cn.cofins_amount_brl AS cofins_brl,
    cn.siscomex_fee_brl AS taxa_siscomex_brl,
    cn.afrmm_brl AS afrmm_brl,
    cn.icms_base_brl AS icms_base_por_dentro,
    cn.icms_amount_brl AS icms_brl,
    cn.total_taxes_brl AS total_tributos_brl,
    cn.landed_cost_brl AS landed_cost_total_brl,
    cn.net_inventory_cost_brl AS estoque_ativo_cpc16,
    cn.recoverable_taxes_brl AS tributos_recuperaveis_ativo,
    cn.journal_entry_id
FROM customs_nationalizations cn
ORDER BY cn.clearance_date DESC;

-- 2. Prova de Fogo: Fechamento Aritmético e Invariantes Aduaneiros
-- EXPECTATIVA: ZERO linhas retornadas! Se houver retorno, há divergência de arredondamento.
SELECT 
    cn.declaration_number,
    cn.tax_regime,
    ABS(cn.cif_amount - (cn.fob_amount + cn.freight_amount + cn.insurance_amount)) AS delta_cif_closure,
    ABS(cn.total_taxes_brl - (cn.ii_amount_brl + cn.ipi_amount_brl + cn.pis_amount_brl + cn.cofins_amount_brl + cn.siscomex_fee_brl + cn.afrmm_brl + cn.icms_amount_brl)) AS delta_fechamento_tributos,
    ABS(cn.landed_cost_brl - (cn.customs_value_brl + cn.total_taxes_brl)) AS delta_landed_cost,
    ABS(cn.landed_cost_brl - (cn.net_inventory_cost_brl + cn.recoverable_taxes_brl)) AS delta_cpc16_segregacao
FROM customs_nationalizations cn
WHERE 
    ABS(cn.cif_amount - (cn.fob_amount + cn.freight_amount + cn.insurance_amount)) > 0.0001
    OR ABS(cn.total_taxes_brl - (cn.ii_amount_brl + cn.ipi_amount_brl + cn.pis_amount_brl + cn.cofins_amount_brl + cn.siscomex_fee_brl + cn.afrmm_brl + cn.icms_amount_brl)) > 0.0001
    OR ABS(cn.landed_cost_brl - (cn.customs_value_brl + cn.total_taxes_brl)) > 0.0001
    OR ABS(cn.landed_cost_brl - (cn.net_inventory_cost_brl + cn.recoverable_taxes_brl)) > 0.0001;

-- 3. Conciliação Fiduciária: Nacionalização vs Razão Contábil (Partidas Dobradas)
WITH ledger_agg AS (
    SELECT 
        jl.entry_id,
        SUM(CASE WHEN jl.account_code = '1.1.3.02' THEN jl.debit_amount ELSE 0.0000 END) AS debito_estoque_nacionalizado,
        SUM(CASE WHEN jl.account_code = '1.1.4.01' THEN jl.debit_amount ELSE 0.0000 END) AS debito_tributos_recuperar,
        SUM(CASE WHEN jl.account_code = '1.1.3.01' THEN jl.credit_amount ELSE 0.0000 END) AS credito_estoque_transito,
        SUM(CASE WHEN jl.account_code = '2.1.2.01' THEN jl.credit_amount ELSE 0.0000 END) AS credito_obrigacoes_aduaneiras,
        SUM(jl.debit_amount) AS total_debitos_lancamento,
        SUM(jl.credit_amount) AS total_creditos_lancamento
    FROM journal_lines jl
    GROUP BY jl.entry_id
)
SELECT 
    cn.declaration_number,
    cn.tax_regime,
    cn.net_inventory_cost_brl AS cpc16_estoque_esperado,
    la.debito_estoque_nacionalizado AS razao_debito_1_1_3_02,
    cn.recoverable_taxes_brl AS cpc16_tributos_esperado,
    la.debito_tributos_recuperar AS razao_debito_1_1_4_01,
    cn.customs_value_brl AS valor_aduaneiro_esperado,
    la.credito_estoque_transito AS razao_credito_1_1_3_01,
    cn.total_taxes_brl AS tributos_pagar_esperado,
    la.credito_obrigacoes_aduaneiras AS razao_credito_2_1_2_01,
    ABS(la.total_debitos_lancamento - la.total_creditos_lancamento) AS delta_soma_zero
FROM customs_nationalizations cn
JOIN ledger_agg la ON cn.journal_entry_id = la.entry_id
ORDER BY cn.clearance_date;
