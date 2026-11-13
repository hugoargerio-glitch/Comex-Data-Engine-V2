-- ==============================================================================
-- 04: Star Schema Analítico — Landed Cost por NCM, País, Modal e PTAX (LOCF)
-- Ferramenta: DBeaver (PostgreSQL 16 — Star Schema comex_fiduciary_db)
-- Nota: DuckDB contém apenas views Parquet de fact_comex_operations e dim_currency_ptax; dimensões completas residem no PostgreSQL.
-- Modelagem: fact_comex_operations + dim_merchandise_ncm + dim_partner_country + dim_date + dim_currency_ptax
-- ==============================================================================

-- 1. Análise de Custo Internalizado (Landed Cost) e Frete por NCM
SELECT 
    f.operation_type,
    f.ncm_code,
    COALESCE(dn.description, 'Nomenclatura Aduaneira Especial') AS descricao_ncm,
    dp.country_name AS pais_parceiro,
    f.uf_code AS estado_desembaraco,
    COUNT(f.operation_hash_id) AS total_declaracoes,
    SUM(f.net_weight_kg) AS peso_liquido_total_kg,
    SUM(f.fob_amount_usd) AS total_fob_usd,
    SUM(f.freight_amount_usd) AS total_frete_usd,
    SUM(f.insurance_amount_usd) AS total_seguro_usd,
    SUM(f.cif_amount_usd) AS total_cif_usd,
    SUM(f.landed_cost_brl) AS total_landed_cost_brl,
    ROUND(SUM(f.freight_amount_usd) / NULLIF(SUM(f.fob_amount_usd), 0) * 100, 2) AS percentual_frete_sobre_fob
FROM fact_comex_operations f
LEFT JOIN dim_merchandise_ncm dn ON f.ncm_code = dn.ncm_code
LEFT JOIN dim_partner_country dp ON f.country_code = dp.country_code
GROUP BY 
    f.operation_type, f.ncm_code, dn.description, dp.country_name, f.uf_code
ORDER BY total_landed_cost_brl DESC;

-- 2. Inspeção da Dimensão PTAX: Verificação do Preenchimento LOCF em Finais de Semana
-- Observe que sábado e domingo repetem a cotação da sexta anterior com is_interpolated_locf = true!
SELECT 
    d.date_id,
    d.full_date,
    p.currency_code,
    p.ptax_buy_rate,
    p.ptax_sell_rate,
    p.is_business_day,
    p.is_interpolated_locf
FROM dim_date d
JOIN dim_currency_ptax p ON d.date_id = p.date_id
ORDER BY p.currency_code, d.date_id ASC;
