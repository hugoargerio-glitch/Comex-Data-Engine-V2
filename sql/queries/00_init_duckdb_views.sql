-- ==============================================================================
-- 00: Inicialização de Views no DuckDB Lakehouse
-- Ferramenta: DBeaver (Conexão: comex_lakehouse.duckdb)
-- Propósito: Criar Views permanentes mapeando os arquivos Apache Parquet particionados
-- ==============================================================================

-- 1. View da Tabela Fato de Operações Aduaneiras (Particionamento Hive year/month)
CREATE OR REPLACE VIEW fact_comex_operations AS 
SELECT * FROM read_parquet(
    '/home/Hugo/comex-data-engine-v2/data/parquet_lakehouse/fact_comex_operations/**/*.parquet', 
    hive_partitioning = 1
);

-- 2. View da Dimensão Câmbio PTAX (Séries Históricas Diárias com LOCF)
CREATE OR REPLACE VIEW dim_currency_ptax AS 
SELECT * FROM read_parquet(
    '/home/Hugo/comex-data-engine-v2/data/parquet_lakehouse/dim_currency_ptax/**/*.parquet', 
    hive_partitioning = 1
);

-- 3. Teste Analítico Rápido no DuckDB
SELECT 
    operation_type,
    ncm_code,
    statistical_quantity,
    fob_amount_usd,
    cif_amount_usd,
    landed_cost_brl,
    year,
    month
FROM fact_comex_operations;
