-- ==============================================================================
-- Schema 02: Star Schema Analítico para BI & Comex Stat (Serving Layer)
-- Boilerplate HTAP (Profile 4) — PostgreSQL 16
-- ==============================================================================

-- 1. Dimensão Calendário
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT PRIMARY KEY, -- Formato YYYYMMDD
    full_date DATE UNIQUE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    quarter INT NOT NULL,
    is_business_day BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_date_full ON dim_date(full_date);

-- 2. Dimensão Mercadorias NCM
CREATE TABLE IF NOT EXISTS dim_merchandise_ncm (
    ncm_code VARCHAR(8) PRIMARY KEY,
    hs_code VARCHAR(6) NOT NULL,
    description TEXT NOT NULL,
    unit_measure VARCHAR(30) NOT NULL DEFAULT 'KG',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_ncm_hs ON dim_merchandise_ncm(hs_code);

-- 3. Dimensão Países Parceiros
CREATE TABLE IF NOT EXISTS dim_partner_country (
    country_code INT PRIMARY KEY,
    country_iso3 CHAR(3) NOT NULL,
    country_name VARCHAR(100) NOT NULL,
    economic_block VARCHAR(50),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 4. Dimensão Câmbio PTAX (Séries Históricas Diárias com LOCF Forward-Fill)
CREATE TABLE IF NOT EXISTS dim_currency_ptax (
    date_id INT NOT NULL REFERENCES dim_date(date_id),
    currency_code CHAR(3) NOT NULL,
    ptax_buy_rate NUMERIC(19, 6) NOT NULL,
    ptax_sell_rate NUMERIC(19, 6) NOT NULL,
    is_business_day BOOLEAN NOT NULL,
    is_interpolated_locf BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_id, currency_code)
);

CREATE INDEX IF NOT EXISTS idx_ptax_date_curr ON dim_currency_ptax(date_id, currency_code);

-- 5. Tabela Fato de Operações Aduaneiras (Microdados MDIC)
CREATE TABLE IF NOT EXISTS fact_comex_operations (
    operation_hash_id CHAR(64) PRIMARY KEY,
    date_id INT NOT NULL REFERENCES dim_date(date_id),
    operation_type VARCHAR(10) NOT NULL CHECK (operation_type IN ('IMPORT', 'EXPORT')),
    ncm_code VARCHAR(8) NOT NULL REFERENCES dim_merchandise_ncm(ncm_code),
    country_code INT NOT NULL REFERENCES dim_partner_country(country_code),
    uf_code CHAR(2) NOT NULL,
    modal_code INT NOT NULL,
    urf_code INT NOT NULL,
    statistical_quantity NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    net_weight_kg NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    fob_amount_usd NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    freight_amount_usd NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    insurance_amount_usd NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    cif_amount_usd NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    landed_cost_brl NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_comex_date ON fact_comex_operations(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_comex_ncm ON fact_comex_operations(ncm_code);
CREATE INDEX IF NOT EXISTS idx_fact_comex_country ON fact_comex_operations(country_code);
