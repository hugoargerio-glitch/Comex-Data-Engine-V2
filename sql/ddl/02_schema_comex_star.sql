-- ==============================================================================
-- Schema 02: Star Schema Analítico para BI & Comex Stat (Serving Layer)
-- Comex Fiduciary Engine V2 — PostgreSQL 16
-- ==============================================================================

-- 1. Dimensão Calendário
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT PRIMARY KEY CHECK (date_id BETWEEN 19000101 AND 29991231), -- Formato YYYYMMDD
    full_date DATE UNIQUE NOT NULL,
    year INT NOT NULL CHECK (year BETWEEN 1900 AND 2999),
    month INT NOT NULL CHECK (month BETWEEN 1 AND 12),
    day INT NOT NULL CHECK (day BETWEEN 1 AND 31),
    quarter INT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    is_business_day BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_dim_date_id_parts CHECK (date_id = (year * 10000 + month * 100 + day))
);

-- Drop redundant index covered by full_date UNIQUE constraint if left over from older DBs
DROP INDEX IF EXISTS idx_dim_date_full;

-- 2. Dimensão Mercadorias NCM
CREATE TABLE IF NOT EXISTS dim_merchandise_ncm (
    ncm_code VARCHAR(8) PRIMARY KEY CHECK (ncm_code ~ '^\d{8}$'),
    hs_code VARCHAR(6) NOT NULL CHECK (hs_code ~ '^\d{6}$'),
    description TEXT NOT NULL CHECK (length(btrim(description)) > 0),
    unit_measure VARCHAR(30) NOT NULL DEFAULT 'KG' CHECK (length(btrim(unit_measure)) > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_ncm_hs ON dim_merchandise_ncm(hs_code);

-- 3. Dimensão Países Parceiros
CREATE TABLE IF NOT EXISTS dim_partner_country (
    country_code INT PRIMARY KEY CHECK (country_code >= 1),
    country_iso3 CHAR(3) NOT NULL CHECK (country_iso3 ~ '^[A-Z]{3}$'),
    country_name VARCHAR(100) NOT NULL CHECK (length(btrim(country_name)) > 0),
    economic_block VARCHAR(50),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 4. Dimensão Câmbio PTAX (Séries Históricas Diárias com LOCF Forward-Fill)
CREATE TABLE IF NOT EXISTS dim_currency_ptax (
    date_id INT NOT NULL REFERENCES dim_date(date_id),
    currency_code CHAR(3) NOT NULL CHECK (currency_code ~ '^[A-Z]{3}$'),
    ptax_buy_rate NUMERIC(19, 6) NOT NULL CHECK (ptax_buy_rate > 0.000000),
    ptax_sell_rate NUMERIC(19, 6) NOT NULL CHECK (ptax_sell_rate > 0.000000),
    is_business_day BOOLEAN NOT NULL,
    is_interpolated_locf BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_id, currency_code),
    CONSTRAINT chk_ptax_positive_and_spread CHECK (
        ptax_buy_rate > 0.000000 AND ptax_sell_rate >= ptax_buy_rate
    )
);

-- Drop redundant index covered by PRIMARY KEY (date_id, currency_code) if left over from older DBs
DROP INDEX IF EXISTS idx_ptax_date_curr;

-- 5. Tabela Fato de Operações Aduaneiras (Microdados MDIC)
CREATE TABLE IF NOT EXISTS fact_comex_operations (
    operation_hash_id CHAR(64) PRIMARY KEY CHECK (operation_hash_id ~ '^[0-9a-fA-F]{64}$'),
    date_id INT NOT NULL REFERENCES dim_date(date_id),
    operation_type VARCHAR(10) NOT NULL CHECK (operation_type IN ('IMPORT', 'EXPORT')),
    ncm_code VARCHAR(8) NOT NULL REFERENCES dim_merchandise_ncm(ncm_code),
    country_code INT NOT NULL REFERENCES dim_partner_country(country_code),
    uf_code CHAR(2) NOT NULL CHECK (uf_code ~ '^[A-Z]{2}$'),
    modal_code INT NOT NULL CHECK (modal_code >= 1),
    urf_code INT NOT NULL CHECK (urf_code >= 1),
    statistical_quantity NUMERIC(19, 4) NOT NULL DEFAULT 0.0000 CHECK (statistical_quantity >= 0.0000),
    net_weight_kg NUMERIC(19, 4) NOT NULL DEFAULT 0.0000 CHECK (net_weight_kg >= 0.0000),
    fob_amount_usd NUMERIC(19, 4) NOT NULL DEFAULT 0.0000 CHECK (fob_amount_usd >= 0.0000),
    freight_amount_usd NUMERIC(19, 4) NOT NULL DEFAULT 0.0000 CHECK (freight_amount_usd >= 0.0000),
    insurance_amount_usd NUMERIC(19, 4) NOT NULL DEFAULT 0.0000 CHECK (insurance_amount_usd >= 0.0000),
    cif_amount_usd NUMERIC(19, 4) NOT NULL DEFAULT 0.0000 CHECK (cif_amount_usd >= 0.0000),
    landed_cost_brl NUMERIC(19, 4) NOT NULL DEFAULT 0.0000 CHECK (landed_cost_brl >= 0.0000),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_fact_cif_ge_fob CHECK (cif_amount_usd >= fob_amount_usd)
);

CREATE INDEX IF NOT EXISTS idx_fact_comex_date ON fact_comex_operations(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_comex_ncm ON fact_comex_operations(ncm_code);
CREATE INDEX IF NOT EXISTS idx_fact_comex_country ON fact_comex_operations(country_code);
