-- ==============================================================================
-- Schema 01: Core Fiduciário & Razão Contábil por Partidas Dobradas (ACID)
-- Boilerplate HTAP (Profile 4) — PostgreSQL 16
-- ==============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Registro de Idempotência de Borda (SHA-256 Fingerprinting)
CREATE TABLE IF NOT EXISTS ingestion_idempotency_registry (
    payload_hash CHAR(64) PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    target_entity_id VARCHAR(100),
    status VARCHAR(30) NOT NULL DEFAULT 'PROCESSED',
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2. Tabela de Faturas Comerciais Internacionais (Commercial Invoices)
CREATE TABLE IF NOT EXISTS commercial_invoices (
    invoice_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    issuer_tax_id VARCHAR(50) NOT NULL,
    document_number VARCHAR(100) NOT NULL,
    issue_date DATE NOT NULL,
    currency_code CHAR(3) NOT NULL,
    subtotal_amount NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    tax_amount NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    freight_amount NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    insurance_amount NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    total_amount NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    incoterm VARCHAR(3) NOT NULL CHECK (incoterm IN ('EXW', 'FOB', 'CIF', 'DDP')),
    extraction_confidence NUMERIC(5, 4) NOT NULL DEFAULT 1.0000,
    raw_payload_hash CHAR(64) NOT NULL,
    mongo_raw_id VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_invoice_natural_key UNIQUE (issuer_tax_id, document_number, issue_date, currency_code, total_amount),
    CONSTRAINT chk_invoice_balance CHECK (
        ABS(total_amount - (subtotal_amount + tax_amount + freight_amount + insurance_amount)) <= 0.0001
    )
);

CREATE INDEX IF NOT EXISTS idx_invoices_issue_date ON commercial_invoices(issue_date);
CREATE INDEX IF NOT EXISTS idx_invoices_tax_id ON commercial_invoices(issuer_tax_id);

-- 3. Plano de Contas Fiduciário (Chart of Accounts)
CREATE TABLE IF NOT EXISTS accounts (
    account_code VARCHAR(50) PRIMARY KEY,
    account_name VARCHAR(150) NOT NULL,
    account_type VARCHAR(20) NOT NULL CHECK (account_type IN ('ASSET', 'LIABILITY', 'EQUITY', 'REVENUE', 'EXPENSE')),
    normal_balance VARCHAR(10) NOT NULL CHECK (normal_balance IN ('DEBIT', 'CREDIT')),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Inserção de Contas Canônicas
INSERT INTO accounts (account_code, account_name, account_type, normal_balance) VALUES
('1.1.1.01', 'Caixa e Bancos Conta Movimento (BRL)', 'ASSET', 'DEBIT'),
('1.1.1.02', 'Bancos Conta Moeda Estrangeira (USD/CHF)', 'ASSET', 'DEBIT'),
('1.1.2.01', 'Clientes a Receber no Exterior (Exportação)', 'ASSET', 'DEBIT'),
('1.1.3.01', 'Estoques de Mercadorias em Trânsito (Importação)', 'ASSET', 'DEBIT'),
('2.1.1.01', 'Fornecedores Internacionais a Pagar', 'LIABILITY', 'CREDIT'),
('3.1.1.01', 'Receita Operacional Bruta de Exportação', 'REVENUE', 'CREDIT'),
('4.1.1.01', 'Custo das Mercadorias Importadas (Landed Cost)', 'EXPENSE', 'DEBIT'),
('4.1.2.01', 'Despesas Bancárias e Tarifas SWIFT', 'EXPENSE', 'DEBIT'),
('4.1.3.01', 'Variação Cambial Passiva (Realized FX Loss)', 'EXPENSE', 'DEBIT'),
('3.1.2.01', 'Variação Cambial Ativa (Realized FX Gain)', 'REVENUE', 'CREDIT'),
('4.1.4.01', 'Despesas Tributárias e Aduaneiras (IOF / Impostos)', 'EXPENSE', 'DEBIT'),
('1.1.3.02', 'Estoques de Mercadorias Nacionalizadas (Disponível)', 'ASSET', 'DEBIT'),
('1.1.4.01', 'Tributos a Recuperar - IPI/PIS/COFINS/ICMS sobre Importação', 'ASSET', 'DEBIT'),
('2.1.2.01', 'Obrigações Tributárias e Aduaneiras a Recolher (DARF/GNRE)', 'LIABILITY', 'CREDIT')
ON CONFLICT (account_code) DO NOTHING;

-- 4. Cabeçalho de Lançamentos Contábeis (Append-Only Journal Entries)
CREATE TABLE IF NOT EXISTS journal_entries (
    entry_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_date DATE NOT NULL,
    competence_date DATE NOT NULL,
    reference_document_id VARCHAR(100),
    description TEXT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'POSTED' CHECK (status IN ('POSTED', 'REVERSED')),
    reversed_from_entry_id UUID REFERENCES journal_entries(entry_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_journal_competence ON journal_entries(competence_date);
CREATE INDEX IF NOT EXISTS idx_journal_ref_doc ON journal_entries(reference_document_id);

-- 5. Linhas de Partidas Dobradas (Journal Lines)
CREATE TABLE IF NOT EXISTS journal_lines (
    line_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entry_id UUID NOT NULL REFERENCES journal_entries(entry_id) ON DELETE RESTRICT,
    account_code VARCHAR(50) NOT NULL REFERENCES accounts(account_code),
    debit_amount NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    credit_amount NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    currency_code CHAR(3) NOT NULL,
    original_currency_amount NUMERIC(19, 4) NOT NULL,
    exchange_rate_applied NUMERIC(19, 6) NOT NULL DEFAULT 1.000000,
    line_order INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_non_negative_amounts CHECK (debit_amount >= 0.0000 AND credit_amount >= 0.0000),
    CONSTRAINT chk_exclusive_debit_credit CHECK ((debit_amount > 0 AND credit_amount = 0) OR (credit_amount > 0 AND debit_amount = 0))
);

CREATE INDEX IF NOT EXISTS idx_journal_lines_entry ON journal_lines(entry_id);
CREATE INDEX IF NOT EXISTS idx_journal_lines_account ON journal_lines(account_code);

-- 6. Tabela de Quarentena & Human-in-the-Loop (HITL)
CREATE TABLE IF NOT EXISTS comex_quarantine_hitl (
    quarantine_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_type VARCHAR(50) NOT NULL,
    raw_payload_hash CHAR(64) NOT NULL,
    mongo_raw_id VARCHAR(64),
    validation_errors JSONB NOT NULL,
    self_correction_attempts INT NOT NULL DEFAULT 0,
    review_status VARCHAR(30) NOT NULL DEFAULT 'PENDING_REVIEW' CHECK (review_status IN ('PENDING_REVIEW', 'APPROVED_OVERRIDE', 'REJECTED')),
    reviewed_by VARCHAR(100),
    reviewed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_quarantine_status ON comex_quarantine_hitl(review_status);


-- 7. Trigger de Imutabilidade Fiduciária (Append-Only Ledger)
CREATE OR REPLACE FUNCTION fn_enforce_append_only_ledger()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_TABLE_NAME = 'journal_lines' THEN
        RAISE EXCEPTION 'Fiduciary Ledger Violation: journal_lines is strictly append-only. Use create_reversal_entry() to post a compensating entry.';
    ELSIF TG_TABLE_NAME = 'journal_entries' THEN
        IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'Fiduciary Ledger Violation: Deleting journal entries is strictly prohibited.';
        ELSIF TG_OP = 'UPDATE' THEN
            IF OLD.status = 'POSTED' AND NOT (NEW.status = 'REVERSED' AND NEW.reversed_from_entry_id IS NOT NULL) THEN
                RAISE EXCEPTION 'Fiduciary Ledger Violation: POSTED journal entries are immutable and cannot be updated.';
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_enforce_append_only_lines ON journal_lines;
CREATE TRIGGER trg_enforce_append_only_lines
BEFORE UPDATE OR DELETE ON journal_lines
FOR EACH ROW
EXECUTE FUNCTION fn_enforce_append_only_ledger();

DROP TRIGGER IF EXISTS trg_enforce_append_only_entries ON journal_entries;
CREATE TRIGGER trg_enforce_append_only_entries
BEFORE UPDATE OR DELETE ON journal_entries
FOR EACH ROW
EXECUTE FUNCTION fn_enforce_append_only_ledger();

-- 8. Tabela de Nacionalizações Aduaneiras e Desembaraço (Customs Nationalizations)
CREATE TABLE IF NOT EXISTS customs_nationalizations (
    nationalization_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    declaration_number VARCHAR(50) NOT NULL UNIQUE,
    registration_date DATE NOT NULL,
    clearance_date DATE NOT NULL,
    invoice_id UUID REFERENCES commercial_invoices(invoice_id),
    importer_tax_id VARCHAR(50) NOT NULL,
    tax_regime VARCHAR(20) NOT NULL CHECK (tax_regime IN ('LUCRO_REAL', 'LUCRO_PRESUMIDO', 'SIMPLES_NACIONAL', 'CUSTO_INTEGRAL')),
    currency_code CHAR(3) NOT NULL CHECK (currency_code IN ('USD', 'EUR', 'CHF', 'BRL')),
    fob_amount NUMERIC(19, 4) NOT NULL,
    freight_amount NUMERIC(19, 4) NOT NULL,
    insurance_amount NUMERIC(19, 4) NOT NULL,
    cif_amount NUMERIC(19, 4) NOT NULL,
    ptax_exchange_rate NUMERIC(19, 6) NOT NULL,
    customs_value_brl NUMERIC(19, 4) NOT NULL,
    ii_rate NUMERIC(7, 4) NOT NULL,
    ii_amount_brl NUMERIC(19, 4) NOT NULL,
    ipi_rate NUMERIC(7, 4) NOT NULL,
    ipi_amount_brl NUMERIC(19, 4) NOT NULL,
    pis_rate NUMERIC(7, 4) NOT NULL,
    pis_amount_brl NUMERIC(19, 4) NOT NULL,
    cofins_rate NUMERIC(7, 4) NOT NULL,
    cofins_amount_brl NUMERIC(19, 4) NOT NULL,
    siscomex_fee_brl NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    afrmm_brl NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    icms_rate NUMERIC(7, 4) NOT NULL,
    icms_base_brl NUMERIC(19, 4) NOT NULL,
    icms_amount_brl NUMERIC(19, 4) NOT NULL,
    total_taxes_brl NUMERIC(19, 4) NOT NULL,
    landed_cost_brl NUMERIC(19, 4) NOT NULL,
    net_inventory_cost_brl NUMERIC(19, 4) NOT NULL,
    recoverable_taxes_brl NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    journal_entry_id UUID REFERENCES journal_entries(entry_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_total_taxes_closure CHECK (
        ABS(total_taxes_brl - (ii_amount_brl + ipi_amount_brl + pis_amount_brl + cofins_amount_brl + siscomex_fee_brl + afrmm_brl + icms_amount_brl)) <= 0.0001
    ),
    CONSTRAINT chk_landed_cost_closure CHECK (
        ABS(landed_cost_brl - (customs_value_brl + total_taxes_brl)) <= 0.0001
    ),
    CONSTRAINT chk_net_inventory_cost CHECK (
        ABS(landed_cost_brl - (net_inventory_cost_brl + recoverable_taxes_brl)) <= 0.0001
    )
);

CREATE INDEX IF NOT EXISTS idx_customs_nat_reg_date ON customs_nationalizations(registration_date);
CREATE INDEX IF NOT EXISTS idx_customs_nat_invoice_id ON customs_nationalizations(invoice_id);
CREATE INDEX IF NOT EXISTS idx_customs_nat_journal_id ON customs_nationalizations(journal_entry_id);

