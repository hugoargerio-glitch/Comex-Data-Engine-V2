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
('4.1.4.01', 'Despesas Tributárias e Aduaneiras (IOF / Impostos)', 'EXPENSE', 'DEBIT')
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
