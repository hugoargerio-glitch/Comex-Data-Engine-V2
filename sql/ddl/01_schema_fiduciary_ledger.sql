-- ==============================================================================
-- Schema 01: Core Fiduciário & Razão Contábil por Partidas Dobradas (ACID)
-- Comex Fiduciary Engine V2 — PostgreSQL 16
-- ==============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Registro de Idempotência de Borda (SHA-256 Fingerprinting)
CREATE TABLE IF NOT EXISTS ingestion_idempotency_registry (
    payload_hash CHAR(64) PRIMARY KEY CHECK (payload_hash ~ '^[0-9a-fA-F]{64}$'),
    source_system VARCHAR(50) NOT NULL CHECK (length(btrim(source_system)) > 0),
    target_entity_id VARCHAR(100),
    status VARCHAR(30) NOT NULL DEFAULT 'PROCESSED'
        CHECK (status IN ('PROCESSED', 'QUARANTINED', 'RESOLVED_OVERRIDE', 'REJECTED_HITL')),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2. Tabela de Faturas Comerciais Internacionais (Commercial Invoices)
CREATE TABLE IF NOT EXISTS commercial_invoices (
    invoice_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    issuer_tax_id VARCHAR(50) NOT NULL CHECK (length(btrim(issuer_tax_id)) > 0),
    document_number VARCHAR(100) NOT NULL CHECK (length(btrim(document_number)) > 0),
    issue_date DATE NOT NULL,
    currency_code CHAR(3) NOT NULL CHECK (currency_code ~ '^[A-Z]{3}$'),
    subtotal_amount NUMERIC(19, 4) NOT NULL DEFAULT 0.0000 CHECK (subtotal_amount >= 0.0000),
    tax_amount NUMERIC(19, 4) NOT NULL DEFAULT 0.0000 CHECK (tax_amount >= 0.0000),
    freight_amount NUMERIC(19, 4) NOT NULL DEFAULT 0.0000 CHECK (freight_amount >= 0.0000),
    insurance_amount NUMERIC(19, 4) NOT NULL DEFAULT 0.0000 CHECK (insurance_amount >= 0.0000),
    total_amount NUMERIC(19, 4) NOT NULL DEFAULT 0.0000 CHECK (total_amount >= 0.0000),
    subtotal_amount_cents BIGINT GENERATED ALWAYS AS (ROUND(subtotal_amount * 10000)::BIGINT) STORED NOT NULL,
    tax_amount_cents BIGINT GENERATED ALWAYS AS (ROUND(tax_amount * 10000)::BIGINT) STORED NOT NULL,
    freight_amount_cents BIGINT GENERATED ALWAYS AS (ROUND(freight_amount * 10000)::BIGINT) STORED NOT NULL,
    insurance_amount_cents BIGINT GENERATED ALWAYS AS (ROUND(insurance_amount * 10000)::BIGINT) STORED NOT NULL,
    total_amount_cents BIGINT GENERATED ALWAYS AS (ROUND(total_amount * 10000)::BIGINT) STORED NOT NULL,
    incoterm VARCHAR(3) NOT NULL CHECK (incoterm IN ('EXW', 'FOB', 'CIF', 'DDP')),
    extraction_confidence NUMERIC(5, 4) NOT NULL DEFAULT 1.0000
        CHECK (extraction_confidence BETWEEN 0.0000 AND 1.0000),
    raw_payload_hash CHAR(64) NOT NULL CHECK (raw_payload_hash ~ '^[0-9a-fA-F]{64}$'),
    mongo_raw_id VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_invoice_natural_key UNIQUE (issuer_tax_id, document_number, issue_date, currency_code, total_amount),
    CONSTRAINT chk_invoice_balance CHECK (
        ABS(total_amount - (subtotal_amount + tax_amount + freight_amount + insurance_amount)) <= 0.0001
    ),
    CONSTRAINT chk_invoice_cif_freight_insurance CHECK (
        incoterm <> 'CIF' OR (freight_amount > 0.0000 AND insurance_amount > 0.0000)
    )
);

CREATE INDEX IF NOT EXISTS idx_invoices_issue_date ON commercial_invoices(issue_date);
CREATE INDEX IF NOT EXISTS idx_invoices_tax_id ON commercial_invoices(issuer_tax_id);

-- 2b. Itens de Linha das Faturas Comerciais Internacionais (Commercial Invoice Lines)
CREATE TABLE IF NOT EXISTS commercial_invoice_lines (
    line_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    invoice_id UUID NOT NULL REFERENCES commercial_invoices(invoice_id) ON DELETE CASCADE,
    line_number INT NOT NULL CHECK (line_number >= 1),
    ncm_code VARCHAR(8) CHECK (ncm_code IS NULL OR ncm_code ~ '^\d{8}$'),
    description TEXT NOT NULL CHECK (length(btrim(description)) > 0),
    quantity NUMERIC(19, 4) NOT NULL CHECK (quantity > 0.0000),
    unit_price NUMERIC(19, 4) NOT NULL CHECK (unit_price >= 0.0000),
    total_price NUMERIC(19, 4) NOT NULL CHECK (total_price >= 0.0000),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_invoice_line_number UNIQUE (invoice_id, line_number),
    CONSTRAINT chk_invoice_line_horizontal_math CHECK (
        ABS(total_price - ROUND(quantity * unit_price, 4)) <= 0.0001
    )
);

CREATE INDEX IF NOT EXISTS idx_invoice_lines_invoice_id ON commercial_invoice_lines(invoice_id);
CREATE INDEX IF NOT EXISTS idx_invoice_lines_ncm_code ON commercial_invoice_lines(ncm_code);

-- 3. Plano de Contas Fiduciário (Chart of Accounts)
CREATE TABLE IF NOT EXISTS accounts (
    account_code VARCHAR(50) PRIMARY KEY CHECK (length(btrim(account_code)) > 0),
    account_name VARCHAR(150) NOT NULL CHECK (length(btrim(account_name)) > 0),
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
    description TEXT NOT NULL CHECK (length(btrim(description)) > 0),
    status VARCHAR(20) NOT NULL DEFAULT 'POSTED' CHECK (status IN ('POSTED', 'REVERSED')),
    reversed_from_entry_id UUID REFERENCES journal_entries(entry_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_journal_entry_reversal_consistency CHECK (
        (status = 'POSTED' AND reversed_from_entry_id IS NULL)
        OR (
            status = 'REVERSED'
            AND reversed_from_entry_id IS NOT NULL
            AND reversed_from_entry_id <> entry_id
        )
    )
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
    currency_code CHAR(3) NOT NULL CHECK (currency_code ~ '^[A-Z]{3}$'),
    original_currency_amount NUMERIC(19, 4) NOT NULL CHECK (original_currency_amount > 0.0000),
    exchange_rate_applied NUMERIC(19, 6) NOT NULL DEFAULT 1.000000 CHECK (exchange_rate_applied > 0.000000),
    line_order INT NOT NULL CHECK (line_order >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_journal_lines_entry_line_order UNIQUE (entry_id, line_order),
    CONSTRAINT chk_non_negative_amounts CHECK (debit_amount >= 0.0000 AND credit_amount >= 0.0000),
    CONSTRAINT chk_exclusive_debit_credit CHECK (
        (debit_amount > 0.0000 AND credit_amount = 0.0000)
        OR (credit_amount > 0.0000 AND debit_amount = 0.0000)
    )
);

CREATE INDEX IF NOT EXISTS idx_journal_lines_entry ON journal_lines(entry_id);
CREATE INDEX IF NOT EXISTS idx_journal_lines_account ON journal_lines(account_code);

-- 6. Tabela de Quarentena & Human-in-the-Loop (HITL)
CREATE TABLE IF NOT EXISTS comex_quarantine_hitl (
    quarantine_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_type VARCHAR(50) NOT NULL CHECK (source_type IN ('INVOICE', 'MDIC_BATCH', 'SWIFT_STATEMENT')),
    raw_payload_hash CHAR(64) NOT NULL CHECK (raw_payload_hash ~ '^[0-9a-fA-F]{64}$'),
    mongo_raw_id VARCHAR(64),
    validation_errors JSONB NOT NULL CHECK (jsonb_typeof(validation_errors) = 'array'),
    self_correction_attempts INT NOT NULL DEFAULT 0 CHECK (self_correction_attempts >= 0),
    review_status VARCHAR(30) NOT NULL DEFAULT 'PENDING_REVIEW'
        CHECK (review_status IN ('PENDING_REVIEW', 'APPROVED_OVERRIDE', 'REJECTED_HITL')),
    reviewed_by VARCHAR(100),
    reviewed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_quarantine_review_consistency CHECK (
        (review_status = 'PENDING_REVIEW' AND reviewed_by IS NULL AND reviewed_at IS NULL)
        OR (
            review_status IN ('APPROVED_OVERRIDE', 'REJECTED_HITL')
            AND reviewed_by IS NOT NULL
            AND length(btrim(reviewed_by)) > 0
            AND reviewed_at IS NOT NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_quarantine_status ON comex_quarantine_hitl(review_status);

-- 7. Trigger de Imutabilidade Fiduciária (Append-Only Ledger)
CREATE OR REPLACE FUNCTION fn_enforce_append_only_ledger()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'Fiduciary Ledger Violation: TRUNCATE on % is strictly prohibited.', TG_TABLE_NAME;
    END IF;

    IF TG_TABLE_NAME = 'journal_lines' THEN
        RAISE EXCEPTION 'Fiduciary Ledger Violation: journal_lines is strictly append-only. Use create_reversal_entry() to post a compensating entry.';
    ELSIF TG_TABLE_NAME = 'journal_entries' THEN
        IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'Fiduciary Ledger Violation: Deleting journal entries is strictly prohibited.';
        ELSIF TG_OP = 'UPDATE' THEN
            IF OLD.status = 'REVERSED' THEN
                RAISE EXCEPTION 'Fiduciary Ledger Violation: REVERSED journal entries are strictly immutable and cannot be updated.';
            ELSIF OLD.status = 'POSTED' THEN
                IF NEW.status IS DISTINCT FROM 'REVERSED' THEN
                    RAISE EXCEPTION 'Fiduciary Ledger Violation: POSTED journal entries are immutable and cannot be updated.';
                END IF;
                IF NEW.reversed_from_entry_id IS NULL OR NEW.reversed_from_entry_id = OLD.entry_id THEN
                    RAISE EXCEPTION 'Fiduciary Ledger Violation: Transitioning POSTED -> REVERSED requires a valid non-self reversed_from_entry_id.';
                END IF;
                IF NEW.entry_id IS DISTINCT FROM OLD.entry_id
                   OR NEW.transaction_date IS DISTINCT FROM OLD.transaction_date
                   OR NEW.competence_date IS DISTINCT FROM OLD.competence_date
                   OR NEW.reference_document_id IS DISTINCT FROM OLD.reference_document_id
                   OR NEW.description IS DISTINCT FROM OLD.description
                   OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                    RAISE EXCEPTION 'Fiduciary Ledger Violation: Only status and reversed_from_entry_id may be updated during POSTED -> REVERSED transition.';
                END IF;
            ELSE
                RAISE EXCEPTION 'Fiduciary Ledger Violation: Unsupported journal entry status transition from %.', OLD.status;
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

DROP TRIGGER IF EXISTS trg_block_truncate_journal_lines ON journal_lines;
CREATE TRIGGER trg_block_truncate_journal_lines
BEFORE TRUNCATE ON journal_lines
FOR EACH STATEMENT
EXECUTE FUNCTION fn_enforce_append_only_ledger();

DROP TRIGGER IF EXISTS trg_block_truncate_journal_entries ON journal_entries;
CREATE TRIGGER trg_block_truncate_journal_entries
BEFORE TRUNCATE ON journal_entries
FOR EACH STATEMENT
EXECUTE FUNCTION fn_enforce_append_only_ledger();

-- 7b. Deferred Constraint Trigger para Soma Zero e Mínimo de 2 Linhas por Lançamento no COMMIT
CREATE OR REPLACE FUNCTION fn_enforce_double_entry_zero_sum()
RETURNS TRIGGER AS $$
DECLARE
    v_entry_id UUID;
    v_line_count BIGINT;
    v_total_debit NUMERIC(19, 4);
    v_total_credit NUMERIC(19, 4);
    v_delta NUMERIC(19, 4);
BEGIN
    IF TG_TABLE_NAME = 'journal_entries' THEN
        v_entry_id := NEW.entry_id;
    ELSIF TG_OP = 'DELETE' THEN
        v_entry_id := OLD.entry_id;
    ELSE
        v_entry_id := NEW.entry_id;
    END IF;

    IF TG_TABLE_NAME = 'journal_lines' AND TG_OP = 'UPDATE' AND OLD.entry_id IS DISTINCT FROM NEW.entry_id THEN
        IF EXISTS (SELECT 1 FROM journal_entries WHERE entry_id = OLD.entry_id) THEN
            SELECT COUNT(*), COALESCE(SUM(debit_amount), 0.0000), COALESCE(SUM(credit_amount), 0.0000)
            INTO v_line_count, v_total_debit, v_total_credit
            FROM journal_lines
            WHERE entry_id = OLD.entry_id;

            v_delta := ABS(v_total_debit - v_total_credit);
            IF v_line_count < 2 OR v_delta > 0.0001 THEN
                RAISE EXCEPTION 'Fiduciary Double-Entry Violation on entry %: line_count=% (min 2), total_debit=%, total_credit=%, delta=% (max 0.0001).',
                    OLD.entry_id, v_line_count, v_total_debit, v_total_credit, v_delta;
            END IF;
        END IF;
    END IF;

    IF v_entry_id IS NOT NULL AND EXISTS (SELECT 1 FROM journal_entries WHERE entry_id = v_entry_id) THEN
        SELECT COUNT(*), COALESCE(SUM(debit_amount), 0.0000), COALESCE(SUM(credit_amount), 0.0000)
        INTO v_line_count, v_total_debit, v_total_credit
        FROM journal_lines
        WHERE entry_id = v_entry_id;

        v_delta := ABS(v_total_debit - v_total_credit);
        IF v_line_count < 2 OR v_delta > 0.0001 THEN
            RAISE EXCEPTION 'Fiduciary Double-Entry Violation on entry %: line_count=% (min 2), total_debit=%, total_credit=%, delta=% (max 0.0001).',
                v_entry_id, v_line_count, v_total_debit, v_total_credit, v_delta;
        END IF;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_check_journal_entry_balance ON journal_entries;
CREATE CONSTRAINT TRIGGER trg_check_journal_entry_balance
AFTER INSERT OR UPDATE ON journal_entries
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION fn_enforce_double_entry_zero_sum();

DROP TRIGGER IF EXISTS trg_check_journal_lines_balance ON journal_lines;
CREATE CONSTRAINT TRIGGER trg_check_journal_lines_balance
AFTER INSERT OR UPDATE OR DELETE ON journal_lines
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION fn_enforce_double_entry_zero_sum();

-- 7c. Trigger de Imutabilidade de Registros HITL Já Resolvidos
CREATE OR REPLACE FUNCTION fn_enforce_quarantine_hitl_immutability()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        IF EXISTS (
            SELECT 1 FROM comex_quarantine_hitl
            WHERE review_status IN ('APPROVED_OVERRIDE', 'REJECTED_HITL')
        ) THEN
            RAISE EXCEPTION 'HITL Quarantine Violation: TRUNCATE blocked while finalized audit records exist.';
        END IF;
        RETURN NULL;
    END IF;

    IF OLD.review_status IN ('APPROVED_OVERRIDE', 'REJECTED_HITL') THEN
        RAISE EXCEPTION 'HITL Quarantine Violation: Resolved quarantine record % (%) is strictly immutable and cannot be modified or deleted.',
            OLD.quarantine_id, OLD.review_status;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF NEW.quarantine_id IS DISTINCT FROM OLD.quarantine_id
           OR NEW.source_type IS DISTINCT FROM OLD.source_type
           OR NEW.raw_payload_hash IS DISTINCT FROM OLD.raw_payload_hash
           OR NEW.validation_errors IS DISTINCT FROM OLD.validation_errors
           OR NEW.self_correction_attempts IS DISTINCT FROM OLD.self_correction_attempts
           OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
            RAISE EXCEPTION 'HITL Quarantine Violation: Forensic quarantine metadata on record % is immutable during review resolution.',
                OLD.quarantine_id;
        END IF;
        RETURN NEW;
    END IF;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_enforce_quarantine_hitl_immutability ON comex_quarantine_hitl;
CREATE TRIGGER trg_enforce_quarantine_hitl_immutability
BEFORE UPDATE OR DELETE ON comex_quarantine_hitl
FOR EACH ROW
EXECUTE FUNCTION fn_enforce_quarantine_hitl_immutability();

DROP TRIGGER IF EXISTS trg_block_truncate_quarantine_hitl ON comex_quarantine_hitl;
CREATE TRIGGER trg_block_truncate_quarantine_hitl
BEFORE TRUNCATE ON comex_quarantine_hitl
FOR EACH STATEMENT
EXECUTE FUNCTION fn_enforce_quarantine_hitl_immutability();

-- 8. Tabela de Nacionalizações Aduaneiras e Desembaraço (Customs Nationalizations)
CREATE TABLE IF NOT EXISTS customs_nationalizations (
    nationalization_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    declaration_number VARCHAR(50) NOT NULL UNIQUE CHECK (length(btrim(declaration_number)) > 0),
    registration_date DATE NOT NULL,
    clearance_date DATE NOT NULL,
    invoice_id UUID REFERENCES commercial_invoices(invoice_id),
    importer_tax_id VARCHAR(50) NOT NULL CHECK (length(btrim(importer_tax_id)) > 0),
    tax_regime VARCHAR(20) NOT NULL CHECK (tax_regime IN ('LUCRO_REAL', 'LUCRO_PRESUMIDO', 'SIMPLES_NACIONAL', 'CUSTO_INTEGRAL')),
    currency_code CHAR(3) NOT NULL CHECK (currency_code ~ '^[A-Z]{3}$' AND currency_code IN ('USD', 'EUR', 'CHF', 'BRL')),
    fob_amount NUMERIC(19, 4) NOT NULL CHECK (fob_amount >= 0.0000),
    freight_amount NUMERIC(19, 4) NOT NULL CHECK (freight_amount >= 0.0000),
    insurance_amount NUMERIC(19, 4) NOT NULL CHECK (insurance_amount >= 0.0000),
    cif_amount NUMERIC(19, 4) NOT NULL CHECK (cif_amount >= 0.0000),
    ptax_exchange_rate NUMERIC(19, 6) NOT NULL CHECK (ptax_exchange_rate > 0.000000),
    customs_value_brl NUMERIC(19, 4) NOT NULL CHECK (customs_value_brl >= 0.0000),
    ii_rate NUMERIC(7, 4) NOT NULL CHECK (ii_rate >= 0.0000),
    ii_amount_brl NUMERIC(19, 4) NOT NULL CHECK (ii_amount_brl >= 0.0000),
    ipi_rate NUMERIC(7, 4) NOT NULL CHECK (ipi_rate >= 0.0000),
    ipi_amount_brl NUMERIC(19, 4) NOT NULL CHECK (ipi_amount_brl >= 0.0000),
    pis_rate NUMERIC(7, 4) NOT NULL CHECK (pis_rate >= 0.0000),
    pis_amount_brl NUMERIC(19, 4) NOT NULL CHECK (pis_amount_brl >= 0.0000),
    cofins_rate NUMERIC(7, 4) NOT NULL CHECK (cofins_rate >= 0.0000),
    cofins_amount_brl NUMERIC(19, 4) NOT NULL CHECK (cofins_amount_brl >= 0.0000),
    siscomex_fee_brl NUMERIC(19, 4) NOT NULL DEFAULT 0.0000 CHECK (siscomex_fee_brl >= 0.0000),
    afrmm_brl NUMERIC(19, 4) NOT NULL DEFAULT 0.0000 CHECK (afrmm_brl >= 0.0000),
    icms_rate NUMERIC(7, 4) NOT NULL CHECK (icms_rate >= 0.0000 AND icms_rate < 1.0000),
    icms_base_brl NUMERIC(19, 4) NOT NULL CHECK (icms_base_brl >= 0.0000),
    icms_amount_brl NUMERIC(19, 4) NOT NULL CHECK (icms_amount_brl >= 0.0000),
    total_taxes_brl NUMERIC(19, 4) NOT NULL CHECK (total_taxes_brl >= 0.0000),
    landed_cost_brl NUMERIC(19, 4) NOT NULL CHECK (landed_cost_brl >= 0.0000),
    net_inventory_cost_brl NUMERIC(19, 4) NOT NULL CHECK (net_inventory_cost_brl >= 0.0000),
    recoverable_taxes_brl NUMERIC(19, 4) NOT NULL DEFAULT 0.0000 CHECK (recoverable_taxes_brl >= 0.0000),
    journal_entry_id UUID REFERENCES journal_entries(entry_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_customs_dates CHECK (clearance_date >= registration_date),
    CONSTRAINT chk_customs_cif_closure CHECK (
        ABS(cif_amount - (fob_amount + freight_amount + insurance_amount)) <= 0.0001
    ),
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
