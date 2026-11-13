-- ==============================================================================
-- 02: Teste de Sabotagem Controlada das Triggers de Imutabilidade & Soma Zero
-- Ferramenta: DBeaver / psql (PostgreSQL 16 - comex_fiduciary_db)
-- Segurança: Executado integralmente dentro de BEGIN ... ROLLBACK; nenhuma linha
--            sintética ou inválida persiste após a execução.
-- ==============================================================================

BEGIN;

DO $$
DECLARE
    v_entry_id UUID := uuid_generate_v4();
    v_rev_entry_id UUID := uuid_generate_v4();
    v_bad_entry_id UUID := uuid_generate_v4();
    v_quarantine_id UUID := uuid_generate_v4();
    v_synthetic_hash CHAR(64) := repeat('a', 64);
BEGIN
    -- Garantir contas canônicas mínimas para os testes em transação isolada (revertida ao final)
    INSERT INTO accounts (account_code, account_name, account_type, normal_balance) VALUES
    ('1.1.3.01', 'Estoques de Mercadorias em Trânsito (Importação)', 'ASSET', 'DEBIT'),
    ('2.1.1.01', 'Fornecedores Internacionais a Pagar', 'LIABILITY', 'CREDIT')
    ON CONFLICT (account_code) DO NOTHING;

    -- Fixture válida: lançamento POSTED balanceado (2 linhas, soma zero) + estorno compensatório
    INSERT INTO journal_entries (entry_id, transaction_date, competence_date, reference_document_id, description, status)
    VALUES
        (v_entry_id, DATE '2026-03-01', DATE '2026-03-01', 'SABOTAGE-FIXTURE-01', 'Fixture teste append-only ledger', 'POSTED'),
        (v_rev_entry_id, DATE '2026-03-02', DATE '2026-03-02', 'SABOTAGE-FIXTURE-01-REV', 'Fixture estorno compensatório', 'POSTED');

    INSERT INTO journal_lines (entry_id, account_code, debit_amount, credit_amount, currency_code, original_currency_amount, exchange_rate_applied, line_order)
    VALUES
        (v_entry_id, '1.1.3.01', 100.0000, 0.0000, 'USD', 20.0000, 5.000000, 1),
        (v_entry_id, '2.1.1.01', 0.0000, 100.0000, 'USD', 20.0000, 5.000000, 2),
        (v_rev_entry_id, '1.1.3.01', 0.0000, 100.0000, 'USD', 20.0000, 5.000000, 1),
        (v_rev_entry_id, '2.1.1.01', 100.0000, 0.0000, 'USD', 20.0000, 5.000000, 2);

    -- 1. Sabotagem A: Bloqueio de UPDATE em journal_lines
    BEGIN
        UPDATE journal_lines SET debit_amount = 999999.0000 WHERE entry_id = v_entry_id AND line_order = 1;
        RAISE EXCEPTION 'SABOTAGE_FAIL: UPDATE em journal_lines foi permitido!';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%Fiduciary Ledger Violation: journal_lines is strictly append-only%' THEN
            RAISE;
        END IF;
    END;

    -- 2. Sabotagem B: Bloqueio de DELETE em journal_lines
    BEGIN
        DELETE FROM journal_lines WHERE entry_id = v_entry_id AND line_order = 1;
        RAISE EXCEPTION 'SABOTAGE_FAIL: DELETE em journal_lines foi permitido!';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%Fiduciary Ledger Violation: journal_lines is strictly append-only%' THEN
            RAISE;
        END IF;
    END;

    -- 3. Sabotagem C: Bloqueio de DELETE em journal_entries
    BEGIN
        DELETE FROM journal_entries WHERE entry_id = v_entry_id;
        RAISE EXCEPTION 'SABOTAGE_FAIL: DELETE em journal_entries foi permitido!';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%Fiduciary Ledger Violation: Deleting journal entries is strictly prohibited%' THEN
            RAISE;
        END IF;
    END;

    -- 4. Sabotagem D: Bloqueio de alteração in-place de lançamento POSTED (descrição)
    BEGIN
        UPDATE journal_entries SET description = 'Alteração fraudulenta' WHERE entry_id = v_entry_id;
        RAISE EXCEPTION 'SABOTAGE_FAIL: UPDATE de descrição em POSTED foi permitido!';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%Fiduciary Ledger Violation: POSTED journal entries are immutable%' THEN
            RAISE;
        END IF;
    END;

    -- 5. Sabotagem E: Bloqueio de transição POSTED -> REVERSED com auto-referência ou alteração de metadados
    BEGIN
        UPDATE journal_entries
        SET status = 'REVERSED', reversed_from_entry_id = v_entry_id
        WHERE entry_id = v_entry_id;
        RAISE EXCEPTION 'SABOTAGE_FAIL: auto-reversão POSTED -> REVERSED foi permitida!';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%Fiduciary Ledger Violation: Transitioning POSTED -> REVERSED requires a valid non-self reversed_from_entry_id%' THEN
            RAISE;
        END IF;
    END;

    BEGIN
        UPDATE journal_entries
        SET status = 'REVERSED', reversed_from_entry_id = v_rev_entry_id, description = 'Adulteração na reversão'
        WHERE entry_id = v_entry_id;
        RAISE EXCEPTION 'SABOTAGE_FAIL: adulteração de description na transição POSTED -> REVERSED foi permitida!';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%Fiduciary Ledger Violation: Only status and reversed_from_entry_id may be updated%' THEN
            RAISE;
        END IF;
    END;

    -- Transição legítima POSTED -> REVERSED (apenas status e reversed_from_entry_id válidos)
    UPDATE journal_entries
    SET status = 'REVERSED', reversed_from_entry_id = v_rev_entry_id
    WHERE entry_id = v_entry_id;

    -- 6. Sabotagem F: Bloqueio total de UPDATE e DELETE em linha já REVERSED
    BEGIN
        UPDATE journal_entries
        SET status = 'POSTED', reversed_from_entry_id = NULL
        WHERE entry_id = v_entry_id;
        RAISE EXCEPTION 'SABOTAGE_FAIL: UPDATE em registro REVERSED foi permitido!';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%Fiduciary Ledger Violation: REVERSED journal entries are strictly immutable%' THEN
            RAISE;
        END IF;
    END;

    BEGIN
        DELETE FROM journal_entries WHERE entry_id = v_entry_id;
        RAISE EXCEPTION 'SABOTAGE_FAIL: DELETE em registro REVERSED foi permitido!';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%Fiduciary Ledger Violation: Deleting journal entries is strictly prohibited%' THEN
            RAISE;
        END IF;
    END;

    -- 7. Sabotagem G: Bloqueio de TRUNCATE em journal_lines e journal_entries
    BEGIN
        TRUNCATE TABLE journal_lines;
        RAISE EXCEPTION 'SABOTAGE_FAIL: TRUNCATE em journal_lines foi permitido!';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%Fiduciary Ledger Violation: TRUNCATE on journal_lines is strictly prohibited%' THEN
            RAISE;
        END IF;
    END;

    BEGIN
        TRUNCATE TABLE journal_entries CASCADE;
        RAISE EXCEPTION 'SABOTAGE_FAIL: TRUNCATE em journal_entries CASCADE foi permitido!';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%Fiduciary Ledger Violation: TRUNCATE on % is strictly prohibited%' THEN
            RAISE;
        END IF;
    END;

    -- 8. Sabotagem H: Constraint Trigger DEFERRABLE bloqueia lançamento sem mínimo de 2 linhas / desequilibrado
    BEGIN
        INSERT INTO journal_entries (entry_id, transaction_date, competence_date, reference_document_id, description, status)
        VALUES (v_bad_entry_id, DATE '2026-03-03', DATE '2026-03-03', 'SABOTAGE-UNBALANCED', 'Lançamento incompleto', 'POSTED');

        INSERT INTO journal_lines (entry_id, account_code, debit_amount, credit_amount, currency_code, original_currency_amount, exchange_rate_applied, line_order)
        VALUES (v_bad_entry_id, '1.1.3.01', 50.0000, 0.0000, 'USD', 10.0000, 5.000000, 1);

        SET CONSTRAINTS trg_check_journal_entry_balance, trg_check_journal_lines_balance IMMEDIATE;
        RAISE EXCEPTION 'SABOTAGE_FAIL: Lançamento desequilibrado / <2 linhas não foi bloqueado!';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%Fiduciary Double-Entry Violation%' THEN
            RAISE;
        END IF;
    END;

    -- 9. Sabotagem I: Bloqueio de alteração ou exclusão em comex_quarantine_hitl já resolvido
    INSERT INTO comex_quarantine_hitl (quarantine_id, source_type, raw_payload_hash, validation_errors, review_status)
    VALUES (v_quarantine_id, 'INVOICE', v_synthetic_hash, '[{"type":"value_error","msg":"sabotage test"}]'::jsonb, 'PENDING_REVIEW');

    UPDATE comex_quarantine_hitl
    SET review_status = 'APPROVED_OVERRIDE',
        reviewed_by = 'auditor_sabotage_test',
        reviewed_at = CURRENT_TIMESTAMP
    WHERE quarantine_id = v_quarantine_id;

    BEGIN
        UPDATE comex_quarantine_hitl
        SET review_status = 'REJECTED_HITL'
        WHERE quarantine_id = v_quarantine_id;
        RAISE EXCEPTION 'SABOTAGE_FAIL: Modificação de quarentena HITL resolvida foi permitida!';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%HITL Quarantine Violation: Resolved quarantine record%' THEN
            RAISE;
        END IF;
    END;

    BEGIN
        DELETE FROM comex_quarantine_hitl WHERE quarantine_id = v_quarantine_id;
        RAISE EXCEPTION 'SABOTAGE_FAIL: Exclusão de quarentena HITL resolvida foi permitida!';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%HITL Quarantine Violation: Resolved quarantine record%' THEN
            RAISE;
        END IF;
    END;

    RAISE NOTICE 'ALL_SABOTAGE_CHECKS_PASSED: Triggers de imutabilidade, REVERSED, TRUNCATE, soma zero e HITL bloqueiam todas as violações.';
END $$;

ROLLBACK;

-- 4. Prova de Integridade Pós-Teste: O Razão Contábil permanece intacto e livre de fixtures de sabotagem
SELECT 
    je.entry_id,
    je.description,
    je.status,
    jl.account_code,
    jl.debit_amount,
    jl.credit_amount
FROM journal_entries je
JOIN journal_lines jl ON je.entry_id = jl.entry_id
ORDER BY je.created_at, jl.line_order;
