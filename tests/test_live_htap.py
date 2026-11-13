import hashlib
from datetime import datetime, timezone
from decimal import Decimal
import os
import uuid

from fastapi.testclient import TestClient
import pyarrow as pa
import pytest
from sqlalchemy import text

from src.api.main import app
from src.connectors import lakehouse
from src.connectors.lakehouse import duckdb_connection, get_parquet_base_dir, write_records_to_parquet_hive
from src.connectors.mongo import get_mongo_client, get_raw_collection
from src.connectors.postgres import engine
from src.pipeline.comex_processor import ComexHTAPProcessor
import src.seed as seed_mod


@pytest.mark.integration
def test_live_mongo_connection_and_idempotency():
    """Valida conectividade real com MongoDB 7 autenticado (porta 27018) e upsert idempotente."""
    col = get_raw_collection("test_raw_collection")
    col.create_index([("idempotency_key", 1)], unique=True)

    test_key = f"TEST-IDEM-{uuid.uuid4().hex[:8]}"
    doc = {
        "idempotency_key": test_key,
        "source": "INTEGRATION_TEST",
        "created_at": datetime.now(timezone.utc),
        "payload": {"amount": "100.0000", "status": "OK"},
    }

    try:
        # Primeiro insert
        res1 = col.update_one(
            {"idempotency_key": test_key},
            {"$setOnInsert": doc},
            upsert=True,
        )
        assert res1.upserted_id is not None or res1.matched_count == 0

        # Segundo insert com mesma chave (idempotência preservada)
        res2 = col.update_one(
            {"idempotency_key": test_key},
            {"$setOnInsert": doc},
            upsert=True,
        )
        assert res2.upserted_id is None
        assert res2.matched_count == 1
    finally:
        col.delete_one({"idempotency_key": test_key})


@pytest.mark.integration
def test_live_postgres_connection_and_comex_ddl():
    """Valida conectividade real com PostgreSQL 16 (porta 5434) e presença completa da DDL Comex V2."""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 AS ping")).scalar()
        assert result == 1

        tables = set(
            conn.execute(
                text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            )
            .scalars()
            .all()
        )
        expected_tables = {
            "ingestion_idempotency_registry",
            "commercial_invoices",
            "commercial_invoice_lines",
            "accounts",
            "journal_entries",
            "journal_lines",
            "comex_quarantine_hitl",
            "dim_date",
            "dim_merchandise_ncm",
            "dim_partner_country",
            "dim_currency_ptax",
            "fact_comex_operations",
            "customs_nationalizations",
        }
        missing = expected_tables - tables
        assert not missing, f"Missing expected PostgreSQL tables: {sorted(missing)}"

        cents_cols = set(
            conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = 'commercial_invoices' "
                    "AND is_generated = 'ALWAYS'"
                )
            )
            .scalars()
            .all()
        )
        assert {
            "subtotal_amount_cents",
            "tax_amount_cents",
            "freight_amount_cents",
            "insurance_amount_cents",
            "total_amount_cents",
        }.issubset(cents_cols)


@pytest.mark.integration
def test_live_postgres_append_only_trigger():
    """
    Valida triggers de imutabilidade append-only, bloqueio de TRUNCATE, transição POSTED->REVERSED,
    soma zero diferida e imutabilidade HITL dentro de transação revertida (zero resíduos em produção).
    """
    with engine.connect() as conn:
        triggers = set(
            conn.execute(
                text(
                    "SELECT tgname FROM pg_trigger WHERE tgname IN ("
                    "'trg_enforce_append_only_lines',"
                    "'trg_enforce_append_only_entries',"
                    "'trg_block_truncate_journal_lines',"
                    "'trg_block_truncate_journal_entries',"
                    "'trg_check_journal_entry_balance',"
                    "'trg_check_journal_lines_balance',"
                    "'trg_enforce_quarantine_hitl_immutability'"
                    ")"
                )
            )
            .scalars()
            .all()
        )
        assert len(triggers) == 7, f"Missing fiduciary PostgreSQL triggers: found {sorted(triggers)}"

        trans = conn.begin()
        try:
            # 1. Bloqueio de TRUNCATE em journal_lines e journal_entries (antes de escritas pendentes)
            with pytest.raises(Exception) as exc_trunc_lines:
                with conn.begin_nested():
                    conn.execute(text("TRUNCATE TABLE journal_lines"))
            assert "Fiduciary Ledger Violation: TRUNCATE on" in str(exc_trunc_lines.value)

            with pytest.raises(Exception) as exc_trunc_entries:
                with conn.begin_nested():
                    conn.execute(text("TRUNCATE TABLE journal_entries CASCADE"))
            assert "Fiduciary Ledger Violation: TRUNCATE on" in str(exc_trunc_entries.value)

            # 2. Inserir contas mínimas + lançamento POSTED balanceado (2 linhas) + compensação válida
            conn.execute(
                text(
                    """
                    INSERT INTO accounts (account_code, account_name, account_type, normal_balance)
                    VALUES
                        ('1.1.1.01', 'Caixa e Equivalentes', 'ASSET', 'DEBIT'),
                        ('1.1.2.01', 'Clientes / Importadores', 'ASSET', 'DEBIT')
                    ON CONFLICT (account_code) DO NOTHING
                    """
                )
            )

            entry_id = uuid.uuid4()
            rev_entry_id = uuid.uuid4()
            bad_entry_id = uuid.uuid4()

            conn.execute(
                text(
                    """
                    INSERT INTO journal_entries (entry_id, transaction_date, competence_date, description, status)
                    VALUES
                        (:eid, '2026-11-10', '2026-11-10', 'TEST_TRIGGER_POSTED', 'POSTED'),
                        (:reid, '2026-11-11', '2026-11-10', 'TEST_TRIGGER_COMPENSATING', 'POSTED')
                    """
                ),
                {"eid": entry_id, "reid": rev_entry_id},
            )
            conn.execute(
                text(
                    """
                    INSERT INTO journal_lines (
                        line_id, entry_id, account_code, debit_amount, credit_amount,
                        currency_code, original_currency_amount, exchange_rate_applied, line_order
                    )
                    VALUES
                        (:l1, :eid, '1.1.1.01', 100.0000, 0.0000, 'BRL', 100.0000, 1.000000, 1),
                        (:l2, :eid, '1.1.2.01', 0.0000, 100.0000, 'BRL', 100.0000, 1.000000, 2),
                        (:l3, :reid, '1.1.1.01', 0.0000, 100.0000, 'BRL', 100.0000, 1.000000, 1),
                        (:l4, :reid, '1.1.2.01', 100.0000, 0.0000, 'BRL', 100.0000, 1.000000, 2)
                    """
                ),
                {
                    "l1": uuid.uuid4(),
                    "l2": uuid.uuid4(),
                    "l3": uuid.uuid4(),
                    "l4": uuid.uuid4(),
                    "eid": entry_id,
                    "reid": rev_entry_id,
                },
            )

            # 3. Bloqueio de UPDATE / DELETE em journal_lines
            with pytest.raises(Exception) as exc_upd_line:
                with conn.begin_nested():
                    conn.execute(
                        text("UPDATE journal_lines SET debit_amount = 999.0000 WHERE entry_id = :eid"),
                        {"eid": entry_id},
                    )
            assert "Fiduciary Ledger Violation: journal_lines is strictly append-only." in str(exc_upd_line.value)

            with pytest.raises(Exception) as exc_del_line:
                with conn.begin_nested():
                    conn.execute(
                        text("DELETE FROM journal_lines WHERE entry_id = :eid"),
                        {"eid": entry_id},
                    )
            assert "Fiduciary Ledger Violation: journal_lines is strictly append-only." in str(exc_del_line.value)

            # 4. Bloqueio de DELETE e mutação em POSTED journal_entries
            with pytest.raises(Exception) as exc_del_entry:
                with conn.begin_nested():
                    conn.execute(text("DELETE FROM journal_entries WHERE entry_id = :eid"), {"eid": entry_id})
            assert "Fiduciary Ledger Violation: Deleting journal entries is strictly prohibited." in str(
                exc_del_entry.value
            )

            with pytest.raises(Exception) as exc_upd_posted:
                with conn.begin_nested():
                    conn.execute(
                        text("UPDATE journal_entries SET description = 'FRAUD' WHERE entry_id = :eid"),
                        {"eid": entry_id},
                    )
            assert "Fiduciary Ledger Violation: POSTED journal entries are immutable and cannot be updated." in str(
                exc_upd_posted.value
            )

            # 5. Bloqueio de transição POSTED -> REVERSED inválida (auto-referência e mutação de campos)
            with pytest.raises(Exception) as exc_self_rev:
                with conn.begin_nested():
                    conn.execute(
                        text(
                            "UPDATE journal_entries SET status = 'REVERSED', reversed_from_entry_id = :eid "
                            "WHERE entry_id = :eid"
                        ),
                        {"eid": entry_id},
                    )
            assert "requires a valid non-self reversed_from_entry_id" in str(exc_self_rev.value)

            with pytest.raises(Exception) as exc_meta_tamper:
                with conn.begin_nested():
                    conn.execute(
                        text(
                            "UPDATE journal_entries SET status = 'REVERSED', reversed_from_entry_id = :reid, "
                            "description = 'TAMPERED' WHERE entry_id = :eid"
                        ),
                        {"eid": entry_id, "reid": rev_entry_id},
                    )
            assert "Only status and reversed_from_entry_id may be updated during POSTED -> REVERSED transition" in str(
                exc_meta_tamper.value
            )

            # 6. Transição válida POSTED -> REVERSED, seguida de imutabilidade total da linha REVERSED
            conn.execute(
                text(
                    "UPDATE journal_entries SET status = 'REVERSED', reversed_from_entry_id = :reid "
                    "WHERE entry_id = :eid"
                ),
                {"eid": entry_id, "reid": rev_entry_id},
            )

            with pytest.raises(Exception) as exc_rev_tamper:
                with conn.begin_nested():
                    conn.execute(
                        text("UPDATE journal_entries SET status = 'POSTED' WHERE entry_id = :eid"),
                        {"eid": entry_id},
                    )
            assert "REVERSED journal entries are strictly immutable and cannot be updated." in str(
                exc_rev_tamper.value
            )

            # 7. Constraint trigger diferida de soma zero (entrada desequilibrada / < 2 linhas)
            with pytest.raises(Exception) as exc_zero_sum:
                with conn.begin_nested():
                    conn.execute(
                        text(
                            """
                            INSERT INTO journal_entries (entry_id, transaction_date, competence_date, description, status)
                            VALUES (:beid, '2026-11-10', '2026-11-10', 'UNBALANCED_PROBE', 'POSTED')
                            """
                        ),
                        {"beid": bad_entry_id},
                    )
                    conn.execute(
                        text(
                            """
                            INSERT INTO journal_lines (
                                line_id, entry_id, account_code, debit_amount, credit_amount,
                                currency_code, original_currency_amount, exchange_rate_applied, line_order
                            )
                            VALUES
                                (:bl1, :beid, '1.1.1.01', 100.0000, 0.0000, 'BRL', 100.0000, 1.000000, 1),
                                (:bl2, :beid, '1.1.2.01', 0.0000, 50.0000, 'BRL', 50.0000, 1.000000, 2)
                            """
                        ),
                        {"bl1": uuid.uuid4(), "bl2": uuid.uuid4(), "beid": bad_entry_id},
                    )
                    conn.execute(
                        text("SET CONSTRAINTS trg_check_journal_entry_balance, trg_check_journal_lines_balance IMMEDIATE")
                    )
            assert "Fiduciary Double-Entry Violation on entry" in str(exc_zero_sum.value)

            # 8. Imutabilidade HITL na tabela comex_quarantine_hitl após resolução
            qid = uuid.uuid4()
            conn.execute(
                text(
                    """
                    INSERT INTO comex_quarantine_hitl (
                        quarantine_id, source_type, raw_payload_hash, validation_errors, review_status
                    )
                    VALUES (
                        :qid, 'INVOICE', :phash, CAST('[{"msg": "test"}]' AS jsonb), 'PENDING_REVIEW'
                    )
                    """
                ),
                {"qid": qid, "phash": hashlib.sha256(f"hitl-{qid}".encode()).hexdigest()},
            )
            conn.execute(
                text(
                    """
                    UPDATE comex_quarantine_hitl
                    SET review_status = 'REJECTED_HITL',
                        reviewed_by = 'auditor@comex.br',
                        reviewed_at = CURRENT_TIMESTAMP
                    WHERE quarantine_id = :qid
                    """
                ),
                {"qid": qid},
            )
            with pytest.raises(Exception) as exc_hitl_lock:
                with conn.begin_nested():
                    conn.execute(
                        text("UPDATE comex_quarantine_hitl SET review_status = 'APPROVED_OVERRIDE' WHERE quarantine_id = :qid"),
                        {"qid": qid},
                    )
            assert "HITL Quarantine Violation: Resolved quarantine record" in str(exc_hitl_lock.value)
        finally:
            trans.rollback()


@pytest.mark.integration
def test_duckdb_parquet_lakehouse_roundtrip(tmp_path, monkeypatch):
    """Valida escrita em Parquet particionado Hive com DECIMAL(19,4) e consulta vetorizada com DuckDB."""
    monkeypatch.setattr(lakehouse, "PARQUET_BASE_DIR", str(tmp_path / "lakehouse"))
    monkeypatch.setattr(lakehouse, "DUCKDB_PATH", str(tmp_path / "live_roundtrip.duckdb"))

    test_table = "test_metrics"
    metrics_schema = pa.schema(
        [
            ("year", pa.int32()),
            ("month", pa.int32()),
            ("entity", pa.string()),
            ("metric_value", pa.decimal128(19, 4)),
        ]
    )
    records = [
        {"year": 2026, "month": 11, "entity": "ORG_A", "metric_value": Decimal("150.7500")},
        {"year": 2026, "month": 11, "entity": "ORG_B", "metric_value": Decimal("240.2500")},
    ]

    write_records_to_parquet_hive(
        records,
        test_table,
        partition_cols=["year", "month"],
        schema=metrics_schema,
    )
    parquet_glob = (tmp_path / "lakehouse" / test_table / "**" / "*.parquet").as_posix()

    with duckdb_connection() as con:
        rows = con.execute(
            "SELECT entity, SUM(metric_value) AS total "
            "FROM read_parquet(?, hive_partitioning=1) "
            "GROUP BY entity ORDER BY entity",
            [parquet_glob],
        ).fetchall()

    assert rows == [
        ("ORG_A", Decimal("150.7500")),
        ("ORG_B", Decimal("240.2500")),
    ]


@pytest.mark.integration
def test_live_comex_htap_processor_e2e():
    """Valida processamento E2E do ComexHTAPProcessor contra MongoDB e PostgreSQL."""
    with engine.begin() as conn:
        assert conn.execute(text("SELECT 1")).scalar() == 1
        conn.execute(
            text(
                """
                INSERT INTO dim_date (date_id, full_date, year, month, day, quarter, is_business_day)
                VALUES (20261110, '2026-11-10', 2026, 11, 10, 4, true)
                ON CONFLICT (date_id) DO NOTHING
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_currency_ptax (
                    date_id, currency_code, ptax_buy_rate, ptax_sell_rate,
                    is_business_day, is_interpolated_locf
                )
                VALUES (20261110, 'USD', 5.840000, 5.840600, true, false)
                ON CONFLICT (date_id, currency_code) DO NOTHING
                """
            )
        )
    col = get_raw_collection("test_processor_raw")
    col.database.command("ping")

    processor = ComexHTAPProcessor(source_system="TEST_SUITE_V2", mongo_collection="test_processor_raw")
    doc_num = f"INV-TEST-{uuid.uuid4().hex[:6]}"

    valid_payload = {
        "issuer_tax_id": "33.000.167/0001-01",
        "document_number": doc_num,
        "issue_date": "2026-11-10",
        "currency_code": "USD",
        "subtotal_amount": "5000.0000",
        "tax_amount": "0.0000",
        "freight_amount": "0.0000",
        "insurance_amount": "0.0000",
        "total_amount": "5000.0000",
        "incoterm": "FOB",
        "extraction_confidence": "0.9900",
        "lines": [
            {
                "line_number": 1,
                "ncm_code": "84713012",
                "description": "Notebooks de teste",
                "quantity": "5.0000",
                "unit_price": "1000.0000",
                "total_price": "5000.0000",
            }
        ],
    }

    res1 = processor.process_invoice(valid_payload)
    assert res1["status"] == "COMMITTED_ACID"
    assert res1["journal_entry_id"] is not None

    # Segunda chamada idêntica -> DUPLICATE_IGNORED
    res2 = processor.process_invoice(valid_payload)
    assert res2["status"] == "DUPLICATE_IGNORED"


@pytest.mark.integration
def test_live_api_health_auth_and_swift_idempotency(monkeypatch):
    """Valida /health, /metrics, X-API-Key 401/403 e persistência idempotente de SWIFT no PG + MongoDB."""
    api_key = os.getenv("API_KEY") or "live-api-key-test"
    auditor_key = os.getenv("AUDITOR_API_KEY") or "live-auditor-key-test"
    pii_salt = os.getenv("PII_HASH_SALT") or "live-pii-salt-test"
    monkeypatch.setenv("API_KEY", api_key)
    monkeypatch.setenv("AUDITOR_API_KEY", auditor_key)
    monkeypatch.setenv("PII_HASH_SALT", pii_salt)
    monkeypatch.setenv("RATE_LIMIT_PER_MINUTE", "1000")
    app.state.limiter.reset()

    live_client = TestClient(app)
    health_resp = live_client.get("/health")
    assert health_resp.status_code == 200
    assert health_resp.json()["status"] == "healthy"
    assert live_client.get("/metrics").status_code == 200

    swift_payload = {
        "reference_document_id": f"LIVE-SWIFT-{uuid.uuid4().hex[:8]}",
        "transaction_date": "2026-11-09",
        "competence_date": "2026-11-01",
        "gross_amount": "10000.0000",
        "fee_amount": "25.0000",
        "currency_code": "USD",
        "exchange_rate": "5.600000",
    }

    assert live_client.post("/api/v2/ledger/reconcile-swift", json=swift_payload).status_code == 401
    assert (
        live_client.post(
            f"/api/v2/quarantine/{uuid.uuid4()}/resolve",
            json={"resolution_action": "REJECTED_HITL", "reviewed_by": "audit@comex.br"},
            headers={"X-API-Key": api_key},
        ).status_code
        == 403
    )

    std_headers = {"X-API-Key": api_key}
    r1 = live_client.post("/api/v2/ledger/reconcile-swift", json=swift_payload, headers=std_headers)
    assert r1.status_code == 200
    d1 = r1.json()
    assert d1["status"] == "RECONCILED"
    assert d1["idempotent_replay"] is False
    assert d1["lines_count"] == 3

    mongo_db = os.getenv("MONGO_DB", "comex_raw_lake")
    mclient = get_mongo_client()
    with engine.connect() as conn:
        je_before = conn.execute(text("SELECT COUNT(*) FROM journal_entries")).scalar_one()
        jl_before = conn.execute(text("SELECT COUNT(*) FROM journal_lines")).scalar_one()
        reg_before = conn.execute(text("SELECT COUNT(*) FROM ingestion_idempotency_registry")).scalar_one()
    mongo_before = mclient[mongo_db]["raw_swift_statements"].count_documents({})

    r2 = live_client.post("/api/v2/ledger/reconcile-swift", json=swift_payload, headers=std_headers)
    assert r2.status_code == 200
    d2 = r2.json()
    assert d2["status"] == "DUPLICATE_IGNORED"
    assert d2["idempotent_replay"] is True
    assert d2["entry_id"] == d1["entry_id"]

    with engine.connect() as conn:
        je_after = conn.execute(text("SELECT COUNT(*) FROM journal_entries")).scalar_one()
        jl_after = conn.execute(text("SELECT COUNT(*) FROM journal_lines")).scalar_one()
        reg_after = conn.execute(text("SELECT COUNT(*) FROM ingestion_idempotency_registry")).scalar_one()
    mongo_after = mclient[mongo_db]["raw_swift_statements"].count_documents({})

    assert je_after == je_before
    assert jl_after == jl_before
    assert reg_after == reg_before
    assert mongo_after == mongo_before


@pytest.mark.integration
def test_live_seed_twice_idempotency_and_rejected_hitl_enum():
    """Executa src.seed.main() duas vezes no ambiente vivo e valida +0 linhas na 2ª execução e REJECTED_HITL."""
    seed_mod.main()

    pg_tables = [
        "ingestion_idempotency_registry",
        "commercial_invoices",
        "commercial_invoice_lines",
        "accounts",
        "journal_entries",
        "journal_lines",
        "comex_quarantine_hitl",
        "dim_date",
        "dim_merchandise_ncm",
        "dim_partner_country",
        "dim_currency_ptax",
        "fact_comex_operations",
        "customs_nationalizations",
    ]
    mongo_db = os.getenv("MONGO_DB", "comex_raw_lake")
    mclient = get_mongo_client()

    def _capture_live_counts() -> dict[str, int]:
        counts: dict[str, int] = {}
        with engine.connect() as conn:
            for tbl in pg_tables:
                counts[f"pg.{tbl}"] = int(conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar_one())
            zero_sum = conn.execute(
                text("SELECT COALESCE(SUM(debit_amount) - SUM(credit_amount), 0) FROM journal_lines")
            ).scalar_one()
            assert zero_sum == Decimal("0.0000")

            check_def = conn.execute(
                text(
                    "SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c "
                    "JOIN pg_class t ON c.conrelid = t.oid "
                    "WHERE t.relname = 'comex_quarantine_hitl' AND c.contype = 'c' "
                    "AND pg_get_constraintdef(c.oid) LIKE '%review_status%'"
                )
            ).scalar_one()
            assert "REJECTED_HITL" in check_def

        for col_name in ("raw_commercial_invoices", "raw_swift_statements"):
            counts[f"mongo.{col_name}"] = int(mclient[mongo_db][col_name].count_documents({}))

        parquet_fact_dir = get_parquet_base_dir() / "fact_comex_operations"
        parquet_glob = (parquet_fact_dir / "**" / "*.parquet").resolve().as_posix()
        with duckdb_connection() as dcon:
            row = dcon.execute(
                "SELECT COUNT(*) FROM read_parquet(?, hive_partitioning=true)",
                [parquet_glob],
            ).fetchone()
            counts["duckdb.fact_comex_operations"] = int(row[0]) if row is not None else 0
        return counts

    after_run1 = _capture_live_counts()
    seed_mod.main()
    after_run2 = _capture_live_counts()

    assert after_run2 == after_run1

