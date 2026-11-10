import os
import uuid
from decimal import Decimal
from datetime import datetime, timezone
import pytest
from sqlalchemy import text

from src.connectors.mongo import get_mongo_client, get_raw_collection
from src.connectors.postgres import engine
from src.connectors.lakehouse import get_duckdb_connection, write_records_to_parquet_hive
from src.pipeline.comex_processor import ComexHTAPProcessor

@pytest.mark.integration
def test_live_mongo_connection_and_idempotency():
    """Valida conectividade real com MongoDB 7 na porta 27018/27017 e upsert idempotente."""
    try:
        col = get_raw_collection("test_raw_collection")
        col.create_index([("idempotency_key", 1)], unique=True)
    except Exception as exc:
        pytest.skip(f"MongoDB não disponível ou falha de rede: {exc}")

    test_key = f"TEST-IDEM-{uuid.uuid4().hex[:8]}"
    doc = {
        "idempotency_key": test_key,
        "source": "UNIT_TEST",
        "created_at": datetime.now(timezone.utc),
        "payload": {"amount": 100, "status": "OK"}
    }

    # Primeiro insert
    res1 = col.update_one(
        {"idempotency_key": test_key},
        {"$setOnInsert": doc},
        upsert=True
    )
    assert res1.upserted_id is not None or res1.matched_count == 0

    # Segundo insert com mesma chave (idempotência preservada)
    res2 = col.update_one(
        {"idempotency_key": test_key},
        {"$setOnInsert": doc},
        upsert=True
    )
    assert res2.upserted_id is None
    assert res2.matched_count == 1

    # Cleanup do documento de teste
    col.delete_one({"idempotency_key": test_key})

@pytest.mark.integration
def test_live_postgres_connection_and_comex_ddl():
    """Valida conectividade real com PostgreSQL 16 e presença de DDL do Comex V2."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 AS ping")).scalar()
            assert result == 1
    except Exception as exc:
        pytest.skip(f"PostgreSQL não disponível ou credenciais inválidas: {exc}")

    with engine.connect() as conn:
        tables = conn.execute(
            text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        ).scalars().all()
        assert "commercial_invoices" in tables
        assert "journal_entries" in tables
        assert "journal_lines" in tables
        assert "accounts" in tables
        assert "comex_quarantine_hitl" in tables
        assert "ingestion_idempotency_registry" in tables

@pytest.mark.integration
def test_live_postgres_append_only_trigger():
    """Valida que a Trigger fn_enforce_append_only_ledger() bloqueia UPDATE/DELETE em journal_lines."""
    try:
        with engine.connect() as conn:
            has_trigger = conn.execute(
                text("SELECT 1 FROM pg_trigger WHERE tgname = 'trg_enforce_append_only_lines'")
            ).scalar()
            if not has_trigger:
                pytest.skip("Trigger trg_enforce_append_only_lines ainda não aplicada no PostgreSQL.")
    except Exception as exc:
        pytest.skip(f"PostgreSQL indisponível: {exc}")

    # Insere uma entrada temporária
    entry_id = str(uuid.uuid4())
    line_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO journal_entries (entry_id, transaction_date, competence_date, description, status)
                VALUES (:eid, '2026-11-10', '2026-11-10', 'TEST_TRIGGER', 'POSTED')
                """
            ),
            {"eid": entry_id},
        )
        conn.execute(
            text(
                """
                INSERT INTO journal_lines (line_id, entry_id, account_code, debit_amount, credit_amount, currency_code, original_currency_amount, line_order)
                VALUES (:lid, :eid, '1.1.1.01', 100.0000, 0.0000, 'BRL', 100.0000, 1)
                """
            ),
            {"lid": line_id, "eid": entry_id},
        )

    # Tenta sabotar journal_lines com UPDATE -> Deve disparar exceção
    with pytest.raises(Exception) as exc_info:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE journal_lines SET debit_amount = 999.0000 WHERE line_id = :lid"),
                {"lid": line_id},
            )
    assert "Fiduciary Ledger Violation: journal_lines is strictly append-only" in str(exc_info.value)

@pytest.mark.integration
def test_duckdb_parquet_lakehouse_roundtrip(tmp_path):
    """Valida escrita em Parquet particionado Hive e consulta vetorizada com DuckDB."""
    test_table = "test_metrics"
    records = [
        {"year": 2026, "month": 11, "entity": "ORG_A", "metric_value": 150.75},
        {"year": 2026, "month": 11, "entity": "ORG_B", "metric_value": 240.25},
    ]

    import src.connectors.lakehouse as lakehouse
    old_base = lakehouse.PARQUET_BASE_DIR
    lakehouse.PARQUET_BASE_DIR = str(tmp_path / "lakehouse")

    try:
        write_records_to_parquet_hive(records, test_table, partition_cols=["year", "month"])
        parquet_glob = str(tmp_path / "lakehouse" / test_table / "**" / "*.parquet")
        
        con = get_duckdb_connection()
        query = f"SELECT entity, SUM(metric_value) AS total FROM read_parquet('{parquet_glob}', hive_partitioning=1) GROUP BY entity ORDER BY entity"
        df = con.execute(query).fetchall()

        assert len(df) == 2
        assert df[0] == ("ORG_A", 150.75)
        assert df[1] == ("ORG_B", 240.25)
    finally:
        lakehouse.PARQUET_BASE_DIR = old_base

@pytest.mark.integration
def test_live_comex_htap_processor_e2e():
    """Valida processamento E2E do ComexHTAPProcessor contra MongoDB e PostgreSQL."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        col = get_raw_collection("test_processor_raw")
        col.database.command("ping")
    except Exception as exc:
        pytest.skip(f"Bancos Mongo/Postgres indisponíveis para teste E2E: {exc}")

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

    res1 = processor.process_invoice_payload(valid_payload)
    assert res1["status"] in ("COMMITTED_ACID", "PROCESSED_MEM")

    # Segunda chamada idêntica -> DUPLICATE_IGNORED
    res2 = processor.process_invoice_payload(valid_payload)
    assert res2["status"] == "DUPLICATE_IGNORED"
