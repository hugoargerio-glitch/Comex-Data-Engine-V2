import os
import uuid
from decimal import Decimal
from datetime import datetime, timezone
import pytest
from sqlalchemy import text

from src.connectors.mongo import get_mongo_client, get_raw_collection
from src.connectors.postgres import engine
from src.connectors.lakehouse import get_duckdb_connection, write_records_to_parquet_hive
from src.pipeline.processor import HTAPProcessor

@pytest.mark.integration
def test_live_mongo_connection_and_idempotency():
    """Valida conectividade real com MongoDB 7 na porta 27017 e upsert idempotente."""
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
def test_live_postgres_connection_and_ddl():
    """Valida conectividade real com PostgreSQL 16 na porta 5433 e presença de DDL."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 AS ping")).scalar()
            assert result == 1
    except Exception as exc:
        pytest.skip(f"PostgreSQL não disponível ou credenciais inválidas: {exc}")

    # Fora do bloco try/except de rede para garantir que falhas de esquema não sejam mascaradas como skip
    with engine.connect() as conn:
        tables = conn.execute(
            text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        ).scalars().all()
        assert "canonical_records" in tables
        assert "idempotency_registry" in tables

@pytest.mark.integration
def test_duckdb_parquet_lakehouse_roundtrip(tmp_path):
    """Valida escrita em Parquet particionado Hive e consulta vetorizada com DuckDB."""
    test_table = "test_metrics"
    records = [
        {"year": 2026, "month": 11, "entity": "ORG_A", "metric_value": 150.75},
        {"year": 2026, "month": 11, "entity": "ORG_B", "metric_value": 240.25},
    ]

    # Temporariamente sobrescreve PARQUET_BASE_DIR para a pasta tmp
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
def test_live_htap_processor_double_barrier_and_quarantine():
    """
    Valida a Dupla Barreira de Idempotência (Mongo SHA-256 + Postgres ACID Upsert)
    e o desvio automático de payload inválido para quarantine_records (HITL).
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        col = get_raw_collection("test_processor_raw")
        col.database.command("ping")
    except Exception as exc:
        pytest.skip(f"Bancos Mongo/Postgres indisponíveis para teste E2E: {exc}")

    processor = HTAPProcessor(source_system="TEST_SUITE", mongo_collection="test_processor_raw")
    entity_code = f"ENT-{uuid.uuid4().hex[:8]}"

    valid_payload = {
        "entity_code": entity_code,
        "reference_date": "2026-11-09",
        "currency_code": "USD",
        "gross_amount": "2000.0000",
        "deduction_amount": "150.0000",
        "net_amount": "1850.0000"
    }

    invalid_payload = {
        "entity_code": f"BAD-{uuid.uuid4().hex[:8]}",
        "reference_date": "2026-11-09",
        "currency_code": "USD",
        "gross_amount": "1000.0000",
        "deduction_amount": "100.0000",
        "net_amount": "500.0000"  # Soma divergente -> Vai para Quarentena HITL!
    }

    try:
        # 1. Primeira execução válida -> COMMITTED_ACID
        res1 = processor.process_payload(valid_payload)
        assert res1["status"] == "COMMITTED_ACID"
        assert res1["net_amount"] == "1850.0000"

        # 2. Segunda execução idêntica -> DUPLICATE_IGNORED (Dupla Barreira de Idempotência)
        res2 = processor.process_payload(valid_payload)
        assert res2["status"] == "DUPLICATE_IGNORED"
        assert res2["record_id"] == res1["record_id"]

        # 3. Execução com erro de contrato -> QUARANTINED (Salvo no Mongo + quarantine_records no Postgres)
        res_bad = processor.process_payload(invalid_payload)
        assert res_bad["status"] == "QUARANTINED"
        assert "quarantine_id" in res_bad
    finally:
        # Limpeza pós-teste
        col.drop()
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM idempotency_registry WHERE source_system = 'TEST_SUITE'")
            )
            conn.execute(
                text("DELETE FROM canonical_records WHERE entity_code = :ec"),
                {"ec": entity_code},
            )
            conn.execute(
                text("DELETE FROM quarantine_records WHERE source_system = 'TEST_SUITE'")
            )
