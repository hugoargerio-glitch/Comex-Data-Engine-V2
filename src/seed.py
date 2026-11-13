"""
Seed Runner — Comex Resilient Data Engine V2 (HTAP Profile)
Populates MongoDB 7, PostgreSQL 16 (ACID Fiduciary Ledger), and DuckDB/Parquet (OLAP Star Schema)
with realistic customs, PTAX, invoice, and SWIFT settlement datasets from samples/.
"""

import csv
import json
import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

from alembic import command
from alembic.config import Config
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

from src.connectors.lakehouse import duckdb_connection, get_parquet_base_dir
from src.connectors.mongo import get_mongo_client
from src.connectors.postgres import engine
from src.extractors.http_client import compute_payload_hash
from src.models.customs import (
    ComexOperationInput,
    CustomsNationalizationCalcInput,
    PTAXRateInput,
)
from src.pipeline.comex_processor import ComexHTAPProcessor

BASE_DIR = Path(__file__).resolve().parent.parent
SAMPLES_DIR = BASE_DIR / "samples"


def apply_database_ddl() -> None:
    """Executa Alembic migrations (`upgrade head`) e popula dimensões de referência idempotentes."""
    print("🐘 [PostgreSQL] Executando Alembic upgrade head e dimensões conformadas...")
    alembic_cfg = Config(str(BASE_DIR / "alembic.ini"))
    alembic_cfg.set_main_option("script_location", str(BASE_DIR / "alembic"))
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        alembic_cfg.set_main_option("sqlalchemy.url", db_url)
    command.upgrade(alembic_cfg, "head")

    with engine.begin() as conn:

        # Popula dim_date para Novembro de 2026 (evita ForeignKeyViolation)
        conn.execute(
            text(
                """
                INSERT INTO dim_date (date_id, full_date, year, month, day, quarter, is_business_day)
                SELECT
                    TO_CHAR(d, 'YYYYMMDD')::INT,
                    d::DATE,
                    EXTRACT(YEAR FROM d)::INT,
                    EXTRACT(MONTH FROM d)::INT,
                    EXTRACT(DAY FROM d)::INT,
                    EXTRACT(QUARTER FROM d)::INT,
                    EXTRACT(DOW FROM d) NOT IN (0, 6)
                FROM generate_series('2026-11-01'::DATE, '2026-11-30'::DATE, '1 day'::INTERVAL) d
                ON CONFLICT (date_id) DO NOTHING;
                """
            )
        )

        # Popula dim_merchandise_ncm de referência
        conn.execute(
            text(
                """
                INSERT INTO dim_merchandise_ncm (ncm_code, hs_code, description, unit_measure)
                VALUES
                    ('84713012', '847130', 'Laptops corporativos e computadores portáteis', 'UNIDADE'),
                    ('30049099', '300490', 'Medicamentos e produtos farmacêuticos dosados', 'KG'),
                    ('09011110', '090111', 'Café verde em grão arábica para exportação', 'KG'),
                    ('85171300', '851713', 'Smartphones e dispositivos de conectividade 5G', 'UNIDADE')
                ON CONFLICT (ncm_code) DO NOTHING;
                """
            )
        )

        # Popula dim_partner_country de referência
        conn.execute(
            text(
                """
                INSERT INTO dim_partner_country (country_code, country_iso3, country_name, economic_block)
                VALUES
                    (249, 'USA', 'Estados Unidos da América', 'USMCA'),
                    (767, 'CHE', 'Suíça', 'EFTA'),
                    (160, 'CHN', 'China', 'BRICS'),
                    (105, 'BRA', 'Brasil', 'MERCOSUL')
                ON CONFLICT (country_code) DO NOTHING;
                """
            )
        )

    print("   ✅ DDLs, Triggers e Dimensões Conformadas configuradas com sucesso!")


def seed_commercial_invoices(processor: ComexHTAPProcessor) -> Dict[str, int]:
    """Processa o lote de faturas comerciais de samples/."""
    invoices_file = SAMPLES_DIR / "commercial_invoices_batch.json"
    if not invoices_file.exists():
        print(f"⚠️ Arquivo {invoices_file} não encontrado.")
        return {"processed": 0, "quarantined": 0}

    print("\n📦 [HTAP Ingestion] Processando Faturas Comerciais (samples/commercial_invoices_batch.json)...")
    with open(invoices_file, "r", encoding="utf-8") as f:
        invoices_data: List[Dict[str, Any]] = json.load(f)

    counts = {"COMMITTED_ACID": 0, "QUARANTINED": 0, "DUPLICATE_IGNORED": 0}

    for inv in invoices_data:
        res = processor.process_invoice(inv)
        status = res.get("status", "UNKNOWN")
        counts[status] = counts.get(status, 0) + 1
        doc_num = inv.get("document_number", "N/A")
        print(f"   • Fatura {doc_num:<22} -> Status: {status}")

    return counts


def seed_hitl_quarantine_resolution(processor: ComexHTAPProcessor) -> Dict[str, Any]:
    """
    Simula a intervenção humana (HITL) na fila de quarentena de forma 100% idempotente:
    Localiza INV-2026-ERR-CIF pelo raw_payload_hash determinístico, verifica se já foi
    aprovada em execuções anteriores e, se ainda PENDING_REVIEW, aplica a retificação
    auditada e promove para o Core Operacional + Ledger.
    Mantém INV-2026-ERR-MATH em PENDING_REVIEW para auditoria externa.
    """
    print("\n🔍 [HITL Quarantine Resolution] Processando fila de resolução humana...")
    invoices_file = SAMPLES_DIR / "commercial_invoices_batch.json"
    if not invoices_file.exists():
        raise FileNotFoundError(f"Seed sample file missing: {invoices_file}")

    with open(invoices_file, "r", encoding="utf-8") as f:
        invoices_data: List[Dict[str, Any]] = json.load(f)

    cif_raw_payload = next(
        (item for item in invoices_data if item.get("document_number") == "INV-2026-ERR-CIF"),
        None,
    )
    if cif_raw_payload is None:
        raise RuntimeError("INV-2026-ERR-CIF not found in commercial_invoices_batch.json")

    cif_payload_hash = compute_payload_hash(cif_raw_payload)
    with engine.connect() as conn:
        q_row = conn.execute(
            text(
                """
                SELECT quarantine_id, review_status, reviewed_by
                FROM comex_quarantine_hitl
                WHERE raw_payload_hash = :hash
                ORDER BY created_at ASC
                LIMIT 1
                """
            ),
            {"hash": cif_payload_hash},
        ).mappings().one_or_none()

    if q_row is None:
        raise RuntimeError(
            "Quarantine record for INV-2026-ERR-CIF was not found after invoice ingestion."
        )

    cif_quarantine_id = str(q_row["quarantine_id"])
    current_status = str(q_row["review_status"])

    if current_status == "APPROVED_OVERRIDE":
        print(
            f"   ⏭️ Quarentena {cif_quarantine_id} (INV-2026-ERR-CIF) já resolvida "
            f"como APPROVED_OVERRIDE; replay idempotente sem mutações."
        )
        return {
            "quarantine_id": cif_quarantine_id,
            "review_status": "APPROVED_OVERRIDE",
            "reviewed_by": q_row.get("reviewed_by"),
            "idempotent_replay": True,
        }

    if current_status != "PENDING_REVIEW":
        raise RuntimeError(
            f"Unexpected quarantine state for INV-2026-ERR-CIF ({cif_quarantine_id}): {current_status}"
        )

    corrected_payload = {
        "issuer_tax_id": "CHE-116.281.710 MWST",
        "document_number": "INV-2026-ERR-CIF",
        "issue_date": "2026-11-09",
        "currency_code": "CHF",
        "subtotal_amount": "85000.0000",
        "tax_amount": "0.0000",
        "freight_amount": "3500.0000",
        "insurance_amount": "500.0000",
        "total_amount": "89000.0000",
        "incoterm": "CIF",
        "extraction_confidence": "0.9800",
        "lines": [
            {
                "line_number": 1,
                "ncm_code": "30049099",
                "description": "Medicamentos e princípios ativos biotecnológicos (frete/seguro auditados)",
                "quantity": "42.5000",
                "unit_price": "2000.0000",
                "total_price": "85000.0000",
            }
        ],
    }

    result = processor.resolve_quarantined_invoice(
        quarantine_id=cif_quarantine_id,
        resolution_action="APPROVED_OVERRIDE",
        reviewed_by="hugo.auditor@comex.br",
        corrected_payload=corrected_payload,
    )
    print(f"   ✅ Quarentena {cif_quarantine_id} promovida com sucesso! Status: {result.get('review_status')} (Auditado por: {result.get('reviewed_by')})")
    print(f"      • Documento: {result.get('document_number')} | Invoice ID: {result.get('invoice_id')} | Journal Entry ID: {result.get('journal_entry_id')}")
    print("      • INV-2026-ERR-MATH mantida em PENDING_REVIEW para auditoria fiscal.")
    return result


def seed_ptax_rates(processor: ComexHTAPProcessor) -> int:
    """Popula a dimensão PTAX com preenchimento LOCF."""
    ptax_file = SAMPLES_DIR / "bacen_ptax_sample.json"
    if not ptax_file.exists():
        return 0

    print("\n💱 [FX Dimension] Populando dim_currency_ptax (samples/bacen_ptax_sample.json)...")
    with open(ptax_file, "r", encoding="utf-8") as f:
        raw_rates = json.load(f)

    rates = [PTAXRateInput.model_validate(r) for r in raw_rates]
    persisted = processor.persist_ptax_rates(rates)
    print(f"   ✅ {persisted} cotações PTAX consolidadas (incluindo preenchimento LOCF de final de semana).")
    return persisted


def seed_swift_settlements(processor: Optional[ComexHTAPProcessor] = None) -> int:
    """
    Gera lançamentos contábeis de liquidação bancária com split de tarifa SWIFT
    usando uuid5 determinístico + idempotência completa (Mongo + PostgreSQL Ledger).
    """
    swift_file = SAMPLES_DIR / "swift_camt053_statements.json"
    if not swift_file.exists():
        return 0

    print("\n🏦 [Fiduciary Ledger] Escriturando liquidações bancárias SWIFT (Partidas Dobradas Compostas)...")
    with open(swift_file, "r", encoding="utf-8") as f:
        statements = json.load(f)

    proc = processor or ComexHTAPProcessor(source_system="SEED_SWIFT_RUNNER")
    inserted = 0
    for stmt in statements:
        t_date = datetime.strptime(stmt["settlement_date"], "%Y-%m-%d").date()
        c_date = datetime.strptime(stmt["competence_date"], "%Y-%m-%d").date()
        res = proc.reconcile_swift_settlement(
            reference_document_id=stmt["invoice_number"],
            transaction_date=t_date,
            competence_date=c_date,
            gross_amount=Decimal(str(stmt["nominal_amount"])),
            fee_amount=Decimal(str(stmt["swift_fee_deducted"])),
            currency_code=stmt["currency_code"],
            exchange_rate=Decimal(str(stmt["spot_rate_settlement"])),
            cash_account=stmt.get("account_credited", "1.1.1.01"),
            fee_account="4.1.2.01",
            ar_account=stmt.get("account_receivable", "1.1.2.01"),
            raw_payload=stmt,
        )
        if res.get("status") == "RECONCILED":
            inserted += 1
            print(f"   • SWIFT {stmt['end_to_end_id']} escriturado (Soma Zero garantida)")
        else:
            print(f"   ⏭️ SWIFT {stmt['end_to_end_id']} já escriturado (replay idempotente)")

    return inserted


def seed_comex_mdic_operations(processor: ComexHTAPProcessor) -> int:
    """Popula fact_comex_operations em PostgreSQL e DuckDB/Parquet."""
    mdic_file = SAMPLES_DIR / "comex_mdic_sample.csv"
    if not mdic_file.exists():
        return 0

    print("\n🚢 [Star Schema OLAP] Populando fact_comex_operations (samples/comex_mdic_sample.csv)...")
    operations: List[ComexOperationInput] = []
    with open(mdic_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            date_id = int(f"{row['year']}{int(row['month']):02d}01")
            op = ComexOperationInput(
                date_id=date_id,
                operation_type=row["operation_type"],
                ncm_code=row["ncm_code"],
                country_code=int(row["country_code"]),
                uf_code=row["uf_code"],
                modal_code=int(row["modal_code"]),
                urf_code=int(row["urf_code"]),
                statistical_quantity=row["statistical_quantity"],
                net_weight_kg=row["net_weight_kg"],
                fob_amount_usd=row["fob_amount_usd"],
                freight_amount_usd=row["freight_amount_usd"],
                insurance_amount_usd=row["insurance_amount_usd"],
                cif_amount_usd=row["cif_amount_usd"],
                landed_cost_brl=row["landed_cost_brl"],
            )
            operations.append(op)

    persisted = processor.persist_comex_operations(operations)
    print(f"   ✅ {persisted} operações aduaneiras persistidas no Star Schema e Parquet Lakehouse.")
    return persisted


def seed_customs_nationalizations(processor: ComexHTAPProcessor) -> int:
    """Processa e persiste as declarações de nacionalização aduaneira (DI/Duimp) com tributos em cascata."""
    nat_file = SAMPLES_DIR / "customs_nationalizations_sample.json"
    if not nat_file.exists():
        print(f"⚠️ Arquivo {nat_file} não encontrado.")
        return 0

    print("\n🏛️ [Customs Nationalization] Processando Declarações Aduaneiras (samples/customs_nationalizations_sample.json)...")
    with open(nat_file, "r", encoding="utf-8") as f:
        nat_data: List[Dict[str, Any]] = json.load(f)

    persisted = 0
    for raw in nat_data:
        calc_in = CustomsNationalizationCalcInput.model_validate(raw)
        res, entry = processor.persist_customs_nationalization(calc_in, persist_db=True)
        persisted += 1
        print(f"   • DI/Duimp {res.declaration_number:<16} ({res.tax_regime}) -> Landed Cost: R$ {res.landed_cost_brl} (Lançamento: {len(entry.lines)} pernas)")

    return persisted


def print_summary() -> None:
    """Exibe o relatório de telemetria da carga."""
    print("\n" + "=" * 70)
    print(" 📊 RESUMO DE TELEMETRIA DO BANCO (DBeaver / MongoDB / DuckDB)")
    print("=" * 70)

    # Contagens no PostgreSQL
    with engine.connect() as conn:
        invoices_count = conn.execute(text("SELECT COUNT(*) FROM commercial_invoices")).scalar() or 0
        entries_count = conn.execute(text("SELECT COUNT(*) FROM journal_entries")).scalar() or 0
        lines_count = conn.execute(text("SELECT COUNT(*) FROM journal_lines")).scalar() or 0
        nationalizations_count = conn.execute(text("SELECT COUNT(*) FROM customs_nationalizations")).scalar() or 0
        quarantine_count = conn.execute(text("SELECT COUNT(*) FROM comex_quarantine_hitl")).scalar() or 0
        quarantine_pending = conn.execute(
            text("SELECT COUNT(*) FROM comex_quarantine_hitl WHERE review_status = 'PENDING_REVIEW'")
        ).scalar() or 0
        quarantine_approved = conn.execute(
            text("SELECT COUNT(*) FROM comex_quarantine_hitl WHERE review_status = 'APPROVED_OVERRIDE'")
        ).scalar() or 0
        ptax_count = conn.execute(text("SELECT COUNT(*) FROM dim_currency_ptax")).scalar() or 0
        facts_count = conn.execute(text("SELECT COUNT(*) FROM fact_comex_operations")).scalar() or 0

        # Balancete de Soma Zero
        zero_sum_delta = conn.execute(
            text("SELECT COALESCE(SUM(debit_amount) - SUM(credit_amount), 0) FROM journal_lines")
        ).scalar() or Decimal("0.0000")

    # Contagem no MongoDB (falha explicitamente se indisponível)
    mclient = get_mongo_client()
    mongo_raw_count = mclient[os.getenv("MONGO_DB", "comex_raw_lake")]["raw_commercial_invoices"].count_documents({})

    # Contagem no DuckDB/Parquet (fecha conexão em finally e falha explicitamente em erro)
    parquet_fact_dir = get_parquet_base_dir() / "fact_comex_operations"
    parquet_glob = (parquet_fact_dir / "**" / "*.parquet").resolve().as_posix()
    with duckdb_connection() as dcon:
        if any(parquet_fact_dir.rglob("*.parquet")):
            row = dcon.execute(
                "SELECT COUNT(*) FROM read_parquet(?, hive_partitioning=true)",
                [parquet_glob],
            ).fetchone()
            duckdb_facts = int(row[0]) if row is not None else 0
        else:
            dcon.execute("SELECT 1").fetchone()
            duckdb_facts = 0

    print(f" 🐘 PostgreSQL commercial_invoices:     {invoices_count:>5} registros (Validados & Desembaraçados)")
    print(f" 🐘 PostgreSQL journal_entries:         {entries_count:>5} lançamentos contábeis")
    print(f" 🐘 PostgreSQL journal_lines:           {lines_count:>5} partidas dobradas")
    print(f" 🐘 PostgreSQL Soma Zero Invariante:    {str(zero_sum_delta):>5} (Zero Absoluto)")
    print(f" 🏛️ PostgreSQL customs_nationalizations: {nationalizations_count:>4} declarações (Cascata & CPC 16)")
    print(f" 🚨 PostgreSQL comex_quarantine_hitl:   {quarantine_count:>5} divergências ({quarantine_pending} PENDING, {quarantine_approved} APPROVED_OVERRIDE)")
    print(f" 💱 PostgreSQL dim_currency_ptax:       {ptax_count:>5} cotações diárias (com LOCF)")
    print(f" 🚢 PostgreSQL fact_comex_operations:   {facts_count:>5} células aduaneiras")
    print(f" 🍃 MongoDB raw_commercial_invoices:    {str(mongo_raw_count):>5} BSONs brutos")
    print(f" 🦆 DuckDB fact_comex_operations:       {str(duckdb_facts):>5} linhas Parquet Hive")
    print("=" * 70)
    print(" 🚀 Seed físico concluído com sucesso!")
    print(" Dica: Abra o DBeaver e execute os scripts de sql/queries/ para inspecionar!")
    print("=" * 70 + "\n")


def main() -> None:
    print("🚢 Iniciando Seed Físico do Comex Data Engine V2...")
    apply_database_ddl()

    processor = ComexHTAPProcessor(source_system="SEED_RUNNER")
    # Seed PTAX rates BEFORE invoices, HITL overrides, and customs calculations
    seed_ptax_rates(processor)
    seed_commercial_invoices(processor)
    seed_hitl_quarantine_resolution(processor)
    seed_swift_settlements(processor)
    seed_comex_mdic_operations(processor)
    seed_customs_nationalizations(processor)
    print_summary()


if __name__ == "__main__":
    main()
