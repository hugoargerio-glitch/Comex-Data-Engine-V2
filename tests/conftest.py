from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import NAMESPACE_DNS, uuid5

import pytest
from pymongo.errors import PyMongoError
from sqlalchemy.exc import SQLAlchemyError

# Carrega .env do projeto se existir
env_file = Path(__file__).parent.parent / ".env"
if env_file.exists():
    from dotenv import load_dotenv

    load_dotenv(env_file)


TEST_API_KEY = "unit-standard-api-key-2026"
TEST_AUDITOR_API_KEY = "unit-auditor-api-key-2026"
TEST_PII_SALT = "unit-test-pii-hash-salt-2026"


class _FakeResult:
    """Minimal SQLAlchemy Result stand-in for isolated unit tests."""

    def __init__(self, rows: Optional[List[Dict[str, Any]]] = None) -> None:
        self._rows: List[Dict[str, Any]] = rows or []

    def scalar(self) -> Any:
        if not self._rows:
            return None
        first = self._rows[0]
        return next(iter(first.values())) if first else None

    def scalar_one(self) -> Any:
        if len(self._rows) != 1:
            raise AssertionError(f"Expected 1 row from scalar_one(), got {len(self._rows)}")
        return next(iter(self._rows[0].values()))

    def scalar_one_or_none(self) -> Any:
        if not self._rows:
            return None
        return self.scalar_one()

    def mappings(self) -> "_FakeMappingsResult":
        return _FakeMappingsResult(self._rows)

    def fetchone(self) -> Optional[tuple[Any, ...]]:
        if not self._rows:
            return None
        return tuple(self._rows[0].values())

    def fetchall(self) -> List[tuple[Any, ...]]:
        return [tuple(r.values()) for r in self._rows]


class _FakeMappingsResult:
    def __init__(self, rows: List[Dict[str, Any]]) -> None:
        self._rows = rows

    def one_or_none(self) -> Optional[Dict[str, Any]]:
        if not self._rows:
            return None
        return self._rows[0]

    def all(self) -> List[Dict[str, Any]]:
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)


class _FakeTransactionContext:
    """Supports engine.begin(), engine.connect(), and transactional rollback on exception."""

    def __init__(self, backend: "FakeHTAPBackend", *, transactional: bool) -> None:
        self._backend = backend
        self._transactional = transactional
        self._snapshot: Optional[Dict[str, Any]] = None
        self._conn = FakePostgresConnection(backend)

    def __enter__(self) -> "FakePostgresConnection":
        if self._backend.fail_pg:
            raise SQLAlchemyError("Simulated PostgreSQL connection failure")
        if self._transactional:
            self._snapshot = deepcopy(self._backend.pg_state)
        return self._conn

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        if self._transactional and exc_type is not None and self._snapshot is not None:
            self._backend.pg_state = self._snapshot
        return False


class FakePostgresEngine:
    def __init__(self, backend: "FakeHTAPBackend") -> None:
        self._backend = backend

    def begin(self) -> _FakeTransactionContext:
        return _FakeTransactionContext(self._backend, transactional=True)

    def connect(self) -> _FakeTransactionContext:
        return _FakeTransactionContext(self._backend, transactional=False)


class FakePostgresConnection:
    """In-memory dispatcher for ComexHTAPProcessor & seed.py PostgreSQL statements."""

    def __init__(self, backend: "FakeHTAPBackend") -> None:
        self._backend = backend

    def execute(self, statement: Any, params: Any = None) -> _FakeResult:
        if self._backend.fail_pg:
            raise SQLAlchemyError("Simulated PostgreSQL query failure")

        sql_norm = " ".join(str(statement).split()).upper()
        state = self._backend.pg_state

        if isinstance(params, list):
            for row_params in params:
                self.execute(statement, row_params)
            return _FakeResult([])

        p: Dict[str, Any] = dict(params or {})

        if sql_norm == "SELECT 1":
            return _FakeResult([{"ok": 1}])

        if "INSERT INTO DIM_DATE" in sql_norm:
            if not state["dim_date"]:
                for day in range(1, 31):
                    state["dim_date"].add(20261100 + day)
            return _FakeResult([])

        if "INSERT INTO DIM_MERCHANDISE_NCM" in sql_norm:
            state["dim_merchandise_ncm"].update({"84713012", "30049099", "09011110", "85171300"})
            return _FakeResult([])

        if "INSERT INTO DIM_PARTNER_COUNTRY" in sql_norm:
            state["dim_partner_country"].update({249, 767, 160, 105})
            return _FakeResult([])

        if "SELECT PTAX_SELL_RATE FROM DIM_CURRENCY_PTAX" in sql_norm:
            curr = str(p["currency_code"]).upper()
            target_date_id = int(p["competence_date_id"])
            matches = [
                row
                for (d_id, c_code), row in state["dim_currency_ptax"].items()
                if c_code == curr and d_id <= target_date_id
            ]
            if not matches:
                return _FakeResult([])
            latest = max(matches, key=lambda r: r["date_id"])
            return _FakeResult([{"ptax_sell_rate": latest["ptax_sell_rate"]}])

        if "INSERT INTO DIM_CURRENCY_PTAX" in sql_norm:
            key = (int(p["date_id"]), str(p["currency_code"]).upper())
            state["dim_currency_ptax"][key] = {
                "date_id": key[0],
                "currency_code": key[1],
                "ptax_buy_rate": Decimal(str(p["buy"])),
                "ptax_sell_rate": Decimal(str(p["sell"])),
                "is_business_day": bool(p["biz_day"]),
                "is_interpolated_locf": bool(p["locf"]),
            }
            return _FakeResult([])

        if "FROM INGESTION_IDEMPOTENCY_REGISTRY WHERE PAYLOAD_HASH" in sql_norm:
            h = p.get("hash") or p.get("payload_hash")
            reg = state["ingestion_idempotency_registry"].get(h)
            return _FakeResult([dict(reg)] if reg is not None else [])

        if "INSERT INTO INGESTION_IDEMPOTENCY_REGISTRY" in sql_norm:
            h = p["hash"]
            overwrite = "DO UPDATE SET" in sql_norm
            if h not in state["ingestion_idempotency_registry"] or overwrite:
                state["ingestion_idempotency_registry"][h] = {
                    "payload_hash": h,
                    "source_system": p["source_system"],
                    "target_entity_id": p["target_id"],
                    "status": p["status"],
                }
            return _FakeResult([])

        if "SELECT ISSUER_TAX_ID, DOCUMENT_NUMBER, TOTAL_AMOUNT, CURRENCY_CODE FROM COMMERCIAL_INVOICES" in sql_norm:
            inv_id = str(p["invoice_id"])
            row = state["commercial_invoices"].get(inv_id)
            if row is None:
                return _FakeResult([])
            return _FakeResult(
                [
                    {
                        "issuer_tax_id": row["issuer_tax_id"],
                        "document_number": row["document_number"],
                        "total_amount": row["total_amount"],
                        "currency_code": row["currency_code"],
                    }
                ]
            )

        if "INSERT INTO COMMERCIAL_INVOICES" in sql_norm:
            nat_key = (
                str(p["issuer_tax_id"]),
                str(p["document_number"]),
                p["issue_date"],
                str(p["currency_code"]),
                Decimal(str(p["total_amount"])),
            )
            existing_id = state["commercial_invoices_nk"].get(nat_key)
            if existing_id is None:
                inv_uuid = p["invoice_id"]
                inv_id_str = str(inv_uuid)
                state["commercial_invoices_nk"][nat_key] = inv_uuid
                state["commercial_invoices"][inv_id_str] = {
                    "invoice_id": inv_uuid,
                    "issuer_tax_id": p["issuer_tax_id"],
                    "document_number": p["document_number"],
                    "issue_date": p["issue_date"],
                    "currency_code": p["currency_code"],
                    "subtotal_amount": p["subtotal_amount"],
                    "tax_amount": p["tax_amount"],
                    "freight_amount": p["freight_amount"],
                    "insurance_amount": p["insurance_amount"],
                    "total_amount": p["total_amount"],
                    "incoterm": p["incoterm"],
                    "extraction_confidence": p["extraction_confidence"],
                    "raw_payload_hash": p["raw_payload_hash"],
                    "mongo_raw_id": p["mongo_raw_id"],
                }
                return _FakeResult([{"invoice_id": inv_uuid}])

            existing_row = state["commercial_invoices"][str(existing_id)]
            existing_row["extraction_confidence"] = max(
                existing_row["extraction_confidence"],
                p["extraction_confidence"],
            )
            if p["mongo_raw_id"] is not None:
                existing_row["mongo_raw_id"] = p["mongo_raw_id"]
            existing_row["raw_payload_hash"] = p["raw_payload_hash"]
            return _FakeResult([{"invoice_id": existing_id}])

        if "INSERT INTO COMMERCIAL_INVOICE_LINES" in sql_norm:
            key = (str(p["invoice_id"]), int(p["line_number"]))
            if key not in state["commercial_invoice_lines"]:
                state["commercial_invoice_lines"][key] = dict(p)
            return _FakeResult([])

        if "FROM JOURNAL_ENTRIES WHERE ENTRY_ID" in sql_norm:
            entry_id_str = str(p["entry_id"])
            row = state["journal_entries"].get(entry_id_str)
            return _FakeResult([dict(row)] if row is not None else [])

        if "FROM JOURNAL_LINES WHERE ENTRY_ID" in sql_norm:
            entry_id = p["entry_id"]
            matching = [
                dict(r)
                for r in state["journal_lines"].values()
                if str(r["entry_id"]) == str(entry_id)
            ]
            matching.sort(key=lambda r: int(r["line_order"]))
            return _FakeResult(matching)

        if "INSERT INTO JOURNAL_ENTRIES" in sql_norm:
            entry_id_str = str(p["entry_id"])
            if entry_id_str not in state["journal_entries"]:
                state["journal_entries"][entry_id_str] = {
                    "entry_id": p["entry_id"],
                    "transaction_date": p["tx_date"],
                    "competence_date": p["comp_date"],
                    "reference_document_id": p["ref_doc"],
                    "description": p["desc"],
                    "status": "POSTED",
                }
            return _FakeResult([])

        if "INSERT INTO JOURNAL_LINES" in sql_norm:
            line_id_str = str(p["line_id"])
            if line_id_str not in state["journal_lines"]:
                state["journal_lines"][line_id_str] = {
                    "line_id": p["line_id"],
                    "entry_id": p["entry_id"],
                    "account_code": p["account_code"],
                    "debit_amount": Decimal(str(p["debit"])),
                    "credit_amount": Decimal(str(p["credit"])),
                    "currency_code": p["currency"],
                    "original_currency_amount": Decimal(str(p["orig_amount"])),
                    "exchange_rate_applied": Decimal(str(p["fx_rate"])),
                    "line_order": int(p["order"]),
                }
            return _FakeResult([])

        if "FROM COMEX_QUARANTINE_HITL" in sql_norm and sql_norm.startswith("SELECT"):
            rows = list(state["comex_quarantine_hitl"].values())
            if "WHERE RAW_PAYLOAD_HASH" in sql_norm:
                h = p.get("hash") or p.get("payload_hash")
                filtered = [r for r in rows if r["raw_payload_hash"] == h]
                filtered.sort(key=lambda r: r["created_at"], reverse=("DESC" in sql_norm))
                return _FakeResult(filtered[:1])
            if "WHERE QUARANTINE_ID" in sql_norm:
                qid_str = str(p["qid"])
                row = state["comex_quarantine_hitl"].get(qid_str)
                return _FakeResult([dict(row)] if row is not None else [])
            if "WHERE REVIEW_STATUS = :STATUS_FILTER" in sql_norm:
                rows = [r for r in rows if r["review_status"] == p["status_filter"]]
            elif "REVIEW_STATUS = 'PENDING_REVIEW'" in sql_norm:
                rows = [r for r in rows if r["review_status"] == "PENDING_REVIEW"]
            elif "REVIEW_STATUS = 'APPROVED_OVERRIDE'" in sql_norm:
                rows = [r for r in rows if r["review_status"] == "APPROVED_OVERRIDE"]
            if sql_norm.startswith("SELECT COUNT(*)"):
                return _FakeResult([{"count": len(rows)}])
            rows.sort(key=lambda r: (r["created_at"], str(r["quarantine_id"])))
            rows.sort(key=lambda r: r["created_at"], reverse=True)
            offset = int(p.get("offset", 0))
            limit = int(p.get("limit", 50))
            return _FakeResult(rows[offset : offset + limit])

        if "INSERT INTO COMEX_QUARANTINE_HITL" in sql_norm:
            qid_str = str(p["quarantine_id"])
            if qid_str not in state["comex_quarantine_hitl"]:
                self._backend.seq += 1
                raw_errs = p["errors"]
                parsed_errs = json.loads(raw_errs) if isinstance(raw_errs, str) else raw_errs
                state["comex_quarantine_hitl"][qid_str] = {
                    "quarantine_id": p["quarantine_id"],
                    "source_type": p["source_type"],
                    "raw_payload_hash": p["payload_hash"],
                    "mongo_raw_id": p["mongo_raw_id"],
                    "validation_errors": parsed_errs,
                    "self_correction_attempts": int(p["attempts"]),
                    "review_status": "PENDING_REVIEW",
                    "reviewed_by": None,
                    "reviewed_at": None,
                    "created_at": datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
                    + timedelta(microseconds=self._backend.seq),
                }
            return _FakeResult([])

        if "UPDATE COMEX_QUARANTINE_HITL" in sql_norm:
            qid_str = str(p["qid"])
            row = state["comex_quarantine_hitl"].get(qid_str)
            if row is None or row["review_status"] != "PENDING_REVIEW":
                return _FakeResult([])
            new_status = "REJECTED_HITL" if "REJECTED_HITL" in sql_norm else "APPROVED_OVERRIDE"
            row["review_status"] = new_status
            row["reviewed_by"] = p["reviewed_by"]
            row["reviewed_at"] = datetime.now(timezone.utc)
            return _FakeResult([{"quarantine_id": row["quarantine_id"]}])

        if "SELECT NATIONALIZATION_ID, JOURNAL_ENTRY_ID FROM CUSTOMS_NATIONALIZATIONS" in sql_norm:
            decl = p["declaration_number"]
            row = state["customs_nationalizations"].get(decl)
            if row is None:
                return _FakeResult([])
            return _FakeResult(
                [
                    {
                        "nationalization_id": row["nationalization_id"],
                        "journal_entry_id": row["journal_entry_id"],
                    }
                ]
            )

        if "UPDATE CUSTOMS_NATIONALIZATIONS SET JOURNAL_ENTRY_ID" in sql_norm:
            nat_id = p["nat_id"]
            for row in state["customs_nationalizations"].values():
                if row["nationalization_id"] == nat_id:
                    row["journal_entry_id"] = p["entry_id"]
            return _FakeResult([])

        if "INSERT INTO CUSTOMS_NATIONALIZATIONS" in sql_norm:
            decl = p["declaration_number"]
            if decl not in state["customs_nationalizations"]:
                state["customs_nationalizations"][decl] = {
                    "nationalization_id": p["nat_id"],
                    "declaration_number": decl,
                    "journal_entry_id": p["journal_entry_id"],
                    "landed_cost_brl": p["landed_cost"],
                }
            return _FakeResult([{"nationalization_id": state["customs_nationalizations"][decl]["nationalization_id"]}])

        if "INSERT INTO FACT_COMEX_OPERATIONS" in sql_norm:
            state["fact_comex_operations"][p["hash_id"]] = dict(p)
            return _FakeResult([])

        if "SUM(DEBIT_AMOUNT) - SUM(CREDIT_AMOUNT)" in sql_norm:
            delta = sum(
                (r["debit_amount"] - r["credit_amount"] for r in state["journal_lines"].values()),
                Decimal("0.0000"),
            )
            return _FakeResult([{"delta": delta}])

        if sql_norm.startswith("SELECT COUNT(*) FROM "):
            table = sql_norm.split("FROM ", 1)[1].split()[0].lower()
            return _FakeResult([{"count": len(state.get(table, {}))}])

        raise NotImplementedError(f"Unsupported SQL in FakePostgresConnection: {sql_norm}")


class FakeMongoCollection:
    def __init__(self, backend: "FakeHTAPBackend", name: str) -> None:
        self._backend = backend
        self.name = name

    def create_index(self, *args: Any, **kwargs: Any) -> str:
        if self._backend.fail_mongo:
            raise PyMongoError("Simulated MongoDB index failure")
        return "payload_hash_1"

    def update_one(self, flt: Dict[str, Any], update: Dict[str, Any], upsert: bool = False) -> None:
        if self._backend.fail_mongo:
            raise PyMongoError("Simulated MongoDB write failure")
        col = self._backend.mongo_docs.setdefault(self.name, {})
        h = flt["payload_hash"]
        if h not in col and upsert:
            doc = deepcopy(update.get("$setOnInsert", {}))
            doc["_id"] = str(uuid5(NAMESPACE_DNS, f"{self.name}:{h}")).replace("-", "")[:24]
            col[h] = doc

    def find_one(self, flt: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if self._backend.fail_mongo:
            raise PyMongoError("Simulated MongoDB read failure")
        col = self._backend.mongo_docs.get(self.name, {})
        return deepcopy(col.get(flt["payload_hash"]))

    def count_documents(self, flt: Dict[str, Any]) -> int:
        if self._backend.fail_mongo:
            raise PyMongoError("Simulated MongoDB count failure")
        return len(self._backend.mongo_docs.get(self.name, {}))


class FakeMongoAdmin:
    def __init__(self, backend: "FakeHTAPBackend") -> None:
        self._backend = backend

    def command(self, cmd: str) -> Dict[str, Any]:
        if self._backend.fail_mongo:
            raise PyMongoError("Simulated MongoDB ping failure")
        return {"ok": 1}


class FakeMongoClient:
    def __init__(self, backend: "FakeHTAPBackend") -> None:
        self._backend = backend
        self.admin = FakeMongoAdmin(backend)

    def __getitem__(self, db_name: str) -> "FakeMongoDatabase":
        return FakeMongoDatabase(self._backend)


class FakeMongoDatabase:
    def __init__(self, backend: "FakeHTAPBackend") -> None:
        self._backend = backend

    def __getitem__(self, col_name: str) -> FakeMongoCollection:
        return FakeMongoCollection(self._backend, col_name)


class FakeHTAPBackend:
    """Isolated, stateful HTAP backend (PG + Mongo + baseline PTAX rates) for unit tests."""

    def __init__(self) -> None:
        self.fail_pg = False
        self.fail_mongo = False
        self.seq = 0
        self.mongo_docs: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.pg_state: Dict[str, Any] = {
            "dim_date": set(),
            "dim_merchandise_ncm": set(),
            "dim_partner_country": set(),
            "dim_currency_ptax": {},
            "ingestion_idempotency_registry": {},
            "commercial_invoices": {},
            "commercial_invoices_nk": {},
            "commercial_invoice_lines": {},
            "journal_entries": {},
            "journal_lines": {},
            "comex_quarantine_hitl": {},
            "customs_nationalizations": {},
            "fact_comex_operations": {},
        }
        self.engine = FakePostgresEngine(self)
        self.mongo_client = FakeMongoClient(self)
        self.seed_default_ptax()

    def seed_ptax(
        self,
        date_id: int,
        currency_code: str,
        buy_rate: str,
        sell_rate: str,
        is_business_day: bool = True,
        is_interpolated_locf: bool = False,
    ) -> None:
        self.pg_state["dim_currency_ptax"][(date_id, currency_code.upper())] = {
            "date_id": date_id,
            "currency_code": currency_code.upper(),
            "ptax_buy_rate": Decimal(buy_rate),
            "ptax_sell_rate": Decimal(sell_rate),
            "is_business_day": is_business_day,
            "is_interpolated_locf": is_interpolated_locf,
        }

    def clear_ptax(self) -> None:
        self.pg_state["dim_currency_ptax"].clear()

    def seed_default_ptax(self) -> None:
        defaults = [
            (20261106, "USD", "5.822800", "5.823400", True, False),
            (20261109, "USD", "5.831000", "5.831600", True, False),
            (20261106, "EUR", "6.280000", "6.281000", True, False),
            (20261109, "EUR", "6.290000", "6.291000", True, False),
            (20261106, "CHF", "6.640000", "6.642000", True, False),
            (20261109, "CHF", "6.650000", "6.652000", True, False),
        ]
        for date_id, curr, buy, sell, biz, locf in defaults:
            self.seed_ptax(date_id, curr, buy, sell, biz, locf)

    def get_raw_collection(self, collection_name: str) -> FakeMongoCollection:
        return FakeMongoCollection(self, collection_name)


@pytest.fixture
def fake_htap_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> FakeHTAPBackend:
    """Patches PG, Mongo, Lakehouse paths, and runtime security env vars cleanly per test."""
    import src.api.main as api_main
    import src.connectors.lakehouse as lakehouse
    import src.connectors.mongo as mongo_conn
    import src.connectors.postgres as pg_conn
    import src.pipeline.comex_processor as proc_mod

    backend = FakeHTAPBackend()

    monkeypatch.setenv("API_KEY", TEST_API_KEY)
    monkeypatch.setenv("AUDITOR_API_KEY", TEST_AUDITOR_API_KEY)
    monkeypatch.setenv("PII_HASH_SALT", TEST_PII_SALT)
    monkeypatch.setenv("RATE_LIMIT_PER_MINUTE", "1000")
    monkeypatch.setenv("PARQUET_BASE_DIR", str(tmp_path / "lakehouse"))
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "lakehouse.duckdb"))

    monkeypatch.setattr(lakehouse, "PARQUET_BASE_DIR", str(tmp_path / "lakehouse"))
    monkeypatch.setattr(lakehouse, "DUCKDB_PATH", str(tmp_path / "lakehouse.duckdb"))

    monkeypatch.setattr(pg_conn, "engine", backend.engine)
    monkeypatch.setattr(proc_mod, "default_postgres_engine", backend.engine)
    monkeypatch.setattr(api_main, "postgres_engine", backend.engine)

    monkeypatch.setattr(mongo_conn, "get_mongo_client", lambda: backend.mongo_client)
    monkeypatch.setattr(mongo_conn, "get_raw_collection", backend.get_raw_collection)
    monkeypatch.setattr(proc_mod, "get_raw_collection", backend.get_raw_collection)
    monkeypatch.setattr(api_main, "get_mongo_client", lambda: backend.mongo_client)

    api_main.app.state.limiter.reset()
    api_main.app.state.metrics.reset()

    return backend


@pytest.fixture
def api_client(fake_htap_env: FakeHTAPBackend):
    from fastapi.testclient import TestClient
    import src.api.main as api_main

    return TestClient(api_main.app)
