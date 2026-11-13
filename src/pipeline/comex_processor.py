"""
Pipeline Processor: Comex HTAP & Fiduciary Engine V2
Implements the 3-tier idempotency architecture, MongoDB Raw Landing Zone,
Pydantic V2 Agentic Self-Correction Loop (up to 3 iterations), Shift-Left Quarantine HITL,
PostgreSQL 16 ACID Fiduciary Ledger atomic upsert, and DuckDB/Parquet Star Schema persistence.
"""

from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
import logging
import math
import os
from typing import Any, Dict, List, Optional, Tuple
from uuid import NAMESPACE_DNS, UUID, uuid5

import pyarrow as pa
from pydantic import ValidationError
from pymongo.errors import PyMongoError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.connectors.lakehouse import write_records_to_parquet_hive
from src.connectors.mongo import get_raw_collection
from src.connectors.postgres import engine as default_postgres_engine
from src.extractors.http_client import compute_payload_hash
from src.models.customs import (
    ComexOperationInput,
    CustomsNationalizationCalcInput,
    CustomsNationalizationResult,
    JournalEntryInput,
    JournalLineInput,
    PTAXRateInput,
    build_import_nationalization_ledger_entry,
    build_swift_fee_split_entry,
    compute_brazilian_landed_cost,
)
from src.models.invoices import CommercialInvoiceInput, hash_natural_person_pii
from src.models.quarantine import VALID_QUARANTINE_REVIEW_STATUSES

logger = logging.getLogger("comex.pipeline.processor")

COMEX_UUID_NAMESPACE = uuid5(NAMESPACE_DNS, "comex.fiduciary.engine.v2")
_UNSET = object()

_NATURAL_PII_KEYS = frozenset(
    {
        "auditor_email",
        "buyer_email",
        "contact_email",
        "contact_name",
        "contact_phone",
        "cpf",
        "customer_email",
        "natural_person_cpf",
        "person_cpf",
        "reviewed_by",
        "seller_email",
        "signer_email",
        "signer_name",
    }
)

PTAX_PARQUET_SCHEMA = pa.schema(
    [
        ("year", pa.int32()),
        ("month", pa.int32()),
        ("date_id", pa.int32()),
        ("currency_code", pa.string()),
        ("ptax_buy_rate", pa.decimal128(19, 6)),
        ("ptax_sell_rate", pa.decimal128(19, 6)),
        ("is_business_day", pa.bool_()),
        ("is_interpolated_locf", pa.bool_()),
    ]
)

FACT_COMEX_PARQUET_SCHEMA = pa.schema(
    [
        ("year", pa.int32()),
        ("month", pa.int32()),
        ("operation_hash_id", pa.string()),
        ("date_id", pa.int32()),
        ("operation_type", pa.string()),
        ("ncm_code", pa.string()),
        ("country_code", pa.int32()),
        ("uf_code", pa.string()),
        ("modal_code", pa.int32()),
        ("urf_code", pa.int32()),
        ("statistical_quantity", pa.decimal128(19, 4)),
        ("net_weight_kg", pa.decimal128(19, 4)),
        ("fob_amount_usd", pa.decimal128(19, 4)),
        ("freight_amount_usd", pa.decimal128(19, 4)),
        ("insurance_amount_usd", pa.decimal128(19, 4)),
        ("cif_amount_usd", pa.decimal128(19, 4)),
        ("landed_cost_brl", pa.decimal128(19, 4)),
    ]
)


class InfrastructureError(RuntimeError):
    """Base exception for backing storage / infrastructure failures."""


class MongoLandingError(InfrastructureError):
    """Raised when raw BSON persistence in MongoDB fails."""


class LedgerPersistenceError(InfrastructureError):
    """Raised when PostgreSQL ACID persistence or query execution fails."""


class LakehousePersistenceError(InfrastructureError):
    """Raised when DuckDB / Apache Parquet Hive persistence fails."""


class MissingPTAXRateError(ValueError):
    """Raised when no competence-date PTAX FX rate exists for a non-BRL currency."""


class QuarantineNotFoundError(KeyError):
    """Raised when a requested quarantine_id does not exist."""


class QuarantineStateConflictError(ValueError):
    """Raised when attempting to modify an already-finalized quarantine record."""


def _sanitize_validation_errors(errors: Any) -> List[Dict[str, Any]]:
    """
    Normalizes Pydantic ValidationError details into a non-empty JSON-safe array
    containing only type, loc, and msg (excluding raw input payloads and exception objects).
    """
    if not isinstance(errors, list) or not errors:
        return [{"type": "validation_error", "loc": [], "msg": "Contract validation failed."}]

    sanitized: List[Dict[str, Any]] = []
    for err in errors:
        if isinstance(err, dict):
            raw_loc = err.get("loc", ())
            loc_list = [
                item if isinstance(item, (str, int)) else str(item)
                for item in (raw_loc if isinstance(raw_loc, (list, tuple)) else [raw_loc])
            ]
            sanitized.append(
                {
                    "type": str(err.get("type", "value_error")),
                    "loc": loc_list,
                    "msg": str(err.get("msg", "Validation error")),
                }
            )
        elif isinstance(err, str) and err.strip():
            sanitized.append({"type": "value_error", "loc": [], "msg": err.strip()})

    return sanitized or [{"type": "validation_error", "loc": [], "msg": "Contract validation failed."}]


def _normalize_review_status_token(raw_status: str) -> str:
    """Normalizes legacy 'REJECTED' alias to canonical 'REJECTED_HITL'."""
    token = raw_status.strip().upper()
    if token == "REJECTED":
        return "REJECTED_HITL"
    return token


class ComexHTAPProcessor:
    """
    Dual-Store HTAP & Fiduciary Orchestrator:
    1. Edge SHA-256 Fingerprint + MongoDB Raw Landing Zone (Append-Only BSON Lake).
    2. Pre-validation Idempotency Registry lookup (prevents duplicate quarantine rows).
    3. Pydantic V2 Shift-Left Validation with 3-Iteration Agentic Self-Correction Loop.
    4. Operational Quarantine (comex_quarantine_hitl) for unresolvable schema drift or math errors.
    5. Relational ACID Barrier: PostgreSQL atomic upsert in commercial_invoices + journal entries.
    6. Columnar Analytical Barrier: Parquet Hive / DuckDB persistence with operation_hash_id deduplication.
    """

    def __init__(
        self,
        source_system: str = "COMEX_INVOICE_PIPELINE",
        mongo_collection: str = "raw_commercial_invoices",
        engine: Any = _UNSET,
        pii_hash_salt: Optional[str] = None,
    ) -> None:
        self.source_system = source_system
        self.mongo_collection = mongo_collection
        self.engine = default_postgres_engine if engine is _UNSET else engine
        self._explicit_pii_hash_salt = pii_hash_salt

    def _get_pii_salt(self) -> str:
        salt = (
            self._explicit_pii_hash_salt
            if self._explicit_pii_hash_salt is not None
            else os.getenv("PII_HASH_SALT", "")
        )
        cleaned = salt.strip()
        if not cleaned:
            raise ValueError("PII_HASH_SALT must be configured and non-empty to hash natural-person PII.")
        return cleaned

    def _hash_pii_value(self, value: str) -> str:
        salt = self._get_pii_salt()
        return hash_natural_person_pii(value, salt)

    def _mask_natural_person_pii(self, obj: Any) -> Any:
        """
        Recursively applies salted SHA-256 hashing to natural-person PII fields
        (contact emails/names/CPFs) while keeping B2B corporate tax IDs in cleartext.
        """
        if isinstance(obj, dict):
            masked: Dict[str, Any] = {}
            for k, v in obj.items():
                key_lower = str(k).strip().lower()
                if key_lower in _NATURAL_PII_KEYS and isinstance(v, str) and v.strip():
                    masked[k] = self._hash_pii_value(v)
                else:
                    masked[k] = self._mask_natural_person_pii(v)
            return masked
        if isinstance(obj, list):
            return [self._mask_natural_person_pii(item) for item in obj]
        return obj

    def _require_engine(self) -> Any:
        if self.engine is None:
            raise LedgerPersistenceError("PostgreSQL engine is not configured for ACID persistence.")
        return self.engine

    def land_raw_to_mongo(
        self,
        payload: Dict[str, Any],
        payload_hash: str,
        collection_name: Optional[str] = None,
    ) -> str:
        """
        Persists raw BSON payload to MongoDB Raw Landing Zone indexed by payload_hash.
        Raises MongoLandingError explicitly if MongoDB is unavailable or write fails.
        """
        target_collection = collection_name or self.mongo_collection
        sanitized_payload = self._mask_natural_person_pii(deepcopy(payload))
        try:
            col = get_raw_collection(target_collection)
            col.create_index([("payload_hash", 1)], unique=True)
            doc = {
                "payload_hash": payload_hash,
                "source_system": self.source_system,
                "ingested_at": datetime.now(timezone.utc),
                "raw_payload": sanitized_payload,
            }
            col.update_one(
                {"payload_hash": payload_hash},
                {"$setOnInsert": doc},
                upsert=True,
            )
            stored = col.find_one({"payload_hash": payload_hash})
            if not stored or "_id" not in stored:
                raise MongoLandingError("MongoDB raw landing succeeded without persisted document _id.")
            return str(stored["_id"])
        except MongoLandingError:
            raise
        except PyMongoError as exc:
            logger.error("MongoDB raw landing failed (collection=%s): %s", target_collection, type(exc).__name__)
            raise MongoLandingError("MongoDB raw landing failed.") from exc
        except Exception as exc:
            logger.error("Unexpected MongoDB landing failure (collection=%s): %s", target_collection, type(exc).__name__)
            raise MongoLandingError("MongoDB raw landing failed.") from exc

    def run_self_correction_loop(
        self,
        payload: Dict[str, Any],
        max_attempts: int = 3,
    ) -> Tuple[Optional[CommercialInvoiceInput], List[Dict[str, Any]], int]:
        """
        Autonomous Agentic Self-Correction Loop:
        Attempts Pydantic V2 validation. On failure, analyzes error feedback and applies
        strictly non-corruptive deterministic corrections (float-to-string conversion,
        whitespace stripping, case normalizations, legacy line_total alias) for up to max_attempts.
        NEVER coerces arithmetic discrepancies or ambiguous comma-formatted numbers like '1,250'.
        """
        current_payload = deepcopy(payload)
        last_errors: List[Dict[str, Any]] = []

        # Attempt 0: Direct validation
        try:
            validated = CommercialInvoiceInput.model_validate(current_payload)
            return validated, [], 0
        except ValidationError as err:
            last_errors = _sanitize_validation_errors(err.errors())

        attempt = 0
        monetary_fields = {
            "subtotal_amount",
            "tax_amount",
            "freight_amount",
            "insurance_amount",
            "total_amount",
            "extraction_confidence",
        }
        string_fields = {"issuer_tax_id", "document_number", "incoterm", "currency_code"}

        for attempt in range(1, max_attempts + 1):
            modified = False

            for error in last_errors:
                loc = error.get("loc", ())
                if not loc:
                    continue

                field_name = loc[0]

                # Case 1: Prohibited finite int/float on monetary/confidence fields -> stringify;
                # strings only strip whitespace (never replace ',' with '.'!).
                if field_name in monetary_fields and field_name in current_payload:
                    val = current_payload[field_name]
                    if isinstance(val, (float, int)) and not isinstance(val, bool):
                        if isinstance(val, float) and not math.isfinite(val):
                            continue
                        current_payload[field_name] = f"{Decimal(str(val)):.4f}"
                        modified = True
                    elif isinstance(val, str):
                        stripped = val.strip()
                        if stripped != val:
                            current_payload[field_name] = stripped
                            modified = True

                # Case 2: String fields with leading/trailing whitespace or lowercase codes
                if field_name in string_fields and field_name in current_payload:
                    val = current_payload[field_name]
                    if isinstance(val, str):
                        cleaned = val.strip()
                        if field_name in {"incoterm", "currency_code"}:
                            cleaned = cleaned.upper()
                        if cleaned != val:
                            current_payload[field_name] = cleaned
                            modified = True

                # Case 3: Line items float/whitespace correction & legacy alias
                if field_name == "lines" and len(loc) >= 3:
                    idx = loc[1]
                    subfield = loc[2]
                    lines_list = current_payload.get("lines")
                    if isinstance(lines_list, list) and isinstance(idx, int) and 0 <= idx < len(lines_list):
                        line = lines_list[idx]
                        if not isinstance(line, dict):
                            continue
                        if subfield == "total_price" and "line_total" in line and "total_price" not in line:
                            line["total_price"] = line.pop("line_total")
                            modified = True
                        if subfield in {"quantity", "unit_price", "total_price"}:
                            lval = line.get(subfield)
                            if isinstance(lval, (float, int)) and not isinstance(lval, bool):
                                if isinstance(lval, float) and not math.isfinite(lval):
                                    continue
                                line[subfield] = f"{Decimal(str(lval)):.4f}"
                                modified = True
                            elif isinstance(lval, str):
                                lstripped = lval.strip()
                                if lstripped != lval:
                                    line[subfield] = lstripped
                                    modified = True
                        elif subfield in {"ncm_code", "description"}:
                            lval = line.get(subfield)
                            if isinstance(lval, str):
                                lstripped = lval.strip()
                                if lstripped != lval:
                                    line[subfield] = lstripped
                                    modified = True

            if not modified:
                return None, last_errors, attempt

            try:
                validated = CommercialInvoiceInput.model_validate(current_payload)
                return validated, [], attempt
            except ValidationError as err:
                last_errors = _sanitize_validation_errors(err.errors())

        return None, last_errors, attempt

    def _resolve_competence_ptax_rate(
        self,
        conn: Any,
        currency_code: str,
        competence_date: date,
    ) -> Decimal:
        """
        Looks up competence-date PTAX selling rate in dim_currency_ptax using
        LOCF without lookahead bias (date_id <= competence_date_id ORDER BY date_id DESC LIMIT 1).
        Returns Decimal('1.000000') only for BRL; raises MissingPTAXRateError if non-BRL rate is missing.
        """
        curr = currency_code.upper().strip()
        if curr == "BRL":
            return Decimal("1.000000")

        competence_date_id = (
            competence_date.year * 10000
            + competence_date.month * 100
            + competence_date.day
        )
        rate_val = conn.execute(
            text(
                """
                SELECT ptax_sell_rate
                FROM dim_currency_ptax
                WHERE currency_code = :currency_code
                  AND date_id <= :competence_date_id
                ORDER BY date_id DESC
                LIMIT 1
                """
            ),
            {
                "currency_code": curr,
                "competence_date_id": competence_date_id,
            },
        ).scalar_one_or_none()

        if rate_val is None:
            raise MissingPTAXRateError(
                f"Missing PTAX rate in dim_currency_ptax for currency '{curr}' "
                f"on or before {competence_date.isoformat()} (date_id={competence_date_id})."
            )

        return Decimal(str(rate_val)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)

    def _upsert_idempotency_registry(
        self,
        conn: Any,
        payload_hash: str,
        target_entity_id: Optional[str],
        reg_status: str,
        *,
        overwrite_on_conflict: bool = False,
    ) -> None:
        if overwrite_on_conflict:
            sql = text(
                """
                INSERT INTO ingestion_idempotency_registry (
                    payload_hash, source_system, target_entity_id, status, processed_at
                )
                VALUES (:hash, :source_system, :target_id, :status, CURRENT_TIMESTAMP)
                ON CONFLICT (payload_hash) DO UPDATE SET
                    target_entity_id = EXCLUDED.target_entity_id,
                    status = EXCLUDED.status,
                    processed_at = CURRENT_TIMESTAMP
                """
            )
        else:
            sql = text(
                """
                INSERT INTO ingestion_idempotency_registry (
                    payload_hash, source_system, target_entity_id, status, processed_at
                )
                VALUES (:hash, :source_system, :target_id, :status, CURRENT_TIMESTAMP)
                ON CONFLICT (payload_hash) DO NOTHING
                """
            )

        conn.execute(
            sql,
            {
                "hash": payload_hash,
                "source_system": self.source_system,
                "target_id": target_entity_id,
                "status": reg_status,
            },
        )

    def _upsert_commercial_invoice(
        self,
        conn: Any,
        inv: CommercialInvoiceInput,
        payload_hash: str,
        mongo_raw_id: Optional[str],
    ) -> UUID:
        natural_key = (
            f"{inv.issuer_tax_id}|{inv.document_number}|{inv.issue_date.isoformat()}|"
            f"{inv.currency_code}|{inv.total_amount}"
        )
        invoice_id = inv.invoice_id or uuid5(COMEX_UUID_NAMESPACE, f"invoice:{natural_key}")

        persisted_invoice_id = conn.execute(
            text(
                """
                INSERT INTO commercial_invoices (
                    invoice_id, issuer_tax_id, document_number, issue_date, currency_code,
                    subtotal_amount, tax_amount, freight_amount, insurance_amount, total_amount,
                    incoterm, extraction_confidence, raw_payload_hash, mongo_raw_id,
                    created_at, updated_at
                )
                VALUES (
                    :invoice_id, :issuer_tax_id, :document_number, :issue_date, :currency_code,
                    :subtotal_amount, :tax_amount, :freight_amount, :insurance_amount, :total_amount,
                    :incoterm, :extraction_confidence, :raw_payload_hash, :mongo_raw_id,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                ON CONFLICT (issuer_tax_id, document_number, issue_date, currency_code, total_amount)
                DO UPDATE SET
                    extraction_confidence = GREATEST(
                        commercial_invoices.extraction_confidence,
                        EXCLUDED.extraction_confidence
                    ),
                    mongo_raw_id = COALESCE(EXCLUDED.mongo_raw_id, commercial_invoices.mongo_raw_id),
                    raw_payload_hash = EXCLUDED.raw_payload_hash,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING invoice_id
                """
            ),
            {
                "invoice_id": invoice_id,
                "issuer_tax_id": inv.issuer_tax_id,
                "document_number": inv.document_number,
                "issue_date": inv.issue_date,
                "currency_code": inv.currency_code,
                "subtotal_amount": inv.subtotal_amount,
                "tax_amount": inv.tax_amount,
                "freight_amount": inv.freight_amount,
                "insurance_amount": inv.insurance_amount,
                "total_amount": inv.total_amount,
                "incoterm": inv.incoterm,
                "extraction_confidence": inv.extraction_confidence,
                "raw_payload_hash": payload_hash,
                "mongo_raw_id": mongo_raw_id,
            },
        ).scalar_one()

        if inv.lines:
            line_sql = text(
                """
                INSERT INTO commercial_invoice_lines (
                    line_id, invoice_id, line_number, ncm_code, description,
                    quantity, unit_price, total_price, created_at
                )
                VALUES (
                    :line_id, :invoice_id, :line_number, :ncm_code, :description,
                    :quantity, :unit_price, :total_price, CURRENT_TIMESTAMP
                )
                ON CONFLICT (invoice_id, line_number) DO NOTHING
                """
            )
            for line in inv.lines:
                line_id = uuid5(
                    COMEX_UUID_NAMESPACE,
                    f"invoice_line:{persisted_invoice_id}:{line.line_number}",
                )
                conn.execute(
                    line_sql,
                    {
                        "line_id": line_id,
                        "invoice_id": persisted_invoice_id,
                        "line_number": line.line_number,
                        "ncm_code": line.ncm_code,
                        "description": line.description,
                        "quantity": line.quantity,
                        "unit_price": line.unit_price,
                        "total_price": line.total_price,
                    },
                )

        return persisted_invoice_id

    def _insert_journal_entry_with_lines(
        self,
        conn: Any,
        entry_id: UUID,
        tx_date: date,
        comp_date: date,
        ref_doc: Optional[str],
        description: str,
        lines: List[Dict[str, Any]],
    ) -> UUID:
        existing_entry = conn.execute(
            text("SELECT entry_id FROM journal_entries WHERE entry_id = :entry_id"),
            {"entry_id": entry_id},
        ).scalar_one_or_none()
        if existing_entry is not None:
            return existing_entry

        conn.execute(
            text(
                """
                INSERT INTO journal_entries (
                    entry_id, transaction_date, competence_date, reference_document_id,
                    description, status, created_at
                )
                VALUES (
                    :entry_id, :tx_date, :comp_date, :ref_doc, :desc, 'POSTED', CURRENT_TIMESTAMP
                )
                ON CONFLICT (entry_id) DO NOTHING
                """
            ),
            {
                "entry_id": entry_id,
                "tx_date": tx_date,
                "comp_date": comp_date,
                "ref_doc": ref_doc,
                "desc": description,
            },
        )

        insert_line_sql = text(
            """
            INSERT INTO journal_lines (
                line_id, entry_id, account_code, debit_amount, credit_amount,
                currency_code, original_currency_amount, exchange_rate_applied,
                line_order, created_at
            )
            VALUES (
                :line_id, :entry_id, :account_code, :debit, :credit,
                :currency, :orig_amount, :fx_rate, :order, CURRENT_TIMESTAMP
            )
            ON CONFLICT (line_id) DO NOTHING
            """
        )
        for line in lines:
            conn.execute(insert_line_sql, line)

        return entry_id

    def _post_invoice_ledger_entry_if_missing(
        self,
        conn: Any,
        invoice_id: UUID,
        inv: CommercialInvoiceInput,
        description_prefix: str,
    ) -> Tuple[Optional[UUID], Decimal]:
        fx_rate = self._resolve_competence_ptax_rate(
            conn=conn,
            currency_code=inv.currency_code,
            competence_date=inv.issue_date,
        )

        if inv.total_amount <= Decimal("0.0000"):
            return None, fx_rate

        total_functional = (inv.total_amount * fx_rate).quantize(
            Decimal("0.0001"), rounding=ROUND_HALF_UP
        )
        if total_functional <= Decimal("0.0000"):
            raise ValueError(
                f"Converted functional amount for invoice {inv.document_number} must be > 0.0000."
            )

        entry_id = uuid5(COMEX_UUID_NAMESPACE, f"invoice_ledger:{invoice_id}")
        lines = [
            {
                "line_id": uuid5(COMEX_UUID_NAMESPACE, f"invoice_line_je:{entry_id}:1"),
                "entry_id": entry_id,
                "account_code": "1.1.3.01",
                "debit": total_functional,
                "credit": Decimal("0.0000"),
                "currency": inv.currency_code,
                "orig_amount": inv.total_amount,
                "fx_rate": fx_rate,
                "order": 1,
            },
            {
                "line_id": uuid5(COMEX_UUID_NAMESPACE, f"invoice_line_je:{entry_id}:2"),
                "entry_id": entry_id,
                "account_code": "2.1.1.01",
                "debit": Decimal("0.0000"),
                "credit": total_functional,
                "currency": inv.currency_code,
                "orig_amount": inv.total_amount,
                "fx_rate": fx_rate,
                "order": 2,
            },
        ]
        persisted_entry_id = self._insert_journal_entry_with_lines(
            conn=conn,
            entry_id=entry_id,
            tx_date=inv.issue_date,
            comp_date=inv.issue_date,
            ref_doc=inv.document_number,
            description=f"{description_prefix}: {inv.document_number} - {inv.issuer_tax_id}",
            lines=lines,
        )
        return persisted_entry_id, fx_rate

    def route_to_quarantine(
        self,
        payload_hash: str,
        mongo_raw_id: Optional[str],
        errors: Any,
        attempts: int,
        conn: Optional[Any] = None,
        source_type: str = "INVOICE",
    ) -> str:
        """
        Routes persistently invalid payloads to comex_quarantine_hitl and registers
        status='QUARANTINED' in ingestion_idempotency_registry idempotently.
        """
        sanitized_errors = _sanitize_validation_errors(errors)
        errors_json = json.dumps(sanitized_errors, default=str)

        def _tx(c: Any) -> str:
            existing_reg = c.execute(
                text(
                    """
                    SELECT target_entity_id, status
                    FROM ingestion_idempotency_registry
                    WHERE payload_hash = :payload_hash
                    FOR UPDATE
                    """
                ),
                {"payload_hash": payload_hash},
            ).mappings().one_or_none()
            if existing_reg is not None and existing_reg.get("target_entity_id"):
                return str(existing_reg["target_entity_id"])

            existing_quar = c.execute(
                text(
                    """
                    SELECT quarantine_id
                    FROM comex_quarantine_hitl
                    WHERE raw_payload_hash = :payload_hash
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"payload_hash": payload_hash},
            ).scalar_one_or_none()
            if existing_quar is not None:
                qid_existing = str(existing_quar)
                self._upsert_idempotency_registry(c, payload_hash, qid_existing, "QUARANTINED")
                return qid_existing

            quarantine_id = uuid5(
                COMEX_UUID_NAMESPACE,
                f"quarantine:{source_type}:{payload_hash}",
            )
            qid_str = str(quarantine_id)

            c.execute(
                text(
                    """
                    INSERT INTO comex_quarantine_hitl (
                        quarantine_id, source_type, raw_payload_hash, mongo_raw_id,
                        validation_errors, self_correction_attempts, review_status, created_at
                    )
                    VALUES (
                        :quarantine_id, :source_type, :payload_hash, :mongo_raw_id,
                        CAST(:errors AS jsonb), :attempts, 'PENDING_REVIEW', CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (quarantine_id) DO NOTHING
                    """
                ),
                {
                    "quarantine_id": quarantine_id,
                    "source_type": source_type,
                    "payload_hash": payload_hash,
                    "mongo_raw_id": mongo_raw_id,
                    "errors": errors_json,
                    "attempts": attempts,
                },
            )
            self._upsert_idempotency_registry(c, payload_hash, qid_str, "QUARANTINED")
            return qid_str

        try:
            if conn is not None:
                return _tx(conn)
            eng = self._require_engine()
            with eng.begin() as local_conn:
                return _tx(local_conn)
        except LedgerPersistenceError:
            raise
        except SQLAlchemyError as exc:
            logger.error("Failed to persist quarantine record in PostgreSQL: %s", type(exc).__name__)
            raise LedgerPersistenceError("Failed to persist quarantine record in PostgreSQL.") from exc

    # Alias for method name compatibility
    deflect_to_quarantine = route_to_quarantine

    def get_quarantine_items_page(
        self,
        status_filter: Optional[str] = "PENDING_REVIEW",
        limit: int = 50,
        offset: int = 0,
        conn: Optional[Any] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Retrieves paginated quarantine items filtered by review_status (default: 'PENDING_REVIEW')
        ordered by created_at DESC, returning (items, total_count).
        """
        normalized_filter: Optional[str] = None
        if status_filter is not None:
            normalized_filter = _normalize_review_status_token(status_filter)
            allowed = VALID_QUARANTINE_REVIEW_STATUSES | {"ALL"}
            if normalized_filter not in allowed:
                raise ValueError(
                    f"Invalid review_status '{status_filter}'. Permitted values: {sorted(allowed)}"
                )

        where_clause = ""
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if normalized_filter and normalized_filter != "ALL":
            where_clause = " WHERE review_status = :status_filter"
            params["status_filter"] = normalized_filter

        count_sql = f"SELECT COUNT(*) FROM comex_quarantine_hitl{where_clause}"
        query_sql = (
            """
            SELECT quarantine_id, source_type, raw_payload_hash, mongo_raw_id,
                   validation_errors, self_correction_attempts, review_status,
                   reviewed_by, reviewed_at, created_at
            FROM comex_quarantine_hitl
            """
            + where_clause
            + " ORDER BY created_at DESC, quarantine_id ASC LIMIT :limit OFFSET :offset"
        )

        def _fetch(c: Any) -> Tuple[List[Dict[str, Any]], int]:
            total = int(c.execute(text(count_sql), params).scalar() or 0)
            result = c.execute(text(query_sql), params)
            rows: List[Dict[str, Any]] = []
            for row in result.mappings():
                row_dict = dict(row)
                row_dict["quarantine_id"] = str(row_dict["quarantine_id"])
                if row_dict.get("reviewed_at") is not None and hasattr(row_dict["reviewed_at"], "isoformat"):
                    row_dict["reviewed_at"] = row_dict["reviewed_at"].isoformat()
                if row_dict.get("created_at") is not None and hasattr(row_dict["created_at"], "isoformat"):
                    row_dict["created_at"] = row_dict["created_at"].isoformat()
                rows.append(row_dict)
            return rows, total

        try:
            if conn is not None:
                return _fetch(conn)
            eng = self._require_engine()
            with eng.connect() as local_conn:
                return _fetch(local_conn)
        except LedgerPersistenceError:
            raise
        except SQLAlchemyError as exc:
            logger.error("Database query on comex_quarantine_hitl failed: %s", type(exc).__name__)
            raise LedgerPersistenceError("Failed to query quarantine records from PostgreSQL.") from exc

    def get_pending_quarantine_items(
        self,
        status_filter: Optional[str] = "PENDING_REVIEW",
        limit: int = 50,
        offset: int = 0,
        conn: Optional[Any] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieves quarantine items filtered by status_filter (default: 'PENDING_REVIEW')
        ordered by created_at DESC with limit/offset pagination.
        """
        items, _ = self.get_quarantine_items_page(
            status_filter=status_filter,
            limit=limit,
            offset=offset,
            conn=conn,
        )
        return items

    def resolve_quarantined_invoice(
        self,
        quarantine_id: UUID | str,
        resolution_action: str,
        reviewed_by: str,
        corrected_payload: Optional[Dict[str, Any]] = None,
        conn: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Atomically resolves a quarantined commercial invoice using SELECT ... FOR UPDATE
        and WHERE review_status = 'PENDING_REVIEW' in a single ACID transaction:
        - 'REJECTED_HITL' (or alias 'REJECTED'): persists review_status='REJECTED_HITL' and
          updates edge idempotency registry to 'REJECTED_HITL'.
        - 'APPROVED_OVERRIDE': validates corrected_payload against CommercialInvoiceInput,
          upserts commercial_invoices, resolves competence-date PTAX, posts balanced double-entry
          in BRL, updates comex_quarantine_hitl to 'APPROVED_OVERRIDE', and updates registry to
          'RESOLVED_OVERRIDE'.
        Hashes natural-person PII in reviewed_by using hash_natural_person_pii + PII_HASH_SALT.
        """
        try:
            qid_uuid = quarantine_id if isinstance(quarantine_id, UUID) else UUID(str(quarantine_id))
        except (ValueError, TypeError) as exc:
            raise ValueError("quarantine_id must be a valid UUID.") from exc

        normalized_action = _normalize_review_status_token(str(resolution_action or ""))
        if normalized_action not in ("APPROVED_OVERRIDE", "REJECTED_HITL"):
            raise ValueError(
                f"Invalid resolution_action '{resolution_action}'. "
                "Must be 'APPROVED_OVERRIDE' or 'REJECTED_HITL' (or alias 'REJECTED')."
            )

        if not isinstance(reviewed_by, str) or not reviewed_by.strip():
            raise ValueError("reviewed_by must be a non-empty string identifier.")

        hashed_reviewed_by = self._hash_pii_value(reviewed_by)
        if not hashed_reviewed_by or len(hashed_reviewed_by) > 100:
            raise ValueError("Hashed reviewed_by must be non-blank and <= 100 characters.")

        validated: Optional[CommercialInvoiceInput] = None
        corrected_hash: Optional[str] = None
        if normalized_action == "APPROVED_OVERRIDE":
            if corrected_payload is None:
                raise ValueError("corrected_payload is required for APPROVED_OVERRIDE resolution")
            validated = CommercialInvoiceInput.model_validate(corrected_payload)
            corrected_hash = compute_payload_hash(corrected_payload)

        qid_str = str(qid_uuid)

        def _resolve_tx(c: Any) -> Dict[str, Any]:
            row = c.execute(
                text(
                    """
                    SELECT quarantine_id, source_type, raw_payload_hash, mongo_raw_id,
                           validation_errors, self_correction_attempts, review_status,
                           reviewed_by, reviewed_at, created_at
                    FROM comex_quarantine_hitl
                    WHERE quarantine_id = :qid
                    FOR UPDATE
                    """
                ),
                {"qid": qid_uuid},
            ).mappings().one_or_none()

            if row is None:
                raise QuarantineNotFoundError(f"Quarantine record {qid_str} not found")

            current_status = row["review_status"]
            if current_status != "PENDING_REVIEW":
                raise QuarantineStateConflictError(
                    f"Quarantine record {qid_str} is already finalized with status {current_status}"
                )

            orig_hash = row.get("raw_payload_hash")

            if normalized_action == "REJECTED_HITL":
                updated_qid = c.execute(
                    text(
                        """
                        UPDATE comex_quarantine_hitl
                        SET review_status = 'REJECTED_HITL',
                            reviewed_by = :reviewed_by,
                            reviewed_at = CURRENT_TIMESTAMP
                        WHERE quarantine_id = :qid
                          AND review_status = 'PENDING_REVIEW'
                        RETURNING quarantine_id
                        """
                    ),
                    {"reviewed_by": hashed_reviewed_by, "qid": qid_uuid},
                ).scalar_one_or_none()
                if updated_qid is None:
                    raise QuarantineStateConflictError(
                        f"Quarantine record {qid_str} is already finalized"
                    )

                if orig_hash:
                    self._upsert_idempotency_registry(
                        c,
                        orig_hash,
                        qid_str,
                        "REJECTED_HITL",
                        overwrite_on_conflict=True,
                    )

                return {
                    "quarantine_id": qid_str,
                    "review_status": "REJECTED_HITL",
                    "reviewed_by": hashed_reviewed_by,
                    "invoice_id": None,
                    "journal_entry_id": None,
                }

            assert validated is not None and corrected_hash is not None
            mongo_raw_id = row.get("mongo_raw_id") or validated.mongo_raw_id
            validated.raw_payload_hash = corrected_hash
            validated.mongo_raw_id = mongo_raw_id

            persisted_inv_id = self._upsert_commercial_invoice(
                conn=c,
                inv=validated,
                payload_hash=corrected_hash,
                mongo_raw_id=mongo_raw_id,
            )
            persisted_entry_id, fx_rate = self._post_invoice_ledger_entry_if_missing(
                conn=c,
                invoice_id=persisted_inv_id,
                inv=validated,
                description_prefix="HITL Override Ingestion",
            )

            updated_qid = c.execute(
                text(
                    """
                    UPDATE comex_quarantine_hitl
                    SET review_status = 'APPROVED_OVERRIDE',
                        reviewed_by = :reviewed_by,
                        reviewed_at = CURRENT_TIMESTAMP
                    WHERE quarantine_id = :qid
                      AND review_status = 'PENDING_REVIEW'
                    RETURNING quarantine_id
                    """
                ),
                {"reviewed_by": hashed_reviewed_by, "qid": qid_uuid},
            ).scalar_one_or_none()
            if updated_qid is None:
                raise QuarantineStateConflictError(
                    f"Quarantine record {qid_str} is already finalized"
                )

            if orig_hash:
                self._upsert_idempotency_registry(
                    c,
                    orig_hash,
                    str(persisted_inv_id),
                    "RESOLVED_OVERRIDE",
                    overwrite_on_conflict=True,
                )
            if corrected_hash != orig_hash:
                self._upsert_idempotency_registry(
                    c,
                    corrected_hash,
                    str(persisted_inv_id),
                    "RESOLVED_OVERRIDE",
                    overwrite_on_conflict=True,
                )

            return {
                "quarantine_id": qid_str,
                "review_status": "APPROVED_OVERRIDE",
                "reviewed_by": hashed_reviewed_by,
                "invoice_id": str(persisted_inv_id),
                "document_number": validated.document_number,
                "journal_entry_id": str(persisted_entry_id) if persisted_entry_id else None,
                "fx_rate_applied": str(fx_rate),
            }

        try:
            if conn is not None:
                return _resolve_tx(conn)
            eng = self._require_engine()
            with eng.begin() as local_conn:
                return _resolve_tx(local_conn)
        except (
            QuarantineNotFoundError,
            QuarantineStateConflictError,
            MissingPTAXRateError,
            ValidationError,
            ValueError,
            LedgerPersistenceError,
        ):
            raise
        except SQLAlchemyError as exc:
            logger.error("Database ACID resolution failed for quarantine_id=%s: %s", qid_str, type(exc).__name__)
            raise LedgerPersistenceError("Database ACID quarantine resolution failed.") from exc

    def _lookup_existing_idempotency(
        self,
        conn: Any,
        payload_hash: str,
        mongo_raw_id: str,
    ) -> Optional[Dict[str, Any]]:
        reg_row = conn.execute(
            text(
                """
                SELECT target_entity_id, status
                FROM ingestion_idempotency_registry
                WHERE payload_hash = :hash
                FOR UPDATE
                """
            ),
            {"hash": payload_hash},
        ).mappings().one_or_none()

        if reg_row is None:
            return None

        reg_status = str(reg_row["status"])
        target_id = reg_row.get("target_entity_id")

        if reg_status in ("PROCESSED", "RESOLVED_OVERRIDE"):
            response: Dict[str, Any] = {
                "status": "DUPLICATE_IGNORED",
                "idempotency_status": reg_status,
                "invoice_id": str(target_id) if target_id is not None else None,
                "mongo_raw_id": mongo_raw_id,
                "payload_hash": payload_hash,
            }
            if target_id:
                try:
                    inv_uuid = UUID(str(target_id))
                    inv_row = conn.execute(
                        text(
                            """
                            SELECT issuer_tax_id, document_number, total_amount, currency_code
                            FROM commercial_invoices
                            WHERE invoice_id = :invoice_id
                            """
                        ),
                        {"invoice_id": inv_uuid},
                    ).mappings().one_or_none()
                    if inv_row is not None:
                        response["issuer_tax_id"] = inv_row["issuer_tax_id"]
                        response["document_number"] = inv_row["document_number"]
                        response["total_amount"] = str(inv_row["total_amount"])
                        response["currency_code"] = inv_row["currency_code"]
                except (ValueError, TypeError):
                    pass
            return response

        if reg_status in ("QUARANTINED", "REJECTED_HITL"):
            review_status = "PENDING_REVIEW" if reg_status == "QUARANTINED" else "REJECTED_HITL"
            existing_errors: List[Dict[str, Any]] = []
            if target_id:
                try:
                    q_uuid = UUID(str(target_id))
                    q_row = conn.execute(
                        text(
                            """
                            SELECT review_status, validation_errors
                            FROM comex_quarantine_hitl
                            WHERE quarantine_id = :qid
                            """
                        ),
                        {"qid": q_uuid},
                    ).mappings().one_or_none()
                    if q_row is not None:
                        review_status = str(q_row["review_status"])
                        existing_errors = _sanitize_validation_errors(q_row["validation_errors"])
                except (ValueError, TypeError):
                    pass
            return {
                "status": "DUPLICATE_IGNORED",
                "idempotency_status": reg_status,
                "review_status": review_status,
                "quarantine_id": str(target_id) if target_id is not None else None,
                "mongo_raw_id": mongo_raw_id,
                "payload_hash": payload_hash,
                "errors": existing_errors,
            }

        return {
            "status": "DUPLICATE_IGNORED",
            "idempotency_status": reg_status,
            "target_entity_id": str(target_id) if target_id is not None else None,
            "mongo_raw_id": mongo_raw_id,
            "payload_hash": payload_hash,
        }

    def process_invoice(
        self,
        payload: Dict[str, Any],
        conn: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Executes end-to-end invoice ingestion matching ODCS v3.2.0:
        1. Mongo Raw Landing (fail-loud on MongoDB unavailability)
        2. Edge SHA-256 Idempotency Registry Check (before self-correction & quarantine)
        3. Agentic Self-Correction Loop (up to 3 iterations)
        4. Quarantine HITL deflection on persistent failure
        5. PostgreSQL ACID Atomic Upsert on commercial_invoices + invoice lines
        6. Append-Only Double-Entry Fiduciary Posting using competence-date PTAX
        """
        payload_hash = compute_payload_hash(payload)

        # 1. Barreira 1: Mongo Raw Landing (never swallowed)
        mongo_raw_id = self.land_raw_to_mongo(payload, payload_hash)

        def _process_tx(c: Any) -> Dict[str, Any]:
            # 2. Check ingestion_idempotency_registry BEFORE self-correction/quarantine
            existing_response = self._lookup_existing_idempotency(c, payload_hash, mongo_raw_id)
            if existing_response is not None:
                return existing_response

            # 3. Shift-Left Validation with Agentic Self-Correction
            validated, errors, attempts = self.run_self_correction_loop(payload, max_attempts=3)

            if validated is None:
                quarantine_id = self.route_to_quarantine(
                    payload_hash=payload_hash,
                    mongo_raw_id=mongo_raw_id,
                    errors=errors,
                    attempts=attempts,
                    conn=c,
                )
                return {
                    "status": "QUARANTINED",
                    "quarantine_id": quarantine_id,
                    "mongo_raw_id": mongo_raw_id,
                    "payload_hash": payload_hash,
                    "self_correction_attempts": attempts,
                    "errors": errors,
                }

            # 4. Barreira 2: Idempotência & Upsert ACID
            validated.raw_payload_hash = payload_hash
            validated.mongo_raw_id = mongo_raw_id
            result = self._execute_acid_posting(validated, payload_hash, mongo_raw_id, c)
            result["self_correction_attempts"] = attempts
            return result

        try:
            if conn is not None:
                return _process_tx(conn)
            eng = self._require_engine()
            with eng.begin() as local_conn:
                return _process_tx(local_conn)
        except (MissingPTAXRateError, ValidationError, ValueError, LedgerPersistenceError):
            raise
        except SQLAlchemyError as exc:
            logger.error("ACID invoice posting failed: %s", type(exc).__name__)
            raise LedgerPersistenceError("PostgreSQL ACID invoice persistence failed.") from exc

    # Alias for method name compatibility
    process_invoice_payload = process_invoice

    def _execute_acid_posting(
        self,
        inv: CommercialInvoiceInput,
        payload_hash: str,
        mongo_raw_id: str,
        conn: Any,
    ) -> Dict[str, Any]:
        """Executes transactional idempotency check, invoice upsert, and idempotent double-entry posting."""
        existing_response = self._lookup_existing_idempotency(conn, payload_hash, mongo_raw_id)
        if existing_response is not None:
            return existing_response

        persisted_invoice_id = self._upsert_commercial_invoice(
            conn=conn,
            inv=inv,
            payload_hash=payload_hash,
            mongo_raw_id=mongo_raw_id,
        )

        entry_id, fx_rate = self._post_invoice_ledger_entry_if_missing(
            conn=conn,
            invoice_id=persisted_invoice_id,
            inv=inv,
            description_prefix="Commercial Invoice Ingestion",
        )

        self._upsert_idempotency_registry(
            conn=conn,
            payload_hash=payload_hash,
            target_entity_id=str(persisted_invoice_id),
            reg_status="PROCESSED",
        )

        return {
            "status": "COMMITTED_ACID",
            "invoice_id": str(persisted_invoice_id),
            "journal_entry_id": str(entry_id) if entry_id else None,
            "mongo_raw_id": mongo_raw_id,
            "payload_hash": payload_hash,
            "issuer_tax_id": inv.issuer_tax_id,
            "document_number": inv.document_number,
            "total_amount": str(inv.total_amount),
            "currency_code": inv.currency_code,
            "fx_rate_applied": str(fx_rate),
        }

    def reconcile_swift_settlement(
        self,
        *,
        reference_document_id: str,
        transaction_date: date,
        competence_date: date,
        gross_amount: Decimal,
        fee_amount: Decimal,
        currency_code: str = "USD",
        exchange_rate: Decimal = Decimal("1.000000"),
        cash_account: str = "1.1.1.01",
        fee_account: str = "4.1.2.01",
        ar_account: str = "1.1.2.01",
        raw_payload: Optional[Dict[str, Any]] = None,
        conn: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Atomically persists a SWIFT fee-split reconciliation into MongoDB (raw_swift_statements)
        and PostgreSQL (journal_entries, journal_lines, ingestion_idempotency_registry)
        with deterministic uuid5 and full replay idempotency.
        """
        built_entry = build_swift_fee_split_entry(
            reference_document_id=reference_document_id,
            transaction_date=transaction_date,
            competence_date=competence_date,
            gross_amount=gross_amount,
            fee_amount=fee_amount,
            currency_code=currency_code,
            exchange_rate=exchange_rate,
            cash_account=cash_account,
            fee_account=fee_account,
            ar_account=ar_account,
        )

        gross_q = Decimal(str(gross_amount)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        fee_q = Decimal(str(fee_amount)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        rate_q = Decimal(str(exchange_rate)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
        curr_norm = currency_code.upper().strip()

        canonical_payload = {
            "reference_document_id": reference_document_id.strip(),
            "transaction_date": transaction_date.isoformat(),
            "competence_date": competence_date.isoformat(),
            "gross_amount": str(gross_q),
            "fee_amount": str(fee_q),
            "currency_code": curr_norm,
            "exchange_rate": str(rate_q),
        }
        payload_hash = compute_payload_hash(canonical_payload)
        deterministic_entry_id = uuid5(COMEX_UUID_NAMESPACE, f"swift_ledger:{payload_hash}")

        deterministic_lines = [
            JournalLineInput(
                line_id=uuid5(
                    COMEX_UUID_NAMESPACE,
                    f"swift_line:{deterministic_entry_id}:{line.line_order}",
                ),
                entry_id=deterministic_entry_id,
                account_code=line.account_code,
                debit_amount=line.debit_amount,
                credit_amount=line.credit_amount,
                currency_code=line.currency_code,
                original_currency_amount=line.original_currency_amount,
                exchange_rate_applied=line.exchange_rate_applied,
                line_order=line.line_order,
            )
            for line in built_entry.lines
        ]
        entry = JournalEntryInput(
            entry_id=deterministic_entry_id,
            transaction_date=built_entry.transaction_date,
            competence_date=built_entry.competence_date,
            reference_document_id=built_entry.reference_document_id,
            description=built_entry.description,
            status="POSTED",
            lines=deterministic_lines,
        )

        mongo_doc = raw_payload if raw_payload is not None else canonical_payload
        mongo_raw_id = self.land_raw_to_mongo(
            mongo_doc,
            payload_hash,
            collection_name="raw_swift_statements",
        )

        def _tx(c: Any) -> Dict[str, Any]:
            existing_reg = c.execute(
                text(
                    """
                    SELECT target_entity_id, status
                    FROM ingestion_idempotency_registry
                    WHERE payload_hash = :hash
                    FOR UPDATE
                    """
                ),
                {"hash": payload_hash},
            ).mappings().one_or_none()

            existing_entry_row = c.execute(
                text(
                    """
                    SELECT entry_id, reference_document_id, transaction_date, description
                    FROM journal_entries
                    WHERE entry_id = :entry_id
                    """
                ),
                {"entry_id": deterministic_entry_id},
            ).mappings().one_or_none()

            is_replay = existing_reg is not None or existing_entry_row is not None

            if not is_replay:
                line_dicts = [
                    {
                        "line_id": line.line_id,
                        "entry_id": deterministic_entry_id,
                        "account_code": line.account_code,
                        "debit": line.debit_amount,
                        "credit": line.credit_amount,
                        "currency": line.currency_code,
                        "orig_amount": line.original_currency_amount,
                        "fx_rate": line.exchange_rate_applied,
                        "order": line.line_order,
                    }
                    for line in entry.lines
                ]
                self._insert_journal_entry_with_lines(
                    conn=c,
                    entry_id=deterministic_entry_id,
                    tx_date=entry.transaction_date,
                    comp_date=entry.competence_date,
                    ref_doc=entry.reference_document_id,
                    description=entry.description,
                    lines=line_dicts,
                )
                self._upsert_idempotency_registry(
                    conn=c,
                    payload_hash=payload_hash,
                    target_entity_id=str(deterministic_entry_id),
                    reg_status="PROCESSED",
                )
                return {
                    "status": "RECONCILED",
                    "idempotent_replay": False,
                    "entry_id": str(deterministic_entry_id),
                    "mongo_raw_id": mongo_raw_id,
                    "payload_hash": payload_hash,
                    "reference_document_id": entry.reference_document_id,
                    "transaction_date": str(entry.transaction_date),
                    "description": entry.description,
                    "lines_count": len(entry.lines),
                    "lines": [
                        {
                            "line_order": line.line_order,
                            "account_code": line.account_code,
                            "debit_amount": str(line.debit_amount),
                            "credit_amount": str(line.credit_amount),
                            "currency_code": line.currency_code,
                            "original_currency_amount": str(line.original_currency_amount),
                            "exchange_rate_applied": str(line.exchange_rate_applied),
                        }
                        for line in entry.lines
                    ],
                }

            resolved_entry_id = (
                UUID(str(existing_reg["target_entity_id"]))
                if existing_reg is not None and existing_reg.get("target_entity_id")
                else deterministic_entry_id
            )
            if existing_entry_row is None:
                existing_entry_row = c.execute(
                    text(
                        """
                        SELECT entry_id, reference_document_id, transaction_date, description
                        FROM journal_entries
                        WHERE entry_id = :entry_id
                        """
                    ),
                    {"entry_id": resolved_entry_id},
                ).mappings().one_or_none()

            persisted_lines = c.execute(
                text(
                    """
                    SELECT line_order, account_code, debit_amount, credit_amount,
                           currency_code, original_currency_amount, exchange_rate_applied
                    FROM journal_lines
                    WHERE entry_id = :entry_id
                    ORDER BY line_order ASC
                    """
                ),
                {"entry_id": resolved_entry_id},
            ).mappings().all()

            lines_payload = (
                [
                    {
                        "line_order": int(r["line_order"]),
                        "account_code": r["account_code"],
                        "debit_amount": str(r["debit_amount"]),
                        "credit_amount": str(r["credit_amount"]),
                        "currency_code": r["currency_code"],
                        "original_currency_amount": str(r["original_currency_amount"]),
                        "exchange_rate_applied": str(r["exchange_rate_applied"]),
                    }
                    for r in persisted_lines
                ]
                if persisted_lines
                else [
                    {
                        "line_order": line.line_order,
                        "account_code": line.account_code,
                        "debit_amount": str(line.debit_amount),
                        "credit_amount": str(line.credit_amount),
                        "currency_code": line.currency_code,
                        "original_currency_amount": str(line.original_currency_amount),
                        "exchange_rate_applied": str(line.exchange_rate_applied),
                    }
                    for line in entry.lines
                ]
            )
            if existing_reg is None:
                self._upsert_idempotency_registry(
                    conn=c,
                    payload_hash=payload_hash,
                    target_entity_id=str(resolved_entry_id),
                    reg_status="PROCESSED",
                )

            return {
                "status": "DUPLICATE_IGNORED",
                "idempotent_replay": True,
                "entry_id": str(resolved_entry_id),
                "mongo_raw_id": mongo_raw_id,
                "payload_hash": payload_hash,
                "reference_document_id": (
                    existing_entry_row["reference_document_id"]
                    if existing_entry_row is not None
                    else entry.reference_document_id
                ),
                "transaction_date": (
                    str(existing_entry_row["transaction_date"])
                    if existing_entry_row is not None
                    else str(entry.transaction_date)
                ),
                "description": (
                    existing_entry_row["description"]
                    if existing_entry_row is not None
                    else entry.description
                ),
                "lines_count": len(lines_payload),
                "lines": lines_payload,
            }

        try:
            if conn is not None:
                return _tx(conn)
            eng = self._require_engine()
            with eng.begin() as local_conn:
                return _tx(local_conn)
        except (ValueError, LedgerPersistenceError):
            raise
        except SQLAlchemyError as exc:
            logger.error("ACID SWIFT reconciliation persistence failed: %s", type(exc).__name__)
            raise LedgerPersistenceError("PostgreSQL ACID SWIFT reconciliation failed.") from exc

    def persist_ptax_rates(
        self,
        rates: List[PTAXRateInput],
        conn: Optional[Any] = None,
    ) -> int:
        """
        Persists daily official PTAX rates to PostgreSQL dim_currency_ptax
        and mirrors to DuckDB / Parquet Star Schema. Raises explicitly on write errors.
        """
        if not rates:
            return 0

        sql = text(
            """
            INSERT INTO dim_currency_ptax (
                date_id, currency_code, ptax_buy_rate, ptax_sell_rate,
                is_business_day, is_interpolated_locf, created_at
            )
            VALUES (
                :date_id, :currency_code, :buy, :sell, :biz_day, :locf, CURRENT_TIMESTAMP
            )
            ON CONFLICT (date_id, currency_code)
            DO UPDATE SET
                ptax_buy_rate = EXCLUDED.ptax_buy_rate,
                ptax_sell_rate = EXCLUDED.ptax_sell_rate,
                is_business_day = EXCLUDED.is_business_day,
                is_interpolated_locf = EXCLUDED.is_interpolated_locf
            """
        )

        rows = [
            {
                "date_id": r.date_id,
                "currency_code": r.currency_code,
                "buy": r.ptax_buy_rate,
                "sell": r.ptax_sell_rate,
                "biz_day": r.is_business_day,
                "locf": r.is_interpolated_locf,
            }
            for r in rates
        ]

        try:
            if conn is not None:
                conn.execute(sql, rows)
            elif self.engine is not None:
                with self.engine.begin() as local_conn:
                    local_conn.execute(sql, rows)
        except SQLAlchemyError as exc:
            logger.error("PostgreSQL PTAX persistence failed: %s", type(exc).__name__)
            raise LedgerPersistenceError("PostgreSQL PTAX persistence failed.") from exc

        parquet_records = [
            {
                "year": r.date_id // 10000,
                "month": (r.date_id % 10000) // 100,
                "date_id": r.date_id,
                "currency_code": r.currency_code,
                "ptax_buy_rate": r.ptax_buy_rate,
                "ptax_sell_rate": r.ptax_sell_rate,
                "is_business_day": r.is_business_day,
                "is_interpolated_locf": r.is_interpolated_locf,
            }
            for r in rates
        ]
        try:
            write_records_to_parquet_hive(
                records=parquet_records,
                table_name="dim_currency_ptax",
                partition_cols=["year", "month"],
                schema=PTAX_PARQUET_SCHEMA,
            )
        except Exception as exc:
            logger.error("Parquet PTAX persistence failed: %s", type(exc).__name__)
            raise LakehousePersistenceError("Parquet PTAX persistence failed.") from exc

        return len(rates)

    def persist_comex_operations(
        self,
        operations: List[ComexOperationInput],
        conn: Optional[Any] = None,
    ) -> int:
        """
        Persists customs operations to PostgreSQL fact_comex_operations
        and Apache Parquet Hive partitioned lakehouse with operation_hash_id deduplication.
        Raises explicitly on storage failures.
        """
        if not operations:
            return 0

        sql = text(
            """
            INSERT INTO fact_comex_operations (
                operation_hash_id, date_id, operation_type, ncm_code,
                country_code, uf_code, modal_code, urf_code,
                statistical_quantity, net_weight_kg, fob_amount_usd,
                freight_amount_usd, insurance_amount_usd, cif_amount_usd,
                landed_cost_brl, created_at
            )
            VALUES (
                :hash_id, :date_id, :op_type, :ncm, :country, :uf, :modal, :urf,
                :qty, :weight, :fob, :freight, :insurance, :cif, :landed, CURRENT_TIMESTAMP
            )
            ON CONFLICT (operation_hash_id)
            DO UPDATE SET
                statistical_quantity = EXCLUDED.statistical_quantity,
                net_weight_kg = EXCLUDED.net_weight_kg,
                fob_amount_usd = EXCLUDED.fob_amount_usd,
                freight_amount_usd = EXCLUDED.freight_amount_usd,
                insurance_amount_usd = EXCLUDED.insurance_amount_usd,
                cif_amount_usd = EXCLUDED.cif_amount_usd,
                landed_cost_brl = EXCLUDED.landed_cost_brl
            """
        )

        rows = [
            {
                "hash_id": op.operation_hash_id,
                "date_id": op.date_id,
                "op_type": op.operation_type,
                "ncm": op.ncm_code,
                "country": op.country_code,
                "uf": op.uf_code,
                "modal": op.modal_code,
                "urf": op.urf_code,
                "qty": op.statistical_quantity,
                "weight": op.net_weight_kg,
                "fob": op.fob_amount_usd,
                "freight": op.freight_amount_usd,
                "insurance": op.insurance_amount_usd,
                "cif": op.cif_amount_usd,
                "landed": op.landed_cost_brl,
            }
            for op in operations
        ]

        try:
            if conn is not None:
                conn.execute(sql, rows)
            elif self.engine is not None:
                with self.engine.begin() as local_conn:
                    local_conn.execute(sql, rows)
        except SQLAlchemyError as exc:
            logger.error("PostgreSQL fact_comex_operations persistence failed: %s", type(exc).__name__)
            raise LedgerPersistenceError("PostgreSQL fact_comex_operations persistence failed.") from exc

        parquet_records = [
            {
                "year": op.date_id // 10000,
                "month": (op.date_id % 10000) // 100,
                "operation_hash_id": op.operation_hash_id,
                "date_id": op.date_id,
                "operation_type": op.operation_type,
                "ncm_code": op.ncm_code,
                "country_code": op.country_code,
                "uf_code": op.uf_code,
                "modal_code": op.modal_code,
                "urf_code": op.urf_code,
                "statistical_quantity": op.statistical_quantity,
                "net_weight_kg": op.net_weight_kg,
                "fob_amount_usd": op.fob_amount_usd,
                "freight_amount_usd": op.freight_amount_usd,
                "insurance_amount_usd": op.insurance_amount_usd,
                "cif_amount_usd": op.cif_amount_usd,
                "landed_cost_brl": op.landed_cost_brl,
            }
            for op in operations
        ]
        try:
            write_records_to_parquet_hive(
                records=parquet_records,
                table_name="fact_comex_operations",
                partition_cols=["year", "month"],
                schema=FACT_COMEX_PARQUET_SCHEMA,
            )
        except Exception as exc:
            logger.error("Parquet fact_comex_operations persistence failed: %s", type(exc).__name__)
            raise LakehousePersistenceError("Parquet fact_comex_operations persistence failed.") from exc

        return len(operations)

    def persist_customs_nationalization(
        self,
        calc_input: CustomsNationalizationCalcInput,
        persist_db: bool = True,
        conn: Optional[Any] = None,
    ) -> Tuple[CustomsNationalizationResult, JournalEntryInput]:
        """
        Computes Brazilian customs cascading duties, generates the balanced double-entry
        posting (CPC 16) with deterministic uuid5 IDs, and atomically upserts into PostgreSQL
        journal_entries, journal_lines, and customs_nationalizations without duplicating
        ledger entries on re-execution.
        """
        result = compute_brazilian_landed_cost(calc_input)
        nat_id = uuid5(
            COMEX_UUID_NAMESPACE,
            f"customs_nationalization:{calc_input.declaration_number}",
        )
        entry_id = uuid5(
            COMEX_UUID_NAMESPACE,
            f"customs_ledger:{calc_input.declaration_number}",
        )
        built_entry = build_import_nationalization_ledger_entry(result, entry_id=entry_id)
        deterministic_lines = [
            JournalLineInput(
                line_id=uuid5(
                    COMEX_UUID_NAMESPACE,
                    f"customs_ledger_line:{entry_id}:{line.line_order}",
                ),
                entry_id=entry_id,
                account_code=line.account_code,
                debit_amount=line.debit_amount,
                credit_amount=line.credit_amount,
                currency_code=line.currency_code,
                original_currency_amount=line.original_currency_amount,
                exchange_rate_applied=line.exchange_rate_applied,
                line_order=line.line_order,
            )
            for line in built_entry.lines
        ]
        journal_entry = JournalEntryInput(
            entry_id=entry_id,
            transaction_date=built_entry.transaction_date,
            competence_date=built_entry.competence_date,
            reference_document_id=built_entry.reference_document_id,
            description=built_entry.description,
            status="POSTED",
            lines=deterministic_lines,
        )

        result.nationalization_id = nat_id
        result.journal_entry_id = entry_id

        if not persist_db:
            return result, journal_entry

        def _execute_tx(c: Any) -> Tuple[UUID, UUID]:
            existing = c.execute(
                text(
                    """
                    SELECT nationalization_id, journal_entry_id
                    FROM customs_nationalizations
                    WHERE declaration_number = :declaration_number
                    FOR UPDATE
                    """
                ),
                {"declaration_number": result.declaration_number},
            ).mappings().one_or_none()

            if existing is not None:
                persisted_nat_id = existing["nationalization_id"]
                persisted_entry_id = existing.get("journal_entry_id")
                if persisted_entry_id is None and journal_entry.lines:
                    persisted_entry_id = self._persist_customs_ledger_entry(c, entry_id, journal_entry)
                    c.execute(
                        text(
                            """
                            UPDATE customs_nationalizations
                            SET journal_entry_id = :entry_id,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE nationalization_id = :nat_id
                            """
                        ),
                        {"entry_id": persisted_entry_id, "nat_id": persisted_nat_id},
                    )
                return persisted_nat_id, persisted_entry_id or entry_id

            persisted_entry_id = (
                self._persist_customs_ledger_entry(c, entry_id, journal_entry)
                if journal_entry.lines
                else entry_id
            )

            persisted_nat_id = c.execute(
                text(
                    """
                    INSERT INTO customs_nationalizations (
                        nationalization_id, declaration_number, registration_date, clearance_date,
                        invoice_id, importer_tax_id, tax_regime, currency_code,
                        fob_amount, freight_amount, insurance_amount, cif_amount,
                        ptax_exchange_rate, customs_value_brl,
                        ii_rate, ii_amount_brl,
                        ipi_rate, ipi_amount_brl,
                        pis_rate, pis_amount_brl,
                        cofins_rate, cofins_amount_brl,
                        siscomex_fee_brl, afrmm_brl,
                        icms_rate, icms_base_brl, icms_amount_brl,
                        total_taxes_brl, landed_cost_brl,
                        net_inventory_cost_brl, recoverable_taxes_brl,
                        journal_entry_id, created_at, updated_at
                    )
                    VALUES (
                        :nat_id, :declaration_number, :reg_date, :clear_date,
                        :invoice_id, :importer_tax_id, :tax_regime, :currency_code,
                        :fob, :freight, :insurance, :cif,
                        :ptax, :va_brl,
                        :ii_rate, :ii_brl,
                        :ipi_rate, :ipi_brl,
                        :pis_rate, :pis_brl,
                        :cofins_rate, :cofins_brl,
                        :siscomex, :afrmm,
                        :icms_rate, :icms_base, :icms_brl,
                        :total_taxes, :landed_cost,
                        :net_inv, :rec_taxes,
                        :journal_entry_id, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (declaration_number)
                    DO UPDATE SET
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING nationalization_id
                    """
                ),
                {
                    "nat_id": nat_id,
                    "declaration_number": result.declaration_number,
                    "reg_date": result.registration_date,
                    "clear_date": result.clearance_date,
                    "invoice_id": result.invoice_id,
                    "importer_tax_id": result.importer_tax_id,
                    "tax_regime": result.tax_regime,
                    "currency_code": result.currency_code,
                    "fob": result.fob_amount,
                    "freight": result.freight_amount,
                    "insurance": result.insurance_amount,
                    "cif": result.cif_amount,
                    "ptax": result.ptax_exchange_rate,
                    "va_brl": result.customs_value_brl,
                    "ii_rate": result.ii_rate,
                    "ii_brl": result.ii_amount_brl,
                    "ipi_rate": result.ipi_rate,
                    "ipi_brl": result.ipi_amount_brl,
                    "pis_rate": result.pis_rate,
                    "pis_brl": result.pis_amount_brl,
                    "cofins_rate": result.cofins_rate,
                    "cofins_brl": result.cofins_amount_brl,
                    "siscomex": result.siscomex_fee_brl,
                    "afrmm": result.afrmm_brl,
                    "icms_rate": result.icms_rate,
                    "icms_base": result.icms_base_brl,
                    "icms_brl": result.icms_amount_brl,
                    "total_taxes": result.total_taxes_brl,
                    "landed_cost": result.landed_cost_brl,
                    "net_inv": result.net_inventory_cost_brl,
                    "rec_taxes": result.recoverable_taxes_brl,
                    "journal_entry_id": persisted_entry_id,
                },
            ).scalar_one()

            return persisted_nat_id, persisted_entry_id

        try:
            if conn is not None:
                final_nat_id, final_entry_id = _execute_tx(conn)
            else:
                eng = self._require_engine()
                with eng.begin() as local_conn:
                    final_nat_id, final_entry_id = _execute_tx(local_conn)
        except LedgerPersistenceError:
            raise
        except SQLAlchemyError as exc:
            logger.error("ACID persistence of customs nationalization failed: %s", type(exc).__name__)
            raise LedgerPersistenceError("PostgreSQL customs nationalization persistence failed.") from exc

        result.nationalization_id = final_nat_id
        result.journal_entry_id = final_entry_id
        if final_entry_id != journal_entry.entry_id:
            rebound_lines = [
                line.model_copy(update={"entry_id": final_entry_id})
                for line in journal_entry.lines
            ]
            journal_entry = journal_entry.model_copy(
                update={"entry_id": final_entry_id, "lines": rebound_lines}
            )

        return result, journal_entry

    def _persist_customs_ledger_entry(
        self,
        conn: Any,
        entry_id: UUID,
        journal_entry: JournalEntryInput,
    ) -> UUID:
        line_dicts = [
            {
                "line_id": line.line_id or uuid5(
                    COMEX_UUID_NAMESPACE,
                    f"customs_ledger_line:{entry_id}:{line.line_order}",
                ),
                "entry_id": entry_id,
                "account_code": line.account_code,
                "debit": line.debit_amount,
                "credit": line.credit_amount,
                "currency": line.currency_code,
                "orig_amount": line.original_currency_amount,
                "fx_rate": line.exchange_rate_applied,
                "order": line.line_order,
            }
            for line in journal_entry.lines
        ]
        return self._insert_journal_entry_with_lines(
            conn=conn,
            entry_id=entry_id,
            tx_date=journal_entry.transaction_date,
            comp_date=journal_entry.competence_date,
            ref_doc=journal_entry.reference_document_id,
            description=journal_entry.description,
            lines=line_dicts,
        )
