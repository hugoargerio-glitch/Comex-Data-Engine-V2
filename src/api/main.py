import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Literal, Optional
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field, ValidationError, field_validator

from src.models.canonical import CanonicalRecordInput
from src.models.customs import (
    CustomsNationalizationCalcInput,
    CustomsNationalizationResult,
    build_swift_fee_split_entry,
)
from src.models.invoices import CommercialInvoiceInput
from src.pipeline.comex_processor import ComexHTAPProcessor
from src.pipeline.processor import HTAPProcessor

app = FastAPI(
    title="Comex Fiduciary Engine HTAP API",
    version="2.0.0",
    description="Multi-Model HTAP ingestion edge (MongoDB Raw Landing + PostgreSQL ACID Core + DuckDB Star Schema)"
)


class SwiftReconciliationRequest(BaseModel):
    reference_document_id: str = Field(..., min_length=1)
    transaction_date: date
    competence_date: date
    gross_amount: Decimal
    fee_amount: Decimal
    currency_code: str = Field(default="USD", min_length=3, max_length=3)
    exchange_rate: Decimal = Field(default=Decimal("1.000000"))

    @field_validator("gross_amount", "fee_amount", "exchange_rate", mode="before")
    @classmethod
    def reject_float(cls, v: Any) -> Decimal:
        if isinstance(v, float):
            raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")
        return Decimal(str(v))


class QuarantineResolutionRequest(BaseModel):
    resolution_action: Literal["APPROVED_OVERRIDE", "REJECTED"]
    reviewed_by: str = Field(..., min_length=1, description="Auditor email or identifier resolving the quarantine")
    corrected_payload: Optional[Dict[str, Any]] = Field(default=None, description="Human-corrected payload required for APPROVED_OVERRIDE")


@app.get("/health")
def health_check() -> Dict[str, str]:
    return {"status": "healthy", "architecture": "HTAP (Mongo + Postgres + DuckDB)"}


# ==============================================================================
# Canonical Boilerplate V1 Endpoints (Preserved for compatibility)
# ==============================================================================

@app.post("/api/v1/ingest", status_code=status.HTTP_201_CREATED)
def ingest_document(payload: Dict[str, Any]) -> Dict[str, Any]:
    raw_bytes = json.dumps(payload, sort_keys=True).encode("utf-8")
    payload_hash = hashlib.sha256(raw_bytes).hexdigest()

    # Validação Shift-Left com Pydantic V2
    try:
        validated = CanonicalRecordInput.model_validate(payload)
    except ValidationError as err:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"message": "Contract validation failed", "errors": err.errors()}
        )

    return {
        "status": "ACCEPTED",
        "payload_hash": payload_hash,
        "entity_code": validated.entity_code,
        "net_amount": str(validated.net_amount),
        "received_at": datetime.now(timezone.utc).isoformat()
    }


@app.post("/api/v1/ingest/pipeline", status_code=status.HTTP_201_CREATED)
def ingest_and_persist_pipeline(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Executa o fluxo completo Dual-Store: MongoDB Landing -> Pydantic -> PostgreSQL ACID / Quarantine."""
    processor = HTAPProcessor(source_system="FASTAPI_WEBHOOK")
    return processor.process_payload(payload)


# ==============================================================================
# Comex Fiduciary Engine V2 Endpoints (ODCS v3.2.0)
# ==============================================================================

@app.post("/api/v2/invoices/ingest", status_code=status.HTTP_201_CREATED)
def ingest_comex_invoice(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ingests an international commercial invoice payload into Comex V2 HTAP Engine:
    MongoDB Raw Landing -> Agentic Self-Correction Loop -> PostgreSQL ACID / HITL Quarantine.
    """
    processor = ComexHTAPProcessor(source_system="COMEX_V2_API")
    result = processor.process_invoice(payload)

    if result.get("status") == "QUARANTINED":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "message": "Invoice failed contract validation and was routed to HITL Quarantine.",
                "quarantine_id": result.get("quarantine_id"),
                "mongo_raw_id": result.get("mongo_raw_id"),
                "payload_hash": result.get("payload_hash"),
                "errors": result.get("errors"),
            }
        )

    return result


@app.post("/api/v2/ledger/reconcile-swift", status_code=status.HTTP_200_OK)
def reconcile_swift_settlement(req: SwiftReconciliationRequest) -> Dict[str, Any]:
    """
    Reconciles SWIFT camt.053 wire statement with intermediary fee deduction,
    generating a balanced 3-legged split journal entry in functional currency.
    """
    try:
        entry = build_swift_fee_split_entry(
            reference_document_id=req.reference_document_id,
            transaction_date=req.transaction_date,
            competence_date=req.competence_date,
            gross_amount=req.gross_amount,
            fee_amount=req.fee_amount,
            currency_code=req.currency_code,
            exchange_rate=req.exchange_rate,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"message": f"SWIFT reconciliation failed: {exc}"}
        )

    return {
        "status": "RECONCILED",
        "entry_id": str(entry.entry_id),
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
        ]
    }


@app.post("/api/v2/analytics/landed-cost-sim", status_code=status.HTTP_200_OK)
def simulate_landed_cost(
    calc_input: CustomsNationalizationCalcInput,
    persist: bool = False,
) -> Dict[str, Any]:
    """
    Simulates Brazilian customs cascading tax calculation (II, IPI, PIS/COFINS,
    Siscomex, AFRMM, ICMS 'Cálculo por Dentro') and generates CPC 16 inventory
    segregation entries. If persist=True, atomically commits to the ledger.
    """
    processor = ComexHTAPProcessor(source_system="COMEX_V2_SIMULATOR")
    try:
        result, journal_entry = processor.persist_customs_nationalization(
            calc_input=calc_input,
            persist_db=persist,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"message": f"Customs calculation/persistence failed: {exc}"}
        )

    return {
        "status": "SIMULATED" if not persist else "PERSISTED",
        "calculation": result.model_dump(mode="json"),
        "journal_entry": {
            "entry_id": str(journal_entry.entry_id),
            "reference_document_id": journal_entry.reference_document_id,
            "transaction_date": str(journal_entry.transaction_date),
            "description": journal_entry.description,
            "status": journal_entry.status,
            "lines_count": len(journal_entry.lines),
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
                for line in journal_entry.lines
            ],
        },
    }


# ==============================================================================
# HITL Quarantine Resolution Endpoints (Sprint Real 4)
# ==============================================================================

@app.get("/api/v2/quarantine/pending", status_code=status.HTTP_200_OK)
def get_pending_quarantine_items(status: Optional[str] = "PENDING_REVIEW") -> Dict[str, Any]:
    """
    Returns quarantine items filtered by status (default: 'PENDING_REVIEW')
    awaiting human auditor review.
    """
    processor = ComexHTAPProcessor(source_system="COMEX_V2_API")
    items = processor.get_pending_quarantine_items(status_filter=status)
    return {"count": len(items), "items": items}


@app.post("/api/v2/quarantine/{quarantine_id}/resolve", status_code=status.HTTP_200_OK)
def resolve_quarantined_invoice_endpoint(
    quarantine_id: str,
    req: QuarantineResolutionRequest,
) -> Dict[str, Any]:
    """
    Human-in-the-Loop resolution endpoint for quarantined commercial invoices.
    Actions:
      - APPROVED_OVERRIDE: validates corrected_payload and atomically promotes to commercial_invoices + ledger.
      - REJECTED: marks quarantine record as rejected and updates edge idempotency registry.
    """
    processor = ComexHTAPProcessor(source_system="COMEX_V2_API")
    try:
        result = processor.resolve_quarantined_invoice(
            quarantine_id=quarantine_id,
            resolution_action=req.resolution_action,
            reviewed_by=req.reviewed_by,
            corrected_payload=req.corrected_payload,
        )
        return result
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"message": str(exc)},
        )
    except ValueError as exc:
        err_msg = str(exc)
        if "already finalized" in err_msg:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"message": err_msg},
            )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"message": err_msg},
        )
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"message": "Corrected payload failed contract validation", "errors": exc.errors()},
        )

