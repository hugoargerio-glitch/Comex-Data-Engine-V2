import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict
from fastapi import FastAPI, HTTPException, status
from pydantic import ValidationError

from src.models.canonical import CanonicalRecordInput
from src.pipeline.processor import HTAPProcessor

app = FastAPI(
    title="DE Agentic Boilerplate HTAP API",
    version="1.0.0",
    description="Multi-Model HTAP ingestion edge (MongoDB NoSQL Landing + PostgreSQL ACID Core)"
)

@app.get("/health")
def health_check() -> Dict[str, str]:
    return {"status": "healthy", "architecture": "HTAP (Mongo + Postgres + DuckDB)"}

@app.post("/api/v1/ingest", status_code=status.HTTP_201_CREATED)
def ingest_document(payload: Dict[str, Any]) -> Dict[str, Any]:
    raw_bytes = json.dumps(payload, sort_keys=True).encode("utf-8")
    payload_hash = hashlib.sha256(raw_bytes).hexdigest()

    # 1. Validação Shift-Left com Pydantic V2
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
