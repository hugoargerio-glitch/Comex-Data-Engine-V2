import json
from datetime import datetime, timezone
from typing import Any, Dict

from pydantic import ValidationError
from sqlalchemy import text

from src.connectors.mongo import get_raw_collection
from src.connectors.postgres import engine
from src.extractors.http_client import compute_payload_hash
from src.models.canonical import CanonicalRecordInput


class HTAPProcessor:
    """
    Orquestrador Central Dual-Store HTAP:
    1. Barreira 1 (NoSQL): Landing imutável no MongoDB indexado por SHA-256 (payload_hash).
    2. Validação Shift-Left: Contrato Pydantic V2 estrito; falhas são roteadas para quarantine_records (HITL).
    3. Barreira 2 (ACID): Registro em idempotency_registry + Upsert transacional em canonical_records.
    """

    def __init__(
        self,
        source_system: str = "CANONICAL_INGESTION",
        mongo_collection: str = "raw_documents",
    ) -> None:
        self.source_system = source_system
        self.mongo_collection = mongo_collection

    def land_raw_to_mongo(self, payload: Dict[str, Any], payload_hash: str) -> str:
        """Persiste o payload bruto no MongoDB de forma idempotente e retorna o ObjectId como string."""
        col = get_raw_collection(self.mongo_collection)
        col.create_index([("payload_hash", 1)], unique=True)

        doc = {
            "payload_hash": payload_hash,
            "source_system": self.source_system,
            "ingested_at": datetime.now(timezone.utc),
            "raw_payload": payload,
        }
        col.update_one(
            {"payload_hash": payload_hash},
            {"$setOnInsert": doc},
            upsert=True,
        )
        stored = col.find_one({"payload_hash": payload_hash})
        return str(stored["_id"]) if stored and "_id" in stored else payload_hash

    def route_to_quarantine(
        self,
        payload_hash: str,
        mongo_raw_id: str,
        errors: Any,
    ) -> str:
        """Registra payload inválido na fila de quarentena (quarantine_records) para inspeção HITL."""
        errors_json = json.dumps(errors, default=str)
        with engine.begin() as conn:
            quarantine_id = conn.execute(
                text(
                    """
                    INSERT INTO quarantine_records (
                        source_system, raw_payload_hash, mongo_raw_id, validation_errors, status
                    )
                    VALUES (:source_system, :payload_hash, :mongo_raw_id, CAST(:errors AS jsonb), 'PENDING_REVIEW')
                    RETURNING quarantine_id
                    """
                ),
                {
                    "source_system": self.source_system,
                    "payload_hash": payload_hash,
                    "mongo_raw_id": mongo_raw_id,
                    "errors": errors_json,
                },
            ).scalar_one()
        return str(quarantine_id)

    def process_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executa o fluxo completo HTAP com Dupla Barreira de Idempotência:
        MongoDB Raw Landing -> Pydantic V2 Validation -> PostgreSQL ACID Upsert (ou Quarantine HITL).
        """
        payload_hash = compute_payload_hash(payload)

        # 1. Barreira 1: Raw Landing no MongoDB (Zero Perda de Dados)
        mongo_raw_id = self.land_raw_to_mongo(payload, payload_hash)

        # 2. Validação Shift-Left Pydantic V2
        try:
            validated = CanonicalRecordInput.model_validate(payload)
        except ValidationError as err:
            quarantine_id = self.route_to_quarantine(
                payload_hash=payload_hash,
                mongo_raw_id=mongo_raw_id,
                errors=err.errors(),
            )
            return {
                "status": "QUARANTINED",
                "quarantine_id": quarantine_id,
                "mongo_raw_id": mongo_raw_id,
                "payload_hash": payload_hash,
                "errors": err.errors(),
            }

        # 3. Barreira 2: Transação ACID no PostgreSQL (Idempotência + Upsert Fiduciário)
        with engine.begin() as conn:
            existing = conn.execute(
                text(
                    "SELECT target_record_id FROM idempotency_registry WHERE payload_hash = :payload_hash"
                ),
                {"payload_hash": payload_hash},
            ).scalar_one_or_none()

            if existing is not None:
                return {
                    "status": "DUPLICATE_IGNORED",
                    "record_id": str(existing),
                    "mongo_raw_id": mongo_raw_id,
                    "payload_hash": payload_hash,
                    "entity_code": validated.entity_code,
                    "net_amount": str(validated.net_amount),
                }

            record_id = conn.execute(
                text(
                    """
                    INSERT INTO canonical_records (
                        entity_code, reference_date, currency_code,
                        gross_amount, deduction_amount, net_amount,
                        mongo_raw_id, raw_payload_hash
                    )
                    VALUES (
                        :entity_code, :reference_date, :currency_code,
                        :gross_amount, :deduction_amount, :net_amount,
                        :mongo_raw_id, :payload_hash
                    )
                    ON CONFLICT (entity_code, reference_date, currency_code)
                    DO UPDATE SET
                        gross_amount = EXCLUDED.gross_amount,
                        deduction_amount = EXCLUDED.deduction_amount,
                        net_amount = EXCLUDED.net_amount,
                        mongo_raw_id = EXCLUDED.mongo_raw_id,
                        raw_payload_hash = EXCLUDED.raw_payload_hash,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING record_id
                    """
                ),
                {
                    "entity_code": validated.entity_code,
                    "reference_date": validated.reference_date,
                    "currency_code": validated.currency_code,
                    "gross_amount": validated.gross_amount,
                    "deduction_amount": validated.deduction_amount,
                    "net_amount": validated.net_amount,
                    "mongo_raw_id": mongo_raw_id,
                    "payload_hash": payload_hash,
                },
            ).scalar_one()

            conn.execute(
                text(
                    """
                    INSERT INTO idempotency_registry (payload_hash, source_system, target_record_id)
                    VALUES (:payload_hash, :source_system, :record_id)
                    ON CONFLICT (payload_hash) DO NOTHING
                    """
                ),
                {
                    "payload_hash": payload_hash,
                    "source_system": self.source_system,
                    "record_id": record_id,
                },
            )

        return {
            "status": "COMMITTED_ACID",
            "record_id": str(record_id),
            "mongo_raw_id": mongo_raw_id,
            "payload_hash": payload_hash,
            "entity_code": validated.entity_code,
            "net_amount": str(validated.net_amount),
        }
