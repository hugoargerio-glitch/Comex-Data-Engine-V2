import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from src.connectors.mongo import get_raw_collection


class RetryableHTTPError(Exception):
    """Exceção disparada para HTTP 429 (Rate Limit) ou 5xx (Erro de Servidor Transitório)."""


def compute_payload_hash(payload: Dict[str, Any]) -> str:
    """Gera hash SHA-256 determinístico a partir de um dicionário JSON canônico."""
    raw_bytes = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw_bytes).hexdigest()


class ResilientAPIExtractor:
    """
    Extrator HTTP resiliente (Modo Pull) com Exponential Backoff + Full Jitter.
    Garante que payloads brutos de APIs externas sejam persistidos de forma idempotente
    no MongoDB (Raw Landing Zone) antes de qualquer normalização relacional.
    """

    def __init__(
        self,
        source_system: str,
        base_url: str = "",
        timeout_seconds: float = 10.0,
        client: Optional[httpx.Client] = None,
    ) -> None:
        self.source_system = source_system
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._client = client

    @retry(
        reraise=True,
        stop=stop_after_attempt(4),
        wait=wait_random_exponential(multiplier=0.5, max=10.0),
        retry=retry_if_exception_type((httpx.TransportError, RetryableHTTPError)),
    )
    def fetch_json(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Executa GET resiliente com retentativa automática em falhas de rede, 429 e 5xx."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}" if self.base_url else endpoint

        if self._client is not None:
            response = self._client.get(url, params=params, headers=headers)
        else:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.get(url, params=params, headers=headers)

        if response.status_code == 429 or response.status_code >= 500:
            raise RetryableHTTPError(
                f"Transient HTTP {response.status_code} from {url}"
            )

        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            return data
        return {"items": data}

    def extract_and_land(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        collection_name: str = "raw_documents",
    ) -> Dict[str, Any]:
        """
        Busca dados da API externa e realiza upsert idempotente na Raw Landing Zone (MongoDB)
        utilizando o SHA-256 do payload bruto como barreira primária.
        """
        payload = self.fetch_json(endpoint=endpoint, params=params, headers=headers)
        payload_hash = compute_payload_hash(payload)
        now = datetime.now(timezone.utc)

        col = get_raw_collection(collection_name)
        col.create_index([("payload_hash", 1)], unique=True)

        doc = {
            "payload_hash": payload_hash,
            "source_system": self.source_system,
            "endpoint": endpoint,
            "ingested_at": now,
            "status": "LANDED",
            "raw_payload": payload,
        }

        col.update_one(
            {"payload_hash": payload_hash},
            {"$setOnInsert": doc},
            upsert=True,
        )
        stored = col.find_one({"payload_hash": payload_hash})
        mongo_id = str(stored["_id"]) if stored and "_id" in stored else payload_hash

        return {
            "mongo_raw_id": mongo_id,
            "payload_hash": payload_hash,
            "source_system": self.source_system,
            "raw_payload": payload,
        }
