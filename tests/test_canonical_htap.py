"""
Unit Tests: Canonical Contracts, V1 Retirement & V2 HTAP Ingestion Core (Isolated from Live DBs).
"""

from decimal import Decimal
import importlib
import os
from uuid import uuid4

import httpx
from pydantic import ValidationError
import pytest

from src.extractors.http_client import ResilientAPIExtractor, compute_payload_hash
from src.models.canonical import CanonicalRecordInput


@pytest.mark.unit
def test_v1_routes_and_legacy_processor_are_retired(api_client):
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("src.pipeline.processor")

    headers = {"X-API-Key": os.environ["API_KEY"]}
    payload = {
        "entity_code": "INV-V1-RETIRED",
        "reference_date": "2026-11-08",
        "currency_code": "EUR",
        "gross_amount": "5000.0000",
        "deduction_amount": "200.0000",
        "net_amount": "4800.0000",
    }
    resp_v1 = api_client.post("/api/v1/ingest", json=payload, headers=headers)
    resp_v1_pipe = api_client.post("/api/v1/ingest/pipeline", json=payload, headers=headers)
    assert resp_v1.status_code == 404
    assert resp_v1_pipe.status_code == 404


@pytest.mark.unit
def test_health_endpoint_unit_mocked(api_client, fake_htap_env):
    response = api_client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["checks"]["postgres"]["status"] == "healthy"
    assert body["checks"]["mongodb"]["status"] == "healthy"
    assert body["checks"]["lakehouse"]["status"] == "healthy"

    fake_htap_env.fail_pg = True
    degraded = api_client.get("/health")
    assert degraded.status_code == 503
    degraded_body = degraded.json()
    assert degraded_body["status"] == "degraded"
    assert degraded_body["checks"]["postgres"]["status"] == "unhealthy"


@pytest.mark.unit
def test_pydantic_rejects_float():
    with pytest.raises(ValidationError) as exc:
        CanonicalRecordInput(
            entity_code="TEST-001",
            reference_date="2026-11-08",
            currency_code="USD",
            gross_amount=100.50,  # FLOAT!
            deduction_amount="0.00",
            net_amount="100.50",
        )
    assert "float is strictly prohibited" in str(exc.value)


@pytest.mark.unit
def test_pydantic_accepts_valid_decimal_and_verifies_math():
    record = CanonicalRecordInput(
        entity_code="TEST-001",
        reference_date="2026-11-08",
        currency_code="USD",
        gross_amount="1000.0000",
        deduction_amount="50.0000",
        net_amount="950.0000",
    )
    assert record.net_amount == Decimal("950.0000")


@pytest.mark.unit
def test_pydantic_rejects_divergent_math():
    with pytest.raises(ValidationError) as exc:
        CanonicalRecordInput(
            entity_code="TEST-001",
            reference_date="2026-11-08",
            currency_code="USD",
            gross_amount="1000.0000",
            deduction_amount="50.0000",
            net_amount="900.0000",  # Errado! Deveria ser 950
        )
    assert "Balance check failed" in str(exc.value)


@pytest.mark.unit
def test_api_v2_ingest_success_and_idempotent_replay(api_client):
    headers = {"X-API-Key": os.environ["API_KEY"]}
    payload = {
        "issuer_tax_id": "33.000.167/0001-01",
        "document_number": f"CANON-V2-{uuid4().hex[:8]}",
        "issue_date": "2026-11-09",
        "currency_code": "EUR",
        "incoterm": "FOB",
        "subtotal_amount": "5000.0000",
        "tax_amount": "0.0000",
        "freight_amount": "0.0000",
        "insurance_amount": "0.0000",
        "total_amount": "5000.0000",
    }
    response = api_client.post("/api/v2/invoices/ingest", json=payload, headers=headers)
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "COMMITTED_ACID"
    assert data["total_amount"] == "5000.0000"
    assert "payload_hash" in data

    replay = api_client.post("/api/v2/invoices/ingest", json=payload, headers=headers)
    assert replay.status_code == 200
    replay_data = replay.json()
    assert replay_data["status"] == "DUPLICATE_IGNORED"
    assert replay_data["payload_hash"] == data["payload_hash"]
    assert replay_data["invoice_id"] == data["invoice_id"]


@pytest.mark.unit
def test_deterministic_payload_hash():
    """Garante que a ordem das chaves no JSON nao altera o hash SHA-256 de idempotencia."""
    doc_a = {"entity_code": "A1", "currency_code": "USD", "net_amount": "100.0000"}
    doc_b = {"net_amount": "100.0000", "entity_code": "A1", "currency_code": "USD"}
    assert compute_payload_hash(doc_a) == compute_payload_hash(doc_b)


@pytest.mark.unit
def test_resilient_api_extractor_retry_on_429_and_503():
    """Valida que o extrator Pull aplica Retry diante de HTTP 429/503 transitorios e falha rapido em 404."""
    attempts = {"count": 0}

    def mock_handler(request: httpx.Request) -> httpx.Response:
        attempts["count"] += 1
        if request.url.path == "/missing":
            return httpx.Response(404, json={"error": "Not found"})
        if attempts["count"] == 1:
            return httpx.Response(429, json={"error": "Rate limit exceeded"})
        if attempts["count"] == 2:
            return httpx.Response(503, json={"error": "Service temporarily unavailable"})
        return httpx.Response(200, json={"moeda": "USD", "cotacao": "5.8240"})

    mock_client = httpx.Client(transport=httpx.MockTransport(mock_handler))
    extractor = ResilientAPIExtractor(
        source_system="BACEN_PTAX",
        base_url="https://api.bcb.gov.br",
        client=mock_client,
    )
    data = extractor.fetch_json("/cotacao")
    assert attempts["count"] == 3
    assert data["cotacao"] == "5.8240"

    before_404 = attempts["count"]
    with pytest.raises(httpx.HTTPStatusError) as exc:
        extractor.fetch_json("/missing")
    assert exc.value.response.status_code == 404
    assert attempts["count"] == before_404 + 1
