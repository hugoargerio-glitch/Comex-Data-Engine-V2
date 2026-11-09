from decimal import Decimal
import httpx
import pytest
from pydantic import ValidationError
from fastapi.testclient import TestClient

from src.models.canonical import CanonicalRecordInput
from src.extractors.http_client import ResilientAPIExtractor, compute_payload_hash
from src.api.main import app

client = TestClient(app)

@pytest.mark.unit
def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

@pytest.mark.unit
def test_pydantic_rejects_float():
    with pytest.raises(ValidationError) as exc:
        CanonicalRecordInput(
            entity_code="TEST-001",
            reference_date="2026-11-08",
            currency_code="USD",
            gross_amount=100.50, # FLOAT!
            deduction_amount="0.00",
            net_amount="100.50"
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
        net_amount="950.0000"
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
            net_amount="900.0000" # Errado! Deveria ser 950
        )
    assert "Balance check failed" in str(exc.value)

@pytest.mark.unit
def test_api_ingest_success():
    payload = {
        "entity_code": "INV-2026-001",
        "reference_date": "2026-11-08",
        "currency_code": "EUR",
        "gross_amount": "5000.0000",
        "deduction_amount": "200.0000",
        "net_amount": "4800.0000"
    }
    response = client.post("/api/v1/ingest", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "ACCEPTED"
    assert data["net_amount"] == "4800.0000"
    assert "payload_hash" in data

@pytest.mark.unit
def test_deterministic_payload_hash():
    """Garante que a ordem das chaves no JSON não altera o hash SHA-256 de idempotência."""
    doc_a = {"entity_code": "A1", "currency_code": "USD", "net_amount": "100.0000"}
    doc_b = {"net_amount": "100.0000", "entity_code": "A1", "currency_code": "USD"}
    assert compute_payload_hash(doc_a) == compute_payload_hash(doc_b)

@pytest.mark.unit
def test_resilient_api_extractor_retry_on_429_and_503():
    """Valida que o extrator Pull aplica Retry (Tenacity) diante de HTTP 429/503 transitórios."""
    attempts = {"count": 0}

    def mock_handler(request: httpx.Request) -> httpx.Response:
        attempts["count"] += 1
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
