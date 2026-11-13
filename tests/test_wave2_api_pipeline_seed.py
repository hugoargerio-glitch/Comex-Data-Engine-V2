"""
Unit Tests: Wave 2 API Hardening, Auth, Rate Limiting, Metrics, Real /health,
SWIFT Persistence/Idempotency, HITL REJECTED_HITL Governance, PTAX No-Lookahead,
and Deterministic Seed x2 Idempotency.
"""

from decimal import Decimal
import os
from uuid import uuid4

import pytest

from src.connectors.lakehouse import duckdb_connection, get_parquet_base_dir
from src.models.invoices import hash_natural_person_pii
from src.pipeline.comex_processor import ComexHTAPProcessor, MissingPTAXRateError

VALID_CNPJ = "33.000.167/0001-01"


def _std_headers() -> dict[str, str]:
    return {"X-API-Key": os.environ["API_KEY"]}


def _auditor_headers() -> dict[str, str]:
    return {"X-API-Key": os.environ["AUDITOR_API_KEY"]}


def _valid_invoice_payload(doc_suffix: str, currency: str = "USD", issue_date: str = "2026-11-09") -> dict:
    return {
        "issuer_tax_id": VALID_CNPJ,
        "document_number": f"W2-INV-{doc_suffix}",
        "issue_date": issue_date,
        "currency_code": currency,
        "incoterm": "FOB",
        "subtotal_amount": "1000.0000",
        "tax_amount": "0.0000",
        "freight_amount": "0.0000",
        "insurance_amount": "0.0000",
        "total_amount": "1000.0000",
    }


def _valid_landed_cost_payload() -> dict:
    return {
        "declaration_number": "26/1948201-3",
        "registration_date": "2026-11-06",
        "clearance_date": "2026-11-07",
        "importer_tax_id": VALID_CNPJ,
        "tax_regime": "LUCRO_REAL",
        "currency_code": "USD",
        "fob_amount": "50000.0000",
        "freight_amount": "3000.0000",
        "insurance_amount": "500.0000",
        "ptax_exchange_rate": "5.823400",
        "ii_rate": "0.1200",
        "ipi_rate": "0.1000",
        "pis_rate": "0.0210",
        "cofins_rate": "0.0965",
        "siscomex_fee_brl": "115.0000",
        "afrmm_brl": "0.0000",
        "icms_rate": "0.1800",
    }


def _valid_swift_payload(ref_id: str = "DUIMP-2026-W2-01", fee: str = "50.0000") -> dict:
    return {
        "reference_document_id": ref_id,
        "transaction_date": "2026-11-09",
        "competence_date": "2026-11-01",
        "gross_amount": "20000.0000",
        "fee_amount": fee,
        "currency_code": "USD",
        "exchange_rate": "5.600000",
    }


# ==============================================================================
# 1. X-API-Key & Auditor Role Authorization Matrix
# ==============================================================================


@pytest.mark.unit
def test_api_key_auth_matrix_401_403_and_valid(api_client, monkeypatch):
    bad_headers = {"X-API-Key": "wrong-key-999"}
    qid = str(uuid4())

    protected_calls = [
        ("POST", "/api/v2/invoices/ingest", _valid_invoice_payload("AUTH")),
        ("POST", "/api/v2/ledger/reconcile-swift", _valid_swift_payload()),
        ("GET", "/api/v2/quarantine/pending", None),
        (
            "POST",
            f"/api/v2/quarantine/{qid}/resolve",
            {"resolution_action": "REJECTED_HITL", "reviewed_by": "auditor@comex.br"},
        ),
        ("POST", "/api/v2/analytics/landed-cost-sim?persist=false", _valid_landed_cost_payload()),
        ("POST", "/api/v2/analytics/landed-cost-sim?persist=true", _valid_landed_cost_payload()),
    ]

    for method, path, body in protected_calls:
        missing_resp = api_client.request(method, path, json=body)
        assert missing_resp.status_code == 401
        invalid_resp = api_client.request(method, path, json=body, headers=bad_headers)
        assert invalid_resp.status_code == 401

    # Non-auditor standard API_KEY gets 403 Forbidden on privileged endpoints
    r_resolve_forbidden = api_client.post(
        f"/api/v2/quarantine/{qid}/resolve",
        json={"resolution_action": "REJECTED_HITL", "reviewed_by": "auditor@comex.br"},
        headers=_std_headers(),
    )
    assert r_resolve_forbidden.status_code == 403

    r_persist_forbidden = api_client.post(
        "/api/v2/analytics/landed-cost-sim?persist=true",
        json=_valid_landed_cost_payload(),
        headers=_std_headers(),
    )
    assert r_persist_forbidden.status_code == 403

    # Non-auditor CAN execute persist=false simulation and standard ingestion
    r_sim_ok = api_client.post(
        "/api/v2/analytics/landed-cost-sim?persist=false",
        json=_valid_landed_cost_payload(),
        headers=_std_headers(),
    )
    assert r_sim_ok.status_code == 200
    assert r_sim_ok.json()["status"] == "SIMULATED"

    # Auditor key IS accepted for privileged persistence & standard reads
    r_persist_ok = api_client.post(
        "/api/v2/analytics/landed-cost-sim?persist=true",
        json=_valid_landed_cost_payload(),
        headers=_auditor_headers(),
    )
    assert r_persist_ok.status_code == 200
    assert r_persist_ok.json()["status"] == "PERSISTED"

    r_pending_auditor = api_client.get("/api/v2/quarantine/pending", headers=_auditor_headers())
    assert r_pending_auditor.status_code == 200

    # Fail closed if configured keys are unset/blank
    monkeypatch.setenv("API_KEY", "")
    monkeypatch.setenv("AUDITOR_API_KEY", "")
    assert api_client.get("/api/v2/quarantine/pending", headers=_std_headers()).status_code == 401


# ==============================================================================
# 2. Rate Limit (429), Correlation IDs & Metrics
# ==============================================================================


@pytest.mark.unit
def test_rate_limit_429_and_exempt_health_metrics(api_client, monkeypatch):
    import src.api.main as api_main

    api_main.app.state.limiter.reset()
    monkeypatch.setenv("RATE_LIMIT_PER_MINUTE", "3")

    for _ in range(3):
        resp = api_client.get("/api/v2/quarantine/pending", headers=_std_headers())
        assert resp.status_code == 200

    limited = api_client.get("/api/v2/quarantine/pending", headers=_std_headers())
    assert limited.status_code == 429
    assert "Rate limit exceeded" in limited.json()["detail"]["message"]
    assert "x-correlation-id" in limited.headers

    # /health and /metrics must remain exempt from rate limits
    assert api_client.get("/health").status_code == 200
    assert api_client.get("/metrics").status_code == 200


@pytest.mark.unit
def test_metrics_json_prometheus_and_correlation_id(api_client):
    custom_cid = "trace-audit-2026-001"
    r1 = api_client.get(
        "/api/v2/quarantine/pending",
        headers={**_std_headers(), "X-Correlation-ID": custom_cid},
    )
    assert r1.status_code == 200
    assert r1.headers["x-correlation-id"] == custom_cid

    # Unsafe correlation header must be replaced with a generated UUID
    r_bad_cid = api_client.get(
        "/health",
        headers={"X-Correlation-ID": "bad\nheader injection"},
    )
    assert r_bad_cid.status_code == 200
    assert r_bad_cid.headers["x-correlation-id"] != "bad\nheader injection"

    # Trigger an error status for errors_total coverage
    api_client.get("/api/v2/quarantine/pending")

    json_metrics = api_client.get("/metrics").json()
    assert json_metrics["requests_total"] >= 3
    assert json_metrics["errors_total"] >= 1
    assert "200" in json_metrics["status_counts"]
    assert "401" in json_metrics["status_counts"]

    prom_resp = api_client.get("/metrics?format=prometheus")
    assert prom_resp.status_code == 200
    assert prom_resp.headers["content-type"].startswith("text/plain")
    assert "comex_http_requests_total" in prom_resp.text
    assert "comex_http_errors_total" in prom_resp.text


# ==============================================================================
# 3. Real /health Probes & Sanitized 503 Degraded Responses
# ==============================================================================


@pytest.mark.unit
def test_health_endpoint_degrades_503_on_pg_mongo_or_duckdb_failure(
    api_client, fake_htap_env, monkeypatch
):
    import src.api.main as api_main

    healthy = api_client.get("/health")
    assert healthy.status_code == 200
    assert healthy.json()["status"] == "healthy"

    # Mongo failure -> 503 sanitized
    fake_htap_env.fail_mongo = True
    r_mongo = api_client.get("/health")
    assert r_mongo.status_code == 503
    assert r_mongo.json()["checks"]["mongodb"]["status"] == "unhealthy"
    assert r_mongo.json()["checks"]["postgres"]["status"] == "healthy"
    fake_htap_env.fail_mongo = False

    # DuckDB / Lakehouse probe failure -> 503 without leaking secrets in body
    def broken_duckdb():
        raise RuntimeError("secret://user:supersecret@db-internal:5432")

    monkeypatch.setattr(api_main, "duckdb_connection", broken_duckdb)
    r_duck = api_client.get("/health")
    assert r_duck.status_code == 503
    assert r_duck.json()["checks"]["lakehouse"]["status"] == "unhealthy"
    assert "supersecret" not in r_duck.text


@pytest.mark.unit
def test_api_infrastructure_failure_returns_sanitized_503(api_client, fake_htap_env):
    fake_htap_env.fail_mongo = True
    resp = api_client.post(
        "/api/v2/invoices/ingest",
        json=_valid_invoice_payload("503"),
        headers=_std_headers(),
    )
    assert resp.status_code == 503
    assert resp.json()["detail"]["message"] == "Upstream storage or database service is temporarily unavailable."


# ==============================================================================
# 4. Persisted & Idempotent SWIFT Settlement Reconciliation
# ==============================================================================


@pytest.mark.unit
def test_swift_reconcile_persists_mongo_pg_and_replays_idempotently(api_client, fake_htap_env):
    payload = _valid_swift_payload("DUIMP-SWIFT-PERSIST-01", fee="50.0000")

    r1 = api_client.post("/api/v2/ledger/reconcile-swift", json=payload, headers=_std_headers())
    assert r1.status_code == 200
    d1 = r1.json()
    assert d1["status"] == "RECONCILED"
    assert d1["idempotent_replay"] is False
    assert d1["lines_count"] == 3
    assert len(fake_htap_env.pg_state["journal_entries"]) == 1
    assert len(fake_htap_env.pg_state["journal_lines"]) == 3
    assert len(fake_htap_env.mongo_docs["raw_swift_statements"]) == 1
    assert d1["payload_hash"] in fake_htap_env.pg_state["ingestion_idempotency_registry"]

    # Replay exact payload -> ZERO new rows, DUPLICATE_IGNORED
    r2 = api_client.post("/api/v2/ledger/reconcile-swift", json=payload, headers=_std_headers())
    assert r2.status_code == 200
    d2 = r2.json()
    assert d2["status"] == "DUPLICATE_IGNORED"
    assert d2["idempotent_replay"] is True
    assert d2["entry_id"] == d1["entry_id"]
    assert d2["lines_count"] == 3
    assert len(fake_htap_env.pg_state["journal_entries"]) == 1
    assert len(fake_htap_env.pg_state["journal_lines"]) == 3
    assert len(fake_htap_env.mongo_docs["raw_swift_statements"]) == 1

    # Zero-fee SWIFT generates 2 balanced legs
    zero_fee = _valid_swift_payload("DUIMP-SWIFT-ZEROFEE", fee="0.0000")
    r_zf = api_client.post("/api/v2/ledger/reconcile-swift", json=zero_fee, headers=_std_headers())
    assert r_zf.status_code == 200
    assert r_zf.json()["lines_count"] == 2

    # Float or fee >= gross -> 422
    bad_float = _valid_swift_payload("DUIMP-BAD-FLOAT")
    bad_float["gross_amount"] = 100.5
    assert api_client.post("/api/v2/ledger/reconcile-swift", json=bad_float, headers=_std_headers()).status_code == 422

    bad_fee = _valid_swift_payload("DUIMP-BAD-FEE", fee="20000.0000")
    assert api_client.post("/api/v2/ledger/reconcile-swift", json=bad_fee, headers=_std_headers()).status_code == 422


# ==============================================================================
# 5. HITL REJECTED_HITL Governance, 409 Replay, Pagination & PII Masking
# ==============================================================================


@pytest.mark.unit
def test_hitl_rejected_hitl_replay_409_pagination_and_pii_masking(api_client, fake_htap_env):
    bad_payload = _valid_invoice_payload("REJ-01")
    bad_payload["total_amount"] = "9999.0000"  # Math error -> quarantine
    bad_payload["contact_email"] = "sensitive.exporter@example.com"

    r_q = api_client.post("/api/v2/invoices/ingest", json=bad_payload, headers=_std_headers())
    assert r_q.status_code == 422
    qid = r_q.json()["detail"]["quarantine_id"]

    # Natural-person PII must be SHA-256 hashed in MongoDB landing doc
    mongo_docs = list(fake_htap_env.mongo_docs["raw_commercial_invoices"].values())
    assert len(mongo_docs) == 1
    expected_contact_hash = hash_natural_person_pii(
        "sensitive.exporter@example.com",
        os.environ["PII_HASH_SALT"],
    )
    assert mongo_docs[0]["raw_payload"]["contact_email"] == expected_contact_hash

    # Replay while PENDING_REVIEW returns 422 duplicate=True
    r_q_dup = api_client.post("/api/v2/invoices/ingest", json=bad_payload, headers=_std_headers())
    assert r_q_dup.status_code == 422
    assert r_q_dup.json()["detail"]["duplicate"] is True

    # Resolve via REJECTED alias -> normalized to REJECTED_HITL with hashed reviewed_by
    r_rej = api_client.post(
        f"/api/v2/quarantine/{qid}/resolve",
        json={"resolution_action": "REJECTED", "reviewed_by": "auditor.chief@comex.br"},
        headers=_auditor_headers(),
    )
    assert r_rej.status_code == 200
    rej_body = r_rej.json()
    assert rej_body["review_status"] == "REJECTED_HITL"
    assert len(rej_body["reviewed_by"]) == 64
    assert rej_body["reviewed_by"] == hash_natural_person_pii(
        "auditor.chief@comex.br",
        os.environ["PII_HASH_SALT"],
    )

    # Replaying a REJECTED_HITL payload via /ingest returns HTTP 409 Conflict
    r_409 = api_client.post("/api/v2/invoices/ingest", json=bad_payload, headers=_std_headers())
    assert r_409.status_code == 409
    assert r_409.json()["detail"]["idempotency_status"] == "REJECTED_HITL"
    assert r_409.json()["detail"]["review_status"] == "REJECTED_HITL"

    # Pending filter, REJECTED alias normalization, invalid filter 422, bad UUID 422
    r_filt = api_client.get(
        "/api/v2/quarantine/pending?review_status=REJECTED&limit=10&offset=0",
        headers=_std_headers(),
    )
    assert r_filt.status_code == 200
    assert r_filt.json()["review_status"] == "REJECTED_HITL"
    assert any(it["quarantine_id"] == qid for it in r_filt.json()["items"])

    assert (
        api_client.get("/api/v2/quarantine/pending?review_status=BOGUS", headers=_std_headers()).status_code
        == 422
    )
    assert (
        api_client.post(
            "/api/v2/quarantine/not-a-uuid/resolve",
            json={"resolution_action": "REJECTED_HITL", "reviewed_by": "a@b.com"},
            headers=_auditor_headers(),
        ).status_code
        == 422
    )


# ==============================================================================
# 6. Competence-Date PTAX No-Lookahead & Missing PTAX Rate Guard
# ==============================================================================


@pytest.mark.unit
def test_processor_competence_ptax_no_lookahead_and_missing_rate(fake_htap_env):
    fake_htap_env.clear_ptax()
    fake_htap_env.seed_ptax(20261108, "USD", "5.700000", "5.710000")
    fake_htap_env.seed_ptax(20261120, "USD", "6.990000", "7.000000")  # Future rate!

    processor = ComexHTAPProcessor(engine=fake_htap_env.engine, source_system="TEST_PTAX")

    # Invoice dated 2026-11-10 MUST select 20261108 (5.710000), never look ahead to 20261120
    res = processor.process_invoice(_valid_invoice_payload("PTAX-NOLK", currency="USD", issue_date="2026-11-10"))
    assert res["status"] == "COMMITTED_ACID"
    assert res["fx_rate_applied"] == "5.710000"

    # Earlier date or unseeded currency fails explicitly with MissingPTAXRateError
    with pytest.raises(MissingPTAXRateError):
        processor.process_invoice(_valid_invoice_payload("PTAX-MISS", currency="EUR", issue_date="2026-11-10"))


# ==============================================================================
# 7. Deterministic Seed x2 Idempotency (+0 Rows on Second Run)
# ==============================================================================


@pytest.mark.unit
def test_seed_main_twice_is_100_percent_idempotent(fake_htap_env, monkeypatch):
    import src.seed as seed_mod

    monkeypatch.setattr(seed_mod.command, "upgrade", lambda cfg, rev: None)
    monkeypatch.setattr(seed_mod, "engine", fake_htap_env.engine)
    monkeypatch.setattr(seed_mod, "get_mongo_client", lambda: fake_htap_env.mongo_client)

    seed_mod.main()

    def _snapshot_counts() -> dict[str, int]:
        parquet_fact_dir = get_parquet_base_dir() / "fact_comex_operations"
        parquet_glob = (parquet_fact_dir / "**" / "*.parquet").resolve().as_posix()
        with duckdb_connection() as dcon:
            duck_count = int(
                dcon.execute(
                    "SELECT COUNT(*) FROM read_parquet(?, hive_partitioning=true)",
                    [parquet_glob],
                ).fetchone()[0]
            )
        return {
            "commercial_invoices": len(fake_htap_env.pg_state["commercial_invoices"]),
            "journal_entries": len(fake_htap_env.pg_state["journal_entries"]),
            "journal_lines": len(fake_htap_env.pg_state["journal_lines"]),
            "comex_quarantine_hitl": len(fake_htap_env.pg_state["comex_quarantine_hitl"]),
            "dim_currency_ptax": len(fake_htap_env.pg_state["dim_currency_ptax"]),
            "fact_comex_operations": len(fake_htap_env.pg_state["fact_comex_operations"]),
            "customs_nationalizations": len(fake_htap_env.pg_state["customs_nationalizations"]),
            "ingestion_idempotency_registry": len(fake_htap_env.pg_state["ingestion_idempotency_registry"]),
            "mongo_invoices": len(fake_htap_env.mongo_docs.get("raw_commercial_invoices", {})),
            "mongo_swift": len(fake_htap_env.mongo_docs.get("raw_swift_statements", {})),
            "duckdb_facts": duck_count,
        }

    run1 = _snapshot_counts()
    assert run1["commercial_invoices"] > 0
    assert run1["journal_entries"] > 0
    assert run1["journal_lines"] > 0
    assert run1["customs_nationalizations"] > 0
    assert run1["fact_comex_operations"] > 0
    assert run1["duckdb_facts"] == run1["fact_comex_operations"]

    # Zero-sum invariant in seeded ledger
    delta = sum(
        (r["debit_amount"] - r["credit_amount"] for r in fake_htap_env.pg_state["journal_lines"].values()),
        Decimal("0.0000"),
    )
    assert delta == Decimal("0.0000")

    # Run #2 must produce +0 extra rows everywhere
    seed_mod.main()
    run2 = _snapshot_counts()
    assert run2 == run1
