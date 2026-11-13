"""
Comex Data Engine V2 — Hardened Fiduciary HTAP API Gateway.

Exposes V2 REST endpoints backed by ACID PostgreSQL + MongoDB Raw Lake + DuckDB/Parquet,
enforcing API-Key / Auditor-Key authorization, rate limiting, structured JSON logging with
correlation IDs, Prometheus/JSON metrics, real health checks, and sanitized error responses.
"""

from __future__ import annotations

import hmac
import logging
import os
import re
import threading
import time
from collections import defaultdict
from contextvars import ContextVar
from datetime import date
from decimal import Decimal
from typing import Any, Dict, Literal, Optional
from uuid import UUID, uuid4

import structlog
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic import ValidationError as PydanticValidationError
from pymongo.errors import PyMongoError
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from src.connectors.lakehouse import duckdb_connection, get_parquet_base_dir
from src.connectors.mongo import get_mongo_client
from src.connectors.postgres import engine as postgres_engine
from src.models._validators import (
    MAX_NUMERIC_19_4,
    MAX_NUMERIC_19_6,
    parse_strict_decimal,
    validate_iso_currency_3,
    validate_non_blank_str,
)
from src.models.customs import CustomsNationalizationCalcInput
from src.pipeline.comex_processor import (
    ComexHTAPProcessor,
    InfrastructureError,
    MissingPTAXRateError,
    QuarantineNotFoundError,
    QuarantineStateConflictError,
    _normalize_review_status_token,
    _sanitize_validation_errors,
)

# ==============================================================================
# Structured JSON Logging (structlog) & Correlation Context
# ==============================================================================

_CORRELATION_ID_CTX: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)
_CORRELATION_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:/-]{1,128}$")


def _configure_structlog() -> None:
    logging.basicConfig(format="%(message)s", level=logging.INFO)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


_configure_structlog()
slog = structlog.get_logger("comex.api")


# ==============================================================================
# In-Process Metrics Registry
# ==============================================================================


class MetricsRegistry:
    """Thread-safe HTTP request counter and latency tracker for /metrics."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        with self._lock:
            self._requests_total: int = 0
            self._errors_total: int = 0
            self._latency_total_ms: float = 0.0
            self._latency_min_ms: Optional[float] = None
            self._latency_max_ms: float = 0.0
            self._status_counts: Dict[str, int] = defaultdict(int)
            self._routes: Dict[str, Dict[str, Any]] = {}

    def record(self, method: str, route_path: str, status_code: int, duration_ms: float) -> None:
        route_key = f"{method.upper()} {route_path}"
        status_key = str(status_code)
        is_error = status_code >= 400
        with self._lock:
            self._requests_total += 1
            if is_error:
                self._errors_total += 1
            self._latency_total_ms += duration_ms
            if self._latency_min_ms is None or duration_ms < self._latency_min_ms:
                self._latency_min_ms = duration_ms
            if duration_ms > self._latency_max_ms:
                self._latency_max_ms = duration_ms
            self._status_counts[status_key] += 1

            bucket = self._routes.get(route_key)
            if bucket is None:
                bucket = {
                    "requests_total": 0,
                    "errors_total": 0,
                    "latency_total_ms": 0.0,
                    "max_latency_ms": 0.0,
                    "status_counts": defaultdict(int),
                }
                self._routes[route_key] = bucket
            bucket["requests_total"] += 1
            if is_error:
                bucket["errors_total"] += 1
            bucket["latency_total_ms"] += duration_ms
            if duration_ms > bucket["max_latency_ms"]:
                bucket["max_latency_ms"] = duration_ms
            bucket["status_counts"][status_key] += 1

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            total = self._requests_total
            avg_ms = round(self._latency_total_ms / total, 3) if total > 0 else 0.0
            routes_out: Dict[str, Any] = {}
            for route_key, bucket in sorted(self._routes.items()):
                r_total = bucket["requests_total"]
                routes_out[route_key] = {
                    "requests_total": r_total,
                    "errors_total": bucket["errors_total"],
                    "avg_latency_ms": round(bucket["latency_total_ms"] / r_total, 3) if r_total > 0 else 0.0,
                    "max_latency_ms": round(bucket["max_latency_ms"], 3),
                    "status_counts": dict(sorted(bucket["status_counts"].items())),
                }
            return {
                "service": "comex-fiduciary-htap-engine",
                "requests_total": total,
                "errors_total": self._errors_total,
                "status_counts": dict(sorted(self._status_counts.items())),
                "latency_ms": {
                    "avg": avg_ms,
                    "min": round(self._latency_min_ms, 3) if self._latency_min_ms is not None else 0.0,
                    "max": round(self._latency_max_ms, 3),
                    "total": round(self._latency_total_ms, 3),
                },
                "routes": routes_out,
            }

    def render_prometheus(self) -> str:
        snap = self.snapshot()
        lines = [
            "# HELP comex_http_requests_total Total HTTP requests processed by the API gateway.",
            "# TYPE comex_http_requests_total counter",
            f"comex_http_requests_total {snap['requests_total']}",
            "# HELP comex_http_errors_total Total HTTP responses with status >= 400.",
            "# TYPE comex_http_errors_total counter",
            f"comex_http_errors_total {snap['errors_total']}",
            "# HELP comex_http_request_duration_ms_avg Average HTTP request latency in milliseconds.",
            "# TYPE comex_http_request_duration_ms_avg gauge",
            f"comex_http_request_duration_ms_avg {snap['latency_ms']['avg']}",
            "# HELP comex_http_request_duration_ms_max Max HTTP request latency in milliseconds.",
            "# TYPE comex_http_request_duration_ms_max gauge",
            f"comex_http_request_duration_ms_max {snap['latency_ms']['max']}",
            "# HELP comex_http_responses_by_status_total HTTP responses partitioned by status code.",
            "# TYPE comex_http_responses_by_status_total counter",
        ]
        for code, count in snap["status_counts"].items():
            lines.append(f'comex_http_responses_by_status_total{{status="{code}"}} {count}')
        return "\n".join(lines) + "\n"


metrics_registry = MetricsRegistry()


class CorrelationAndMetricsMiddleware:
    """ASGI middleware assigning X-Correlation-ID, recording metrics, and logging JSON requests."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        incoming_id: Optional[str] = None
        for header_name, header_val in scope.get("headers", []):
            if header_name.lower() == b"x-correlation-id":
                candidate = header_val.decode("latin-1").strip()
                if _CORRELATION_ID_PATTERN.match(candidate):
                    incoming_id = candidate
                break

        correlation_id = incoming_id or str(uuid4())
        token = _CORRELATION_ID_CTX.set(correlation_id)
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(correlation_id=correlation_id)

        start_ns = time.perf_counter_ns()
        status_code = 500
        method = str(scope.get("method", "GET"))
        raw_path = str(scope.get("path", "/"))

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = int(message["status"])
                headers = [
                    (k, v)
                    for k, v in message.get("headers", [])
                    if k.lower() != b"x-correlation-id"
                ]
                headers.append((b"x-correlation-id", correlation_id.encode("ascii")))
                message["headers"] = headers
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            route = scope.get("route")
            route_path = str(getattr(route, "path", raw_path))
            metrics_registry.record(method, route_path, status_code, duration_ms)
            slog.info(
                "http_request_completed",
                method=method,
                path=raw_path,
                route=route_path,
                status_code=status_code,
                duration_ms=round(duration_ms, 3),
            )
            structlog.contextvars.clear_contextvars()
            _CORRELATION_ID_CTX.reset(token)


# ==============================================================================
# Rate Limiter Configuration (slowapi)
# ==============================================================================


def _rate_limit_spec() -> str:
    raw = os.getenv("RATE_LIMIT_PER_MINUTE", "60").strip()
    try:
        per_minute = int(raw)
        if per_minute < 1:
            per_minute = 60
    except ValueError:
        per_minute = 60
    return f"{per_minute}/minute"


limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[_rate_limit_spec],
    headers_enabled=True,
)


# ==============================================================================
# FastAPI Application & Exception Handlers (Sanitized — No Raw Secrets/Traces)
# ==============================================================================

app = FastAPI(
    title="Comex Data Engine V2 — Fiduciary HTAP API",
    description=(
        "Enterprise HTAP Pipeline for International Trade (MongoDB Raw + PostgreSQL ACID "
        "Ledger + DuckDB/Parquet Star Schema)."
    ),
    version="2.1.0",
)
app.state.limiter = limiter
app.state.metrics = metrics_registry

# Outer middleware wraps SlowAPIMiddleware so 429 responses get X-Correlation-ID & metrics.
app.add_middleware(SlowAPIMiddleware)
app.add_middleware(CorrelationAndMetricsMiddleware)


@app.exception_handler(RateLimitExceeded)
def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        content={"detail": {"message": "Rate limit exceeded. Please retry later."}},
    )


@app.exception_handler(RequestValidationError)
def request_validation_exception_handler(
    request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    sanitized_errors = _sanitize_validation_errors(list(exc.errors()))
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={
            "detail": {
                "message": "Request validation failed.",
                "errors": sanitized_errors,
            }
        },
    )


@app.exception_handler(InfrastructureError)
@app.exception_handler(SQLAlchemyError)
@app.exception_handler(PyMongoError)
def infrastructure_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    slog.error(
        "upstream_infrastructure_failure",
        error_type=type(exc).__name__,
        path=request.url.path,
    )
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "detail": {
                "message": "Upstream storage or database service is temporarily unavailable."
            }
        },
    )


@app.exception_handler(Exception)
def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    slog.error(
        "unhandled_api_exception",
        error_type=type(exc).__name__,
        path=request.url.path,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": {"message": "An unexpected internal server error occurred."}},
    )


# ==============================================================================
# X-API-Key & Auditor Authorization Dependencies
# ==============================================================================

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def _keys_equal(provided: str, expected: str) -> bool:
    if not provided or not expected:
        return False
    return hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8"))


def _configured_keys() -> tuple[str, str]:
    api_key = os.getenv("API_KEY", "").strip()
    auditor_key = os.getenv("AUDITOR_API_KEY", "").strip()
    return api_key, auditor_key


def require_api_key(
    x_api_key: Optional[str] = Depends(_api_key_header),
) -> str:
    """
    Validates X-API-Key for standard ingestion and read endpoints.
    Accepts either standard API_KEY or distinct AUDITOR_API_KEY. Missing or
    invalid keys fail closed with HTTP 401 Unauthorized.
    """
    api_key, auditor_key = _configured_keys()
    provided = x_api_key.strip() if isinstance(x_api_key, str) else ""
    if auditor_key and _keys_equal(provided, auditor_key):
        return "auditor"
    if api_key and _keys_equal(provided, api_key):
        return "standard"
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail={"message": "Missing or invalid X-API-Key."},
    )


def require_auditor_api_key(
    x_api_key: Optional[str] = Depends(_api_key_header),
) -> str:
    """
    Validates X-API-Key for privileged HITL override/persistence endpoints.
    - Missing/invalid key -> 401 Unauthorized.
    - Valid non-auditor API_KEY -> 403 Forbidden.
    - Valid AUDITOR_API_KEY -> authorized ('auditor').
    """
    api_key, auditor_key = _configured_keys()
    provided = x_api_key.strip() if isinstance(x_api_key, str) else ""
    if not provided:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"message": "Missing or invalid X-API-Key."},
        )
    if auditor_key and _keys_equal(provided, auditor_key):
        return "auditor"
    if api_key and _keys_equal(provided, api_key):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"message": "Auditor authorization required for this operation."},
        )
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail={"message": "Missing or invalid X-API-Key."},
    )


def require_landed_cost_auth(
    persist: bool = Query(default=False),
    x_api_key: Optional[str] = Depends(_api_key_header),
) -> str:
    """
    Authorizes landed-cost simulation requests:
    - persist=False requires standard API_KEY (or AUDITOR_API_KEY) -> 401 on bad key.
    - persist=True requires AUDITOR_API_KEY -> 401 on bad key, 403 on non-auditor API_KEY.
    """
    if persist:
        return require_auditor_api_key(x_api_key)
    return require_api_key(x_api_key)


# ==============================================================================
# Request Schemas
# ==============================================================================


class SwiftReconciliationRequest(BaseModel):
    reference_document_id: str
    transaction_date: date
    competence_date: date
    gross_amount: Decimal
    fee_amount: Decimal
    currency_code: str = Field(default="USD")
    exchange_rate: Decimal = Field(default=Decimal("1.000000"))
    cash_account: str = Field(default="1.1.1.01")
    fee_account: str = Field(default="4.1.2.01")
    ar_account: str = Field(default="1.1.2.01")

    @field_validator("reference_document_id", mode="before")
    @classmethod
    def _validate_ref_doc(cls, v: Any) -> str:
        val = validate_non_blank_str(v, field_label="reference_document_id", max_length=100)
        assert val is not None
        return val

    @field_validator("cash_account", "fee_account", "ar_account", mode="before")
    @classmethod
    def _validate_accounts(cls, v: Any) -> str:
        val = validate_non_blank_str(v, field_label="account_code", max_length=50)
        assert val is not None
        return val

    @field_validator("currency_code", mode="before")
    @classmethod
    def _validate_currency(cls, v: Any) -> str:
        return validate_iso_currency_3(v, field_label="currency_code")

    @field_validator("gross_amount", mode="before")
    @classmethod
    def _validate_gross(cls, v: Any) -> Decimal:
        val = parse_strict_decimal(
            v,
            scale=Decimal("0.0001"),
            max_value=MAX_NUMERIC_19_4,
            strictly_positive=True,
            field_label="gross_amount",
        )
        assert val is not None
        return val

    @field_validator("fee_amount", mode="before")
    @classmethod
    def _validate_fee(cls, v: Any) -> Decimal:
        val = parse_strict_decimal(
            v,
            scale=Decimal("0.0001"),
            max_value=MAX_NUMERIC_19_4,
            min_value=Decimal("0.0000"),
            strictly_positive=False,
            field_label="fee_amount",
        )
        assert val is not None
        return val

    @field_validator("exchange_rate", mode="before")
    @classmethod
    def _validate_fx(cls, v: Any) -> Decimal:
        val = parse_strict_decimal(
            v,
            scale=Decimal("0.000001"),
            max_value=MAX_NUMERIC_19_6,
            strictly_positive=True,
            field_label="exchange_rate",
        )
        assert val is not None
        return val

    @model_validator(mode="after")
    def _validate_fee_lt_gross(self) -> "SwiftReconciliationRequest":
        if self.fee_amount >= self.gross_amount:
            raise ValueError("fee_amount must be strictly less than gross_amount.")
        return self


class QuarantineResolutionRequest(BaseModel):
    resolution_action: Literal["APPROVED_OVERRIDE", "REJECTED_HITL", "REJECTED"]
    reviewed_by: str = Field(
        ...,
        description="Auditor email or identifier resolving the quarantine (hashed at rest)",
    )
    corrected_payload: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Human-corrected payload required for APPROVED_OVERRIDE",
    )

    @field_validator("reviewed_by", mode="before")
    @classmethod
    def _validate_reviewer(cls, v: Any) -> str:
        val = validate_non_blank_str(v, field_label="reviewed_by", max_length=255)
        assert val is not None
        return val

    @model_validator(mode="after")
    def _validate_override_payload(self) -> "QuarantineResolutionRequest":
        if self.resolution_action == "APPROVED_OVERRIDE" and self.corrected_payload is None:
            raise ValueError("corrected_payload is required for APPROVED_OVERRIDE resolution.")
        return self


# ==============================================================================
# Unauthenticated Health & Metrics Endpoints (Exempt from Auth & Rate Limit)
# ==============================================================================


@app.get("/health")
@limiter.exempt
def health_check(response: Response) -> Dict[str, Any]:
    """
    Live infrastructure health probe verifying PostgreSQL (SELECT 1),
    MongoDB (admin ping), and DuckDB/Parquet lakehouse accessibility.
    Returns HTTP 200 when healthy, HTTP 503 when degraded.
    """
    checks: Dict[str, Dict[str, str]] = {}
    all_healthy = True

    # 1. PostgreSQL Probe
    try:
        with postgres_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        checks["postgres"] = {"status": "healthy"}
    except Exception as exc:
        all_healthy = False
        slog.warning("health_probe_postgres_failed", error_type=type(exc).__name__)
        checks["postgres"] = {"status": "unhealthy"}

    # 2. MongoDB Probe
    try:
        mongo_client = get_mongo_client()
        mongo_client.admin.command("ping")
        checks["mongodb"] = {"status": "healthy"}
    except Exception as exc:
        all_healthy = False
        slog.warning("health_probe_mongodb_failed", error_type=type(exc).__name__)
        checks["mongodb"] = {"status": "unhealthy"}

    # 3. DuckDB & Parquet Lakehouse Probe
    try:
        parquet_dir = get_parquet_base_dir()
        parquet_dir.mkdir(parents=True, exist_ok=True)
        if not os.access(parquet_dir, os.R_OK | os.W_OK):
            raise PermissionError("Parquet directory lacks R/W permission")
        with duckdb_connection() as duck_conn:
            probe_row = duck_conn.execute("SELECT 1").fetchone()
            if probe_row is None or probe_row[0] != 1:
                raise RuntimeError("DuckDB SELECT 1 probe failed")
        checks["lakehouse"] = {"status": "healthy"}
    except Exception as exc:
        all_healthy = False
        slog.warning("health_probe_lakehouse_failed", error_type=type(exc).__name__)
        checks["lakehouse"] = {"status": "unhealthy"}

    if not all_healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "degraded",
            "architecture": "HTAP (Mongo + Postgres + DuckDB)",
            "checks": checks,
        }

    response.status_code = status.HTTP_200_OK
    return {
        "status": "healthy",
        "architecture": "HTAP (Mongo + Postgres + DuckDB)",
        "checks": checks,
    }


@app.get("/metrics")
@limiter.exempt
def metrics_endpoint(
    format: Optional[Literal["json", "prometheus"]] = Query(default="json"),
) -> Any:
    """
    Unauthenticated observability endpoint exposing request/status counters and latency metrics.
    Supports JSON (default) or Prometheus text exposition (?format=prometheus).
    """
    if format == "prometheus":
        return PlainTextResponse(
            content=metrics_registry.render_prometheus(),
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )
    return metrics_registry.snapshot()


# ==============================================================================
# Comex V2 Fiduciary & Customs Endpoints (Protected by X-API-Key)
# ==============================================================================


@app.post("/api/v2/invoices/ingest", status_code=status.HTTP_201_CREATED)
def ingest_comex_invoice(
    payload: Dict[str, Any],
    _auth_role: str = Depends(require_api_key),
) -> Any:
    """
    Ingests an international commercial invoice payload into Comex V2 HTAP Engine:
    MongoDB Raw Landing -> Idempotency Check -> Self-Correction Loop -> ACID Ledger / HITL Quarantine.
    """
    processor = ComexHTAPProcessor(source_system="COMEX_V2_API")
    try:
        result = processor.process_invoice(payload)
    except MissingPTAXRateError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "message": "Missing competence-date PTAX exchange rate for invoice currency."
            },
        ) from None
    except PydanticValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "message": "Invoice payload failed contract validation.",
                "errors": _sanitize_validation_errors(exc.errors()),
            },
        ) from None
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"message": "Invoice payload failed domain validation."},
        ) from None

    result_status = result.get("status")
    idem_status = result.get("idempotency_status")

    if result_status == "QUARANTINED":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "message": "Invoice failed contract validation and was routed to HITL Quarantine.",
                "duplicate": False,
                "quarantine_id": result.get("quarantine_id"),
                "review_status": "PENDING_REVIEW",
                "mongo_raw_id": result.get("mongo_raw_id"),
                "payload_hash": result.get("payload_hash"),
                "self_correction_attempts": result.get("self_correction_attempts"),
                "errors": result.get("errors"),
            },
        )

    if result_status == "DUPLICATE_IGNORED":
        if idem_status == "QUARANTINED":
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail={
                    "message": "Duplicate payload matches an existing HITL quarantine record.",
                    "duplicate": True,
                    "idempotency_status": "QUARANTINED",
                    "quarantine_id": result.get("quarantine_id"),
                    "review_status": result.get("review_status", "PENDING_REVIEW"),
                    "mongo_raw_id": result.get("mongo_raw_id"),
                    "payload_hash": result.get("payload_hash"),
                    "errors": result.get("errors"),
                },
            )
        if idem_status == "REJECTED_HITL":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": "Duplicate payload matches a rejected HITL quarantine record.",
                    "duplicate": True,
                    "idempotency_status": "REJECTED_HITL",
                    "quarantine_id": result.get("quarantine_id"),
                    "review_status": result.get("review_status", "REJECTED_HITL"),
                    "mongo_raw_id": result.get("mongo_raw_id"),
                    "payload_hash": result.get("payload_hash"),
                    "errors": result.get("errors"),
                },
            )
        return JSONResponse(status_code=status.HTTP_200_OK, content=result)

    return result


@app.post("/api/v2/ledger/reconcile-swift", status_code=status.HTTP_200_OK)
def reconcile_swift_settlement(
    req: SwiftReconciliationRequest,
    _auth_role: str = Depends(require_api_key),
) -> Dict[str, Any]:
    """
    Reconciles a SWIFT camt.053 wire statement with intermediary fee deduction,
    atomically persisting MongoDB raw statement + balanced PostgreSQL double-entry
    ledger lines with deterministic uuid5 replay idempotency.
    """
    processor = ComexHTAPProcessor(source_system="COMEX_V2_SWIFT_API")
    raw_doc = req.model_dump(mode="json")
    try:
        return processor.reconcile_swift_settlement(
            reference_document_id=req.reference_document_id,
            transaction_date=req.transaction_date,
            competence_date=req.competence_date,
            gross_amount=req.gross_amount,
            fee_amount=req.fee_amount,
            currency_code=req.currency_code,
            exchange_rate=req.exchange_rate,
            cash_account=req.cash_account,
            fee_account=req.fee_account,
            ar_account=req.ar_account,
            raw_payload=raw_doc,
        )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"message": "SWIFT reconciliation payload failed domain validation."},
        ) from None


@app.post("/api/v2/analytics/landed-cost-sim", status_code=status.HTTP_200_OK)
def simulate_landed_cost(
    calc_input: CustomsNationalizationCalcInput,
    persist: bool = Query(default=False),
    _auth_role: str = Depends(require_landed_cost_auth),
) -> Dict[str, Any]:
    """
    Simulates Brazilian customs cascading tax calculation (II, IPI, PIS/COFINS,
    Siscomex, AFRMM, ICMS 'Calculo por Dentro') and generates CPC 16 inventory
    segregation entries. If persist=True (AUDITOR_API_KEY required), atomically commits.
    """
    processor = ComexHTAPProcessor(source_system="COMEX_V2_SIMULATOR")
    try:
        result, journal_entry = processor.persist_customs_nationalization(
            calc_input=calc_input,
            persist_db=persist,
        )
    except MissingPTAXRateError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "message": "Missing competence-date PTAX exchange rate for customs currency."
            },
        ) from None
    except PydanticValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "message": "Customs calculation input failed contract validation.",
                "errors": _sanitize_validation_errors(exc.errors()),
            },
        ) from None
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"message": "Customs calculation failed domain validation."},
        ) from None

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


@app.get("/api/v2/quarantine/pending", status_code=status.HTTP_200_OK)
def get_pending_quarantine_items(
    review_status: Optional[str] = Query(
        default="PENDING_REVIEW",
        description="Filter by review status: PENDING_REVIEW, APPROVED_OVERRIDE, REJECTED_HITL (or REJECTED), ALL",
    ),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    _auth_role: str = Depends(require_api_key),
) -> Dict[str, Any]:
    """
    Returns paginated quarantine items filtered by review_status (default: 'PENDING_REVIEW')
    awaiting human auditor review.
    """
    processor = ComexHTAPProcessor(source_system="COMEX_V2_API")
    try:
        items, total = processor.get_quarantine_items_page(
            status_filter=review_status,
            limit=limit,
            offset=offset,
        )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "message": (
                    "Invalid review_status filter. Permitted values: "
                    "PENDING_REVIEW, APPROVED_OVERRIDE, REJECTED_HITL, ALL."
                )
            },
        ) from None

    normalized_status = (
        _normalize_review_status_token(review_status)
        if review_status is not None
        else "ALL"
    )
    return {
        "count": len(items),
        "total": total,
        "limit": limit,
        "offset": offset,
        "review_status": normalized_status,
        "items": items,
    }


@app.post("/api/v2/quarantine/{quarantine_id}/resolve", status_code=status.HTTP_200_OK)
def resolve_quarantined_invoice_endpoint(
    quarantine_id: UUID,
    req: QuarantineResolutionRequest,
    _auth_role: str = Depends(require_auditor_api_key),
) -> Dict[str, Any]:
    """
    Human-in-the-Loop resolution endpoint (AUDITOR_API_KEY required):
      - APPROVED_OVERRIDE: validates corrected_payload and atomically promotes to commercial_invoices + ledger.
      - REJECTED_HITL (or REJECTED alias): marks quarantine record as REJECTED_HITL and updates registry.
    """
    processor = ComexHTAPProcessor(source_system="COMEX_V2_API")
    try:
        return processor.resolve_quarantined_invoice(
            quarantine_id=quarantine_id,
            resolution_action=req.resolution_action,
            reviewed_by=req.reviewed_by,
            corrected_payload=req.corrected_payload,
        )
    except QuarantineNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"message": "Quarantine record not found."},
        ) from None
    except QuarantineStateConflictError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"message": "Quarantine record has already been finalized."},
        ) from None
    except MissingPTAXRateError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "message": "Missing competence-date PTAX exchange rate for corrected invoice currency."
            },
        ) from None
    except PydanticValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "message": "Corrected payload failed contract validation.",
                "errors": _sanitize_validation_errors(exc.errors()),
            },
        ) from None
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"message": "Invalid quarantine resolution request."},
        ) from None
