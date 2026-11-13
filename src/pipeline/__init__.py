"""Motor de processamento HTAP Dual-Store V2 (MongoDB Landing -> Pydantic Contract -> PostgreSQL ACID / Quarantine -> DuckDB/Parquet)."""

from src.pipeline.comex_processor import (
    COMEX_UUID_NAMESPACE,
    FACT_COMEX_PARQUET_SCHEMA,
    PTAX_PARQUET_SCHEMA,
    ComexHTAPProcessor,
    InfrastructureError,
    LakehousePersistenceError,
    LedgerPersistenceError,
    MissingPTAXRateError,
    MongoLandingError,
    QuarantineNotFoundError,
    QuarantineStateConflictError,
)

__all__ = [
    "COMEX_UUID_NAMESPACE",
    "ComexHTAPProcessor",
    "FACT_COMEX_PARQUET_SCHEMA",
    "InfrastructureError",
    "LakehousePersistenceError",
    "LedgerPersistenceError",
    "MissingPTAXRateError",
    "MongoLandingError",
    "PTAX_PARQUET_SCHEMA",
    "QuarantineNotFoundError",
    "QuarantineStateConflictError",
]
