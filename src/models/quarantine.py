"""
Domain Model: HITL Quarantine Queue (ODCS v3.2.0 & PostgreSQL comex_quarantine_hitl).
"""

from datetime import datetime
import json
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator

from src.models._validators import (
    MAX_PG_INT,
    reject_bool_int,
    validate_non_blank_str,
    validate_sha256_hex,
)

VALID_QUARANTINE_SOURCE_TYPES = {"INVOICE", "MDIC_BATCH", "SWIFT_STATEMENT"}
VALID_QUARANTINE_REVIEW_STATUSES = {
    "PENDING_REVIEW",
    "APPROVED_OVERRIDE",
    "REJECTED_HITL",
}


class QuarantineRecordInput(BaseModel):
    """
    Canonical HITL Quarantine Record conforming to comex_quarantine_hitl (DDL & ODCS v3.2.0).
    Enforces valid source_type/review_status enums, 64-char SHA-256 raw_payload_hash,
    non-empty JSONB validation_errors array, and strict reviewer consistency invariants.
    """

    quarantine_id: Optional[UUID] = Field(default=None, description="Primary key UUID for quarantined record")
    source_type: str = Field(..., description="Source pipeline: INVOICE, MDIC_BATCH, or SWIFT_STATEMENT")
    raw_payload_hash: str = Field(..., description="Hexadecimal SHA-256 fingerprint of rejected payload")
    mongo_raw_id: Optional[str] = Field(
        default=None,
        max_length=64,
        description="ObjectId pointer to MongoDB raw landing collection",
    )
    validation_errors: List[Union[Dict[str, Any], str]] = Field(
        ...,
        min_length=1,
        description="Non-empty JSON array of Pydantic V2 ValidationError details",
    )
    self_correction_attempts: int = Field(
        default=0,
        ge=0,
        le=MAX_PG_INT,
        description="Autonomous LLM/deterministic self-correction attempts prior to quarantine",
    )
    review_status: str = Field(
        default="PENDING_REVIEW",
        description="Human reviewer status: PENDING_REVIEW, APPROVED_OVERRIDE, or REJECTED_HITL",
    )
    reviewed_by: Optional[str] = Field(
        default=None,
        max_length=100,
        description="Email or username of fiduciary auditor resolving the quarantine",
    )
    reviewed_at: Optional[datetime] = Field(
        default=None,
        description="UTC timestamp when review resolution occurred",
    )
    created_at: Optional[datetime] = Field(
        default=None,
        description="UTC timestamp when the record entered quarantine",
    )

    @field_validator("source_type")
    @classmethod
    def validate_source_type(cls, v: str) -> str:
        cleaned = validate_non_blank_str(v, field_label="source_type", max_length=50)
        assert cleaned is not None
        st = cleaned.upper()
        if st not in VALID_QUARANTINE_SOURCE_TYPES:
            raise ValueError(
                f"Invalid source_type '{v}'. Permitted values: {sorted(VALID_QUARANTINE_SOURCE_TYPES)}"
            )
        return st

    @field_validator("raw_payload_hash", mode="before")
    @classmethod
    def validate_payload_hash(cls, v: Any) -> str:
        digest = validate_sha256_hex(v, field_label="raw_payload_hash", allow_none=False)
        assert digest is not None
        return digest

    @field_validator("mongo_raw_id", mode="before")
    @classmethod
    def validate_mongo_id(cls, v: Any) -> Optional[str]:
        return validate_non_blank_str(
            v,
            field_label="mongo_raw_id",
            max_length=64,
            allow_none=True,
        )

    @field_validator("validation_errors")
    @classmethod
    def validate_errors_array(
        cls,
        v: List[Union[Dict[str, Any], str]],
    ) -> List[Union[Dict[str, Any], str]]:
        if not isinstance(v, list) or len(v) == 0:
            raise ValueError("validation_errors must be a non-empty JSON array.")
        for idx, item in enumerate(v):
            if isinstance(item, dict):
                if not item:
                    raise ValueError(f"validation_errors[{idx}] cannot be an empty dict.")
                try:
                    json.dumps(item, default=str)
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"validation_errors[{idx}] is not JSON-serializable: {exc}") from exc
            elif isinstance(item, str):
                if not item.strip():
                    raise ValueError(f"validation_errors[{idx}] cannot be a blank string.")
            else:
                raise ValueError(
                    f"validation_errors[{idx}] must be a dict or non-blank string."
                )
        return v

    @field_validator("self_correction_attempts", mode="before")
    @classmethod
    def validate_attempts(cls, v: Any) -> int:
        parsed = reject_bool_int(
            v,
            field_label="self_correction_attempts",
            min_value=0,
            max_value=MAX_PG_INT,
            allow_none=False,
        )
        assert parsed is not None
        return parsed

    @field_validator("review_status", mode="before")
    @classmethod
    def validate_review_status(cls, v: Any) -> str:
        cleaned = validate_non_blank_str(v, field_label="review_status", max_length=30)
        assert cleaned is not None
        st = cleaned.upper()
        if st not in VALID_QUARANTINE_REVIEW_STATUSES:
            raise ValueError(
                f"Invalid review_status '{v}'. Permitted values: {sorted(VALID_QUARANTINE_REVIEW_STATUSES)}"
            )
        return st

    @field_validator("reviewed_by", mode="before")
    @classmethod
    def validate_reviewer(cls, v: Any) -> Optional[str]:
        return validate_non_blank_str(
            v,
            field_label="reviewed_by",
            max_length=100,
            allow_none=True,
        )

    @model_validator(mode="after")
    def validate_review_consistency(self) -> "QuarantineRecordInput":
        if self.review_status == "PENDING_REVIEW":
            if self.reviewed_by is not None or self.reviewed_at is not None:
                raise ValueError(
                    "PENDING_REVIEW quarantine records must have reviewed_by=None and reviewed_at=None."
                )
        else:
            if self.reviewed_by is None or self.reviewed_at is None:
                raise ValueError(
                    f"Resolved quarantine record ({self.review_status}) requires both reviewed_by and reviewed_at."
                )
        return self
