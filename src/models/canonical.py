"""
Compatibility Canonical Model (hardened to Comex Fiduciary Engine V2 standards).
Preserved for V1 ingestion endpoints/tests while enforcing strict anti-float,
non-None, finite Decimal, NUMERIC(19,4) upper bounds, and ISO currency checks.
"""

from datetime import date
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from src.models._validators import (
    MAX_NUMERIC_19_4,
    parse_strict_decimal,
    validate_iso_currency_3,
    validate_non_blank_str,
)


class CanonicalRecordInput(BaseModel):
    """Canonical record input hardened to V2 DECIMAL(19,4) and strict validation rules."""

    entity_code: str = Field(..., min_length=2, max_length=100)
    reference_date: date
    currency_code: str = Field(..., min_length=3, max_length=3)
    gross_amount: Decimal
    deduction_amount: Decimal = Field(default=Decimal("0.0000"))
    net_amount: Decimal

    @field_validator("entity_code", mode="before")
    @classmethod
    def validate_entity_code(cls, v: Any) -> str:
        cleaned = validate_non_blank_str(v, field_label="entity_code", max_length=100)
        assert cleaned is not None
        if len(cleaned) < 2:
            raise ValueError("entity_code must be at least 2 characters long.")
        return cleaned

    @field_validator("currency_code")
    @classmethod
    def validate_currency_code(cls, v: str) -> str:
        return validate_iso_currency_3(v, field_label="currency_code")

    @field_validator("gross_amount", "deduction_amount", "net_amount", mode="before")
    @classmethod
    def reject_float_and_quantize(cls, v: Any) -> Decimal:
        parsed = parse_strict_decimal(
            v,
            scale=Decimal("0.0001"),
            max_value=MAX_NUMERIC_19_4,
            min_value=Decimal("0.0000"),
            allow_none=False,
            field_label="Monetary amount",
        )
        assert parsed is not None
        return parsed

    @model_validator(mode="after")
    def verify_math_balance(self) -> "CanonicalRecordInput":
        expected = self.gross_amount - self.deduction_amount
        if abs(self.net_amount - expected) > Decimal("0.0001"):
            raise ValueError(
                f"Balance check failed: net_amount ({self.net_amount}) != "
                f"gross ({self.gross_amount}) - deduction ({self.deduction_amount})"
            )
        return self
