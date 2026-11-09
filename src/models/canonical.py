from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Any
from pydantic import BaseModel, Field, field_validator, model_validator

class CanonicalRecordInput(BaseModel):
    entity_code: str = Field(..., min_length=2, max_length=100)
    reference_date: date
    currency_code: str = Field(..., min_length=3, max_length=3)
    gross_amount: Decimal
    deduction_amount: Decimal = Field(default=Decimal("0.0000"))
    net_amount: Decimal

    @field_validator("gross_amount", "deduction_amount", "net_amount", mode="before")
    def reject_float_and_quantize(cls, v: Any) -> Decimal:
        if isinstance(v, float):
            raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")
        dec = Decimal(str(v)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        if dec < Decimal("0.0000"):
            raise ValueError("Amount cannot be negative.")
        return dec

    @model_validator(mode="after")
    def verify_math_balance(self) -> "CanonicalRecordInput":
        expected = self.gross_amount - self.deduction_amount
        if abs(self.net_amount - expected) > Decimal("0.0001"):
            raise ValueError(f"Balance check failed: net_amount ({self.net_amount}) != gross ({self.gross_amount}) - deduction ({self.deduction_amount})")
        return self
