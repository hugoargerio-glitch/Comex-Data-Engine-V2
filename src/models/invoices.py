"""
Domain Models: International Commercial Invoices (ODCS v3.2.0 & PostgreSQL ACID Core)
Strictly enforces no IEEE 754 floats, DECIMAL(19,4) monetary precision, scaled cents (* 10000),
corporate B2B tax ID validation (CNPJ Modulo 11 and Swiss UID), Incoterms, and vertical math closure.
"""

from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import re
from typing import Any, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator

# Swiss UID pattern: CHE-XXX.XXX.XXX with optional MWST/TVA/IVA suffix
SWISS_UID_PATTERN = re.compile(r"^CHE-\d{3}\.\d{3}\.\d{3}(\s?(MWST|TVA|IVA))?$")

VALID_CURRENCY_CODES = {"USD", "EUR", "CHF", "BRL"}
VALID_INCOTERMS = {"EXW", "FOB", "CIF", "DDP"}


def validate_cnpj(cnpj: str) -> bool:
    """
    Validates a Brazilian CNPJ (14 digits) using the official Modulo 11 algorithm.
    Accepts formatted ('00.000.000/0000-00') or unformatted digits.
    """
    digits = [int(c) for c in cnpj if c.isdigit()]
    if len(digits) != 14 or len(set(digits)) == 1:
        return False

    # First verification digit
    w1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    s1 = sum(d * w for d, w in zip(digits[:12], w1))
    r1 = s1 % 11
    d1 = 0 if r1 < 2 else 11 - r1
    if digits[12] != d1:
        return False

    # Second verification digit
    w2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    s2 = sum(d * w for d, w in zip(digits[:13], w2))
    r2 = s2 % 11
    d2 = 0 if r2 < 2 else 11 - r2
    return digits[13] == d2


def validate_corporate_tax_id(tax_id: str) -> bool:
    """
    Validates B2B corporate tax identifiers:
    1. Brazilian CNPJ 14-digit Modulo 11
    2. Swiss UID conforming to ^CHE-\\d{3}\\.\\d{3}\\.\\d{3}(\\s?(MWST|TVA|IVA))?$
    Corporate tax IDs must be kept in cleartext for statutory audit.
    """
    if not tax_id or not isinstance(tax_id, str):
        return False
    cleaned = tax_id.strip()
    if SWISS_UID_PATTERN.match(cleaned):
        return True
    return validate_cnpj(cleaned)


def hash_natural_person_pii(value: str, salt: str) -> str:
    """
    Applies salted cryptographic SHA-256 hashing strictly to natural-person PII
    (individual CPF, personal contact names, personal emails, IPs) before persistence.
    Public B2B corporate tax IDs (CNPJ, Swiss UID) are excluded and kept in cleartext.
    """
    if not value:
        return ""
    salted = f"{salt}:{value.strip()}".encode("utf-8")
    return hashlib.sha256(salted).hexdigest()


class CommercialInvoiceLineInput(BaseModel):
    """
    Individual line item on a commercial invoice.
    Strictly prohibits floats and enforces horizontal line math closure.
    """
    line_number: int = Field(..., ge=1, description="Sequential line number")
    description: str = Field(..., min_length=1, description="Merchandise description")
    ncm_code: Optional[str] = Field(None, pattern=r"^\d{8}$", description="8-digit Mercosul NCM tariff code")
    quantity: Decimal = Field(..., gt=Decimal("0.0000"), description="Declared invoice quantity")
    unit_price: Decimal = Field(..., ge=Decimal("0.0000"), description="Unit price in invoice currency")
    total_price: Decimal = Field(..., ge=Decimal("0.0000"), description="Line total price")

    @model_validator(mode="before")
    @classmethod
    def map_legacy_line_total(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "total_price" not in data and "line_total" in data:
                data["total_price"] = data.get("line_total")
        return data

    @field_validator("quantity", "unit_price", "total_price", mode="before")
    @classmethod
    def reject_float_and_quantize(cls, v: Any) -> Decimal:
        if isinstance(v, float):
            raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")
        dec = Decimal(str(v)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        if dec < Decimal("0.0000"):
            raise ValueError("Monetary amount cannot be negative.")
        return dec

    @model_validator(mode="after")
    def verify_horizontal_line_math(self) -> "CommercialInvoiceLineInput":
        expected = (self.quantity * self.unit_price).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        if abs(self.total_price - expected) > Decimal("0.0001"):
            raise ValueError(
                f"Line {self.line_number} horizontal math failed: total_price ({self.total_price}) != "
                f"quantity ({self.quantity}) * unit_price ({self.unit_price}) = {expected}"
            )
        return self


class CommercialInvoiceInput(BaseModel):
    """
    Operational International Commercial Invoice Header conforming to ODCS v3.2.0.
    Enforces DECIMAL(19,4) precision, scaled cents (* 10000), B2B corporate tax ID validation,
    Incoterm rules, and vertical math closure.
    """
    invoice_id: Optional[UUID] = None
    issuer_tax_id: str = Field(..., description="Corporate B2B Tax ID (CNPJ or Swiss UID) in cleartext")
    document_number: str = Field(..., min_length=1, max_length=100, description="Commercial Invoice document serial")
    issue_date: date = Field(..., description="Official invoice issuance date")
    currency_code: str = Field(..., min_length=3, max_length=3, description="ISO 4217 Currency (USD, EUR, CHF, BRL)")
    subtotal_amount: Decimal = Field(default=Decimal("0.0000"), description="Merchandise net amount")
    tax_amount: Decimal = Field(default=Decimal("0.0000"), description="Declared tax amount")
    freight_amount: Decimal = Field(default=Decimal("0.0000"), description="Declared freight amount")
    insurance_amount: Decimal = Field(default=Decimal("0.0000"), description="Declared cargo insurance amount")
    total_amount: Decimal = Field(..., description="Gross total invoice amount")

    # Scaled integer cents representation (amount * 10000)
    subtotal_amount_cents: Optional[int] = None
    tax_amount_cents: Optional[int] = None
    freight_amount_cents: Optional[int] = None
    insurance_amount_cents: Optional[int] = None
    total_amount_cents: Optional[int] = None

    incoterm: str = Field(..., description="ICC Incoterm 2020 (EXW, FOB, CIF, DDP)")
    extraction_confidence: Decimal = Field(default=Decimal("1.0000"), description="Extractor confidence (0.0000 to 1.0000)")
    raw_payload_hash: Optional[str] = Field(None, max_length=64)
    mongo_raw_id: Optional[str] = Field(None, max_length=64)
    lines: List[CommercialInvoiceLineInput] = Field(default_factory=list)

    @field_validator(
        "subtotal_amount",
        "tax_amount",
        "freight_amount",
        "insurance_amount",
        "total_amount",
        mode="before",
    )
    @classmethod
    def reject_float_and_quantize_monetary(cls, v: Any) -> Decimal:
        if isinstance(v, float):
            raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")
        if v is None:
            return Decimal("0.0000")
        dec = Decimal(str(v)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        if dec < Decimal("0.0000"):
            raise ValueError("Monetary amount cannot be negative.")
        return dec

    @field_validator("extraction_confidence", mode="before")
    @classmethod
    def reject_float_and_quantize_confidence(cls, v: Any) -> Decimal:
        if isinstance(v, float):
            raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")
        if v is None:
            return Decimal("1.0000")
        dec = Decimal(str(v)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        if not (Decimal("0.0000") <= dec <= Decimal("1.0000")):
            raise ValueError(f"extraction_confidence must be between 0.0000 and 1.0000, got {dec}")
        return dec

    @field_validator("currency_code")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        code = v.upper().strip()
        if code not in VALID_CURRENCY_CODES:
            raise ValueError(f"Invalid currency_code '{v}'. Permitted values: {sorted(VALID_CURRENCY_CODES)}")
        return code

    @field_validator("incoterm")
    @classmethod
    def validate_incoterm(cls, v: str) -> str:
        inc = v.upper().strip()
        if inc not in VALID_INCOTERMS:
            raise ValueError(f"Invalid incoterm '{v}'. Permitted values: {sorted(VALID_INCOTERMS)}")
        return inc

    @field_validator("issuer_tax_id")
    @classmethod
    def validate_tax_id(cls, v: str) -> str:
        if not validate_corporate_tax_id(v):
            raise ValueError(
                f"Invalid corporate B2B tax ID '{v}'. Must be a valid 14-digit Brazilian CNPJ (Modulo 11) "
                f"or Swiss UID matching '^CHE-\\d{{3}}\\.\\d{{3}}\\.\\d{{3}}(\\s?(MWST|TVA|IVA))?'."
            )
        return v.strip()

    @model_validator(mode="after")
    def validate_closure_incoterms_and_cents(self) -> "CommercialInvoiceInput":
        # 1. Compute scaled integer cents (* 10000)
        self.subtotal_amount_cents = int(self.subtotal_amount * 10000)
        self.tax_amount_cents = int(self.tax_amount * 10000)
        self.freight_amount_cents = int(self.freight_amount * 10000)
        self.insurance_amount_cents = int(self.insurance_amount * 10000)
        self.total_amount_cents = int(self.total_amount * 10000)

        # 2. CIF Incoterm requires explicit freight and insurance separation (> 0.0000)
        if self.incoterm == "CIF":
            if self.freight_amount <= Decimal("0.0000") or self.insurance_amount <= Decimal("0.0000"):
                raise ValueError(
                    f"CIF Incoterm requires explicit freight_amount and insurance_amount separation (> 0.0000). "
                    f"Got freight={self.freight_amount}, insurance={self.insurance_amount}."
                )

        # 3. Vertical math closure: total == subtotal + tax + freight + insurance (+/- 0.0001)
        expected_total = self.subtotal_amount + self.tax_amount + self.freight_amount + self.insurance_amount
        delta = abs(self.total_amount - expected_total)
        if delta > Decimal("0.0001"):
            raise ValueError(
                f"Invoice vertical closure failed: total_amount ({self.total_amount}) != "
                f"subtotal ({self.subtotal_amount}) + tax ({self.tax_amount}) + "
                f"freight ({self.freight_amount}) + insurance ({self.insurance_amount}) = {expected_total} "
                f"(drift: {delta} > 0.0001)."
            )

        # 4. If line items are present, verify sum of line totals matches subtotal_amount
        if self.lines:
            lines_sum = sum(line.total_price for line in self.lines)
            if abs(lines_sum - self.subtotal_amount) > Decimal("0.0001"):
                raise ValueError(
                    f"Line items total sum ({lines_sum}) does not match invoice subtotal_amount ({self.subtotal_amount})."
                )

        return self
