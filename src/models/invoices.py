"""
Domain Models: International Commercial Invoices (ODCS v3.2.0 & PostgreSQL ACID Core)
Strictly enforces no IEEE 754 floats, DECIMAL(19,4) monetary precision, scaled cents (* 10000),
corporate B2B tax ID validation (CNPJ Modulo 11 and Swiss UID), Incoterms, and vertical math closure.
"""

from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import re
from typing import Any, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator

from src.models._validators import (
    MAX_BIGINT_CENTS,
    MAX_CENTS_MONETARY_19_4,
    MAX_NUMERIC_19_4,
    MAX_PG_INT,
    parse_strict_decimal,
    reject_bool_int,
    validate_non_blank_str,
    validate_sha256_hex,
)
from src.models.quarantine import QuarantineRecordInput  # noqa: F401

# Swiss UID pattern: CHE-XXX.XXX.XXX with optional MWST/TVA/IVA suffix
SWISS_UID_PATTERN = re.compile(r"^CHE-\d{3}\.\d{3}\.\d{3}(\s?(MWST|TVA|IVA))?$")
CNPJ_CHAR_PATTERN = re.compile(r"^[\d.\-/]+$")
MAX_TAX_ID_LENGTH = 50

VALID_CURRENCY_CODES = {"USD", "EUR", "CHF", "BRL"}
VALID_INCOTERMS = {"EXW", "FOB", "CIF", "DDP"}


def validate_cnpj(cnpj: str) -> bool:
    """
    Validates a Brazilian CNPJ (14 digits) using the official Modulo 11 algorithm.
    Enforces max length <= 50 characters and permits standard formatting ('00.000.000/0000-00')
    or unformatted digits.
    """
    if not cnpj or not isinstance(cnpj, str):
        return False
    cleaned = cnpj.strip()
    if not cleaned or len(cleaned) > MAX_TAX_ID_LENGTH:
        return False
    if not CNPJ_CHAR_PATTERN.match(cleaned):
        return False

    digits = [int(c) for c in cleaned if c.isdigit()]
    if len(digits) != 14 or len(set(digits)) == 1:
        return False

    # First verification digit
    w1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    s1 = sum(d * w for d, w in zip(digits[:12], w1, strict=True))
    r1 = s1 % 11
    d1 = 0 if r1 < 2 else 11 - r1
    if digits[12] != d1:
        return False

    # Second verification digit
    w2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    s2 = sum(d * w for d, w in zip(digits[:13], w2, strict=True))
    r2 = s2 % 11
    d2 = 0 if r2 < 2 else 11 - r2
    return digits[13] == d2


def validate_corporate_tax_id(tax_id: str) -> bool:
    """
    Validates B2B corporate tax identifiers (<= 50 chars):
    1. Brazilian CNPJ 14-digit Modulo 11
    2. Swiss UID conforming to ^CHE-\\d{3}\\.\\d{3}\\.\\d{3}(\\s?(MWST|TVA|IVA))?$
    Corporate tax IDs must be kept in cleartext for statutory audit.
    """
    if not tax_id or not isinstance(tax_id, str):
        return False
    cleaned = tax_id.strip()
    if not cleaned or len(cleaned) > MAX_TAX_ID_LENGTH:
        return False
    if SWISS_UID_PATTERN.match(cleaned):
        return True
    return validate_cnpj(cleaned)


def hash_natural_person_pii(value: str, salt: str) -> str:
    """
    Applies salted cryptographic SHA-256 hashing strictly to natural-person PII
    (individual CPF, personal contact names, personal emails, IPs) before persistence.
    Public B2B corporate tax IDs (CNPJ, Swiss UID) are excluded and kept in cleartext.
    Requires a non-empty, non-whitespace salt.
    """
    if not isinstance(salt, str) or not salt.strip():
        raise ValueError("Salt must be a non-empty string for PII hashing.")
    if not value or not isinstance(value, str) or not value.strip():
        return ""
    salted = f"{salt}:{value.strip()}".encode("utf-8")
    return hashlib.sha256(salted).hexdigest()


class CommercialInvoiceLineInput(BaseModel):
    """
    Individual line item on a commercial invoice.
    Strictly prohibits floats and enforces horizontal line math closure.
    """

    line_number: int = Field(..., ge=1, le=MAX_PG_INT, description="Sequential line number")
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
                copied = dict(data)
                copied["total_price"] = copied.get("line_total")
                return copied
        return data

    @field_validator("line_number", mode="before")
    @classmethod
    def validate_line_number(cls, v: Any) -> int:
        parsed = reject_bool_int(
            v,
            field_label="line_number",
            min_value=1,
            max_value=MAX_PG_INT,
            allow_none=False,
        )
        assert parsed is not None
        return parsed

    @field_validator("description", mode="before")
    @classmethod
    def validate_description(cls, v: Any) -> str:
        cleaned = validate_non_blank_str(v, field_label="description")
        assert cleaned is not None
        return cleaned

    @field_validator("quantity", "unit_price", "total_price", mode="before")
    @classmethod
    def reject_float_and_quantize(cls, v: Any) -> Decimal:
        parsed = parse_strict_decimal(
            v,
            scale=Decimal("0.0001"),
            max_value=MAX_NUMERIC_19_4,
            min_value=Decimal("0.0000"),
            allow_none=False,
            field_label="Monetary/quantity amount",
        )
        assert parsed is not None
        return parsed

    @model_validator(mode="after")
    def verify_horizontal_line_math(self) -> "CommercialInvoiceLineInput":
        product = self.quantity * self.unit_price
        if product > MAX_NUMERIC_19_4 + Decimal("1"):
            raise ValueError(
                f"Line {self.line_number} horizontal math failed: product ({product}) "
                f"exceeds NUMERIC(19,4) bound ({MAX_NUMERIC_19_4})."
            )
        try:
            expected = product.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        except InvalidOperation as exc:
            raise ValueError(
                f"Line {self.line_number} horizontal math failed: cannot quantize product."
            ) from exc

        if abs(self.total_price - expected) > Decimal("0.0001"):
            raise ValueError(
                f"Line {self.line_number} horizontal math failed: total_price ({self.total_price}) != "
                f"quantity ({self.quantity}) * unit_price ({self.unit_price}) = {expected}"
            )
        return self


class CommercialInvoiceInput(BaseModel):
    """
    Operational International Commercial Invoice Header conforming to ODCS v3.2.0.
    Enforces DECIMAL(19,4) precision, scaled cents (* 10000) within BIGINT bounds,
    B2B corporate tax ID validation, Incoterm rules, and vertical math closure.
    """

    invoice_id: Optional[UUID] = None
    issuer_tax_id: str = Field(..., min_length=1, max_length=50, description="Corporate B2B Tax ID (CNPJ or Swiss UID) in cleartext")
    document_number: str = Field(..., min_length=1, max_length=100, description="Commercial Invoice document serial")
    issue_date: date = Field(..., description="Official invoice issuance date")
    currency_code: str = Field(..., min_length=3, max_length=3, description="ISO 4217 Currency (USD, EUR, CHF, BRL)")
    subtotal_amount: Decimal = Field(..., description="Merchandise net amount")
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
        parsed = parse_strict_decimal(
            v,
            scale=Decimal("0.0001"),
            max_value=MAX_CENTS_MONETARY_19_4,
            min_value=Decimal("0.0000"),
            allow_none=False,
            field_label="Monetary amount",
        )
        assert parsed is not None
        return parsed

    @field_validator(
        "subtotal_amount_cents",
        "tax_amount_cents",
        "freight_amount_cents",
        "insurance_amount_cents",
        "total_amount_cents",
        mode="before",
    )
    @classmethod
    def validate_cents_bounds(cls, v: Any) -> Optional[int]:
        return reject_bool_int(
            v,
            field_label="Scaled cents amount",
            min_value=0,
            max_value=MAX_BIGINT_CENTS,
            allow_none=True,
        )

    @field_validator("extraction_confidence", mode="before")
    @classmethod
    def reject_float_and_quantize_confidence(cls, v: Any) -> Decimal:
        parsed = parse_strict_decimal(
            v,
            scale=Decimal("0.0001"),
            max_value=Decimal("1.0000"),
            min_value=Decimal("0.0000"),
            allow_none=False,
            field_label="extraction_confidence",
        )
        assert parsed is not None
        return parsed

    @field_validator("document_number", mode="before")
    @classmethod
    def validate_document_number(cls, v: Any) -> str:
        cleaned = validate_non_blank_str(v, field_label="document_number", max_length=100)
        assert cleaned is not None
        return cleaned

    @field_validator("raw_payload_hash", mode="before")
    @classmethod
    def validate_raw_payload_hash(cls, v: Any) -> Optional[str]:
        return validate_sha256_hex(v, field_label="raw_payload_hash", allow_none=True)

    @field_validator("mongo_raw_id", mode="before")
    @classmethod
    def validate_mongo_raw_id(cls, v: Any) -> Optional[str]:
        return validate_non_blank_str(
            v,
            field_label="mongo_raw_id",
            max_length=64,
            allow_none=True,
        )

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

    @field_validator("issuer_tax_id", mode="before")
    @classmethod
    def normalize_tax_id(cls, v: Any) -> str:
        cleaned = validate_non_blank_str(v, field_label="issuer_tax_id", max_length=MAX_TAX_ID_LENGTH)
        assert cleaned is not None
        return cleaned

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
        # 1. Compute and validate scaled integer cents (* 10000) within BIGINT bounds
        for amount_field, cents_field in (
            ("subtotal_amount", "subtotal_amount_cents"),
            ("tax_amount", "tax_amount_cents"),
            ("freight_amount", "freight_amount_cents"),
            ("insurance_amount", "insurance_amount_cents"),
            ("total_amount", "total_amount_cents"),
        ):
            computed_cents = int((getattr(self, amount_field) * 10000).to_integral_exact())
            if computed_cents > MAX_BIGINT_CENTS:
                raise ValueError(
                    f"{cents_field} ({computed_cents}) exceeds PostgreSQL BIGINT limit ({MAX_BIGINT_CENTS})."
                )
            declared_cents = getattr(self, cents_field)
            if declared_cents is not None and declared_cents != computed_cents:
                raise ValueError(
                    f"{cents_field} mismatch: declared {declared_cents} != expected {computed_cents}."
                )
            setattr(self, cents_field, computed_cents)

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

        # 4. If line items are present, reject duplicate line_number and verify sum matches subtotal_amount
        if self.lines:
            seen_line_numbers: set[int] = set()
            for line in self.lines:
                if line.line_number in seen_line_numbers:
                    raise ValueError(
                        f"Duplicate line_number {line.line_number} detected in commercial invoice lines."
                    )
                seen_line_numbers.add(line.line_number)

            lines_sum = sum(
                (line.total_price for line in self.lines),
                Decimal("0.0000"),
            ).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
            if abs(lines_sum - self.subtotal_amount) > Decimal("0.0001"):
                raise ValueError(
                    f"Line items total sum ({lines_sum}) does not match invoice subtotal_amount ({self.subtotal_amount})."
                )

        return self
