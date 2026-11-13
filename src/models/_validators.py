"""
Shared strict validation helpers and SQL/ODCS precision bounds for Comex V2 domain models.
Enforces zero-float prohibition, safe Decimal quantization, BIGINT/NUMERIC bounds,
YYYYMMDD calendar validation, and SHA-256 hex digest verification.
"""

from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re
from typing import Any, Optional

MAX_NUMERIC_19_4 = Decimal("999999999999999.9999")
MAX_NUMERIC_19_6 = Decimal("9999999999999.999999")
MAX_NUMERIC_7_4 = Decimal("999.9999")
MAX_BIGINT_CENTS = 9_223_372_036_854_775_807
MAX_CENTS_MONETARY_19_4 = Decimal("922337203685477.5807")
MAX_PG_INT = 2_147_483_647

SHA256_HEX_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")
CURRENCY_ISO3_PATTERN = re.compile(r"^[A-Z]{3}$")


def parse_strict_decimal(
    v: Any,
    *,
    scale: Decimal,
    max_value: Decimal,
    min_value: Optional[Decimal] = Decimal("0.0000"),
    strictly_positive: bool = False,
    strictly_less_than: Optional[Decimal] = None,
    allow_none: bool = False,
    field_label: str = "Value",
) -> Optional[Decimal]:
    """
    Safely parses str, int, or Decimal into a quantized Decimal without IEEE 754 float coercion.
    Explicitly rejects None (unless allow_none=True), bool, float, NaN, Infinity, malformed strings,
    and out-of-range inputs so Pydantic emits clean ValidationErrors.
    """
    if v is None:
        if allow_none:
            return None
        raise ValueError(f"{field_label} cannot be None.")

    if isinstance(v, bool):
        raise ValueError(f"{field_label} cannot be a boolean value.")

    if isinstance(v, float):
        raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")

    if not isinstance(v, (Decimal, int, str)):
        raise ValueError(
            f"{field_label} must be str, int, or Decimal; got {type(v).__name__}."
        )

    try:
        if isinstance(v, str):
            raw = v.strip()
            if not raw:
                raise ValueError(f"{field_label} cannot be empty.")
            dec = Decimal(raw)
        elif isinstance(v, Decimal):
            dec = v
        else:
            dec = Decimal(v)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"Invalid decimal value for {field_label}: {v!r}") from exc

    if not dec.is_finite():
        raise ValueError(
            f"Non-finite values (NaN, Infinity) are strictly prohibited for {field_label}."
        )

    # Guard against Decimal context InvalidOperation during quantize on huge exponents
    if dec > max_value + Decimal("1"):
        raise ValueError(
            f"{field_label} ({dec}) exceeds maximum allowed precision bound ({max_value})."
        )
    if min_value is not None and dec < min_value - Decimal("1"):
        raise ValueError(
            f"{field_label} ({dec}) is below minimum allowed bound ({min_value})."
        )

    try:
        quantized = dec.quantize(scale, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"Cannot quantize {field_label} ({v!r}) to scale {scale}.") from exc

    if strictly_positive and quantized <= Decimal("0"):
        raise ValueError(f"{field_label} must be strictly positive (> 0).")
    if min_value is not None and quantized < min_value:
        raise ValueError(f"{field_label} cannot be less than {min_value}.")
    if quantized > max_value:
        raise ValueError(
            f"{field_label} ({quantized}) exceeds maximum allowed bound ({max_value})."
        )
    if strictly_less_than is not None and quantized >= strictly_less_than:
        raise ValueError(
            f"{field_label} ({quantized}) must be strictly less than {strictly_less_than}."
        )

    return quantized


def reject_bool_int(
    v: Any,
    *,
    field_label: str,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
    allow_none: bool = False,
) -> Optional[int]:
    """Validates integer inputs while rejecting booleans, floats, and None (unless allowed)."""
    if v is None:
        if allow_none:
            return None
        raise ValueError(f"{field_label} cannot be None.")
    if isinstance(v, bool):
        raise ValueError(f"{field_label} cannot be a boolean value.")
    if isinstance(v, float):
        raise ValueError(f"{field_label} cannot be a float; pass an integer.")

    if isinstance(v, int):
        parsed = v
    elif isinstance(v, str):
        raw = v.strip()
        if not re.fullmatch(r"-?\d+", raw):
            raise ValueError(f"Invalid integer for {field_label}: {v!r}")
        parsed = int(raw)
    else:
        raise ValueError(f"{field_label} must be an integer; got {type(v).__name__}.")

    if min_value is not None and parsed < min_value:
        raise ValueError(f"{field_label} ({parsed}) must be >= {min_value}.")
    if max_value is not None and parsed > max_value:
        raise ValueError(f"{field_label} ({parsed}) must be <= {max_value}.")
    return parsed


def validate_date_id(v: Any) -> int:
    """Validates YYYYMMDD integer key as a real calendar date within [19000101, 29991231]."""
    parsed = reject_bool_int(
        v,
        field_label="date_id",
        min_value=19000101,
        max_value=29991231,
        allow_none=False,
    )
    assert parsed is not None
    try:
        datetime.strptime(f"{parsed:08d}", "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid calendar date_id {parsed}: {exc}") from exc
    return parsed


def date_from_date_id(date_id: int) -> date:
    """Converts a validated YYYYMMDD integer key to datetime.date."""
    return datetime.strptime(f"{date_id:08d}", "%Y%m%d").date()


def validate_sha256_hex(
    v: Any,
    *,
    field_label: str = "raw_payload_hash",
    allow_none: bool = False,
) -> Optional[str]:
    """Validates a 64-character hexadecimal SHA-256 digest and normalizes to lowercase."""
    if v is None:
        if allow_none:
            return None
        raise ValueError(f"{field_label} cannot be None.")
    if not isinstance(v, str):
        raise ValueError(f"{field_label} must be a 64-char hex string.")
    cleaned = v.strip()
    if not SHA256_HEX_PATTERN.match(cleaned):
        raise ValueError(
            f"Invalid {field_label} {v!r}: must be a 64-character hexadecimal SHA-256 digest."
        )
    return cleaned.lower()


def validate_non_blank_str(
    v: Any,
    *,
    field_label: str,
    max_length: Optional[int] = None,
    allow_none: bool = False,
) -> Optional[str]:
    """Strips a string and rejects empty/whitespace-only values and length overflows."""
    if v is None:
        if allow_none:
            return None
        raise ValueError(f"{field_label} cannot be None.")
    if not isinstance(v, str):
        raise ValueError(f"{field_label} must be a string.")
    cleaned = v.strip()
    if not cleaned:
        raise ValueError(f"{field_label} cannot be empty or whitespace-only.")
    if max_length is not None and len(cleaned) > max_length:
        raise ValueError(f"{field_label} exceeds maximum length of {max_length} characters.")
    return cleaned


def validate_iso_currency_3(v: Any, *, field_label: str = "currency_code") -> str:
    """Validates a 3-letter uppercase ISO 4217 currency code."""
    cleaned = validate_non_blank_str(v, field_label=field_label, max_length=3)
    assert cleaned is not None
    code = cleaned.upper()
    if not CURRENCY_ISO3_PATTERN.match(code):
        raise ValueError(
            f"Invalid {field_label} {v!r}: must be 3 uppercase ASCII letters (ISO 4217)."
        )
    return code
