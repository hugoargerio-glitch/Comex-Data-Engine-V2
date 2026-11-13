"""
Domain Models: Customs Operations, PTAX Time-Series, Dimensions & Double-Entry Fiduciary Ledger
(ODCS v3.2.0 & PostgreSQL 16 ACID Fiduciary Ledger / DuckDB Parquet Star Schema)

Enforces zero-float prohibition, DECIMAL(19,4) monetary precision, DECIMAL(19,6) FX rates,
LOCF forward-fill without lookahead bias, delimiter-safe operation_hash_id,
immutable frozen double-entry bookkeeping with statutory reversal, SWIFT split builders,
and Brazilian customs cascading taxes (CPC 16 / ICMS por dentro).
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import re
from typing import Any, List, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.models._validators import (
    MAX_NUMERIC_19_4,
    MAX_NUMERIC_19_6,
    MAX_NUMERIC_7_4,
    MAX_PG_INT,
    date_from_date_id,
    parse_strict_decimal,
    reject_bool_int,
    validate_date_id,
    validate_iso_currency_3,
    validate_non_blank_str,
    validate_sha256_hex,
)
from src.models.invoices import MAX_TAX_ID_LENGTH, validate_cnpj
from src.models.quarantine import (  # noqa: F401
    QuarantineRecordInput,
    VALID_QUARANTINE_REVIEW_STATUSES,
    VALID_QUARANTINE_SOURCE_TYPES,
)

VALID_PTAX_CURRENCIES = {"USD", "EUR", "CHF"}
VALID_OPERATION_TYPES = {"IMPORT", "EXPORT"}
VALID_ENTRY_STATUSES = {"POSTED", "REVERSED"}
UF_CODE_PATTERN = re.compile(r"^[A-Z]{2}$")
NCM_CODE_PATTERN = re.compile(r"^\d{8}$")
HS_CODE_PATTERN = re.compile(r"^\d{6}$")
ISO3_COUNTRY_PATTERN = re.compile(r"^[A-Z]{3}$")


class PTAXRateInput(BaseModel):
    """
    Daily official foreign exchange rate dimension conforming to dim_currency_ptax.
    Enforces DECIMAL(19,6) precision, real calendar date_id, prohibits lookahead bias,
    and supports LOCF.
    """

    date_id: int = Field(..., description="Integer date key in YYYYMMDD format", ge=19000101, le=29991231)
    currency_code: str = Field(..., min_length=3, max_length=3, description="ISO 4217 currency (USD, EUR, CHF)")
    ptax_buy_rate: Decimal = Field(..., gt=Decimal("0.000000"), description="Official BACEN Olinda buying rate")
    ptax_sell_rate: Decimal = Field(..., gt=Decimal("0.000000"), description="Official BACEN Olinda selling rate")
    is_business_day: bool = Field(..., description="True if BACEN quotation published on this date")
    is_interpolated_locf: bool = Field(default=False, description="True if forward-filled via LOCF from previous business day")

    @field_validator("date_id", mode="before")
    @classmethod
    def validate_calendar_date_id(cls, v: Any) -> int:
        return validate_date_id(v)

    @field_validator("ptax_buy_rate", "ptax_sell_rate", mode="before")
    @classmethod
    def reject_float_and_quantize_fx(cls, v: Any) -> Decimal:
        parsed = parse_strict_decimal(
            v,
            scale=Decimal("0.000001"),
            max_value=MAX_NUMERIC_19_6,
            min_value=Decimal("0.000000"),
            strictly_positive=True,
            allow_none=False,
            field_label="Exchange rate",
        )
        assert parsed is not None
        return parsed

    @field_validator("currency_code")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        code = v.upper().strip()
        if code not in VALID_PTAX_CURRENCIES:
            raise ValueError(f"Invalid PTAX currency '{v}'. Permitted values: {sorted(VALID_PTAX_CURRENCIES)}")
        return code

    @model_validator(mode="after")
    def validate_no_lookahead_and_spread(self) -> "PTAXRateInput":
        rate_date = date_from_date_id(self.date_id)

        today = datetime.now(timezone.utc).date()
        if rate_date > today:
            raise ValueError(
                f"Lookahead bias prohibited: exchange rate date ({rate_date}) cannot be in the future (today={today})."
            )

        if self.ptax_sell_rate < self.ptax_buy_rate:
            raise ValueError(
                f"PTAX sell rate ({self.ptax_sell_rate}) cannot be less than buy rate ({self.ptax_buy_rate})."
            )
        return self


def apply_locf_forward_fill(
    rates: List[PTAXRateInput],
    start_date: date,
    end_date: date,
    currency_code: str,
) -> List[PTAXRateInput]:
    """
    Imputes weekend and non-business holiday FX rates strictly using Last Observation
    Carried Forward (LOCF) from the immediately preceding business day.
    Strictly prohibits lookahead bias by never backfilling from future quotations.
    """
    code = currency_code.upper().strip()
    if code not in VALID_PTAX_CURRENCIES:
        raise ValueError(f"Invalid currency '{currency_code}'. Permitted: {sorted(VALID_PTAX_CURRENCIES)}")
    if start_date > end_date:
        raise ValueError(f"start_date ({start_date}) must be <= end_date ({end_date}).")

    existing_by_date = {r.date_id: r for r in rates if r.currency_code == code}
    sorted_known = sorted(
        [r for r in rates if r.currency_code == code],
        key=lambda x: x.date_id,
    )

    result: List[PTAXRateInput] = []
    current_date = start_date
    last_known: Optional[PTAXRateInput] = None

    for r in sorted_known:
        r_date = date_from_date_id(r.date_id)
        if r_date <= start_date:
            last_known = r
        else:
            break

    while current_date <= end_date:
        curr_id = current_date.year * 10000 + current_date.month * 100 + current_date.day
        if curr_id in existing_by_date:
            obs = existing_by_date[curr_id]
            last_known = obs
            result.append(obs)
        else:
            if last_known is None:
                raise ValueError(
                    f"Lookahead violation: cannot forward-fill rate for {current_date} "
                    f"because no preceding business day observation exists for {code}."
                )
            interpolated = PTAXRateInput(
                date_id=curr_id,
                currency_code=code,
                ptax_buy_rate=last_known.ptax_buy_rate,
                ptax_sell_rate=last_known.ptax_sell_rate,
                is_business_day=False,
                is_interpolated_locf=True,
            )
            result.append(interpolated)

        current_date += timedelta(days=1)

    return result


def compute_operation_hash_id(
    ano: int,
    mes: int,
    tipo_op: str,
    co_ncm: str,
    co_pais: int,
    sg_uf: str,
    co_via: int,
    co_urf: int,
) -> str:
    """
    Computes deterministic delimiter-safe SHA-256 natural key hash for customs operations facts:
    SHA-256(ano|mes|tipo_op|co_ncm|co_pais|sg_uf|co_via|co_urf).
    Guarantees collision-free analytical idempotency during re-runs.
    """
    canonical_string = (
        f"{ano}|{mes:02d}|{tipo_op.upper().strip()}|{co_ncm.strip()}|"
        f"{co_pais}|{sg_uf.upper().strip()}|{co_via}|{co_urf}"
    )
    return hashlib.sha256(canonical_string.encode("utf-8")).hexdigest()


class ComexOperationInput(BaseModel):
    """
    Analytical grain representing monthly aggregated customs declarations from MDIC.
    Conforms to fact_comex_operations (Apache Parquet Hive / DuckDB).
    """

    operation_hash_id: Optional[str] = Field(None, max_length=64, description="Deterministic SHA-256 key")
    date_id: int = Field(..., description="Date key (YYYYMM01 representing the statistical reference month)")
    operation_type: str = Field(..., description="Customs flow direction: IMPORT or EXPORT")
    ncm_code: str = Field(..., pattern=r"^\d{8}$", description="Mercosul 8-digit NCM tariff code")
    country_code: int = Field(..., ge=1, le=MAX_PG_INT, description="Partner country code (MDIC / ISO 3166)")
    uf_code: str = Field(..., min_length=2, max_length=2, description="Brazilian federated state (UF)")
    modal_code: int = Field(..., ge=1, le=MAX_PG_INT, description="Transport modal code (e.g. 1=Maritime, 4=Air, 7=Road)")
    urf_code: int = Field(..., ge=1, le=MAX_PG_INT, description="Customs Clearance Unit (URF)")

    statistical_quantity: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"))
    net_weight_kg: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"))
    fob_amount_usd: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"))
    freight_amount_usd: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"))
    insurance_amount_usd: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"))
    cif_amount_usd: Decimal = Field(..., ge=Decimal("0.0000"))
    landed_cost_brl: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"))

    @field_validator("operation_hash_id", mode="before")
    @classmethod
    def validate_operation_hash(cls, v: Any) -> Optional[str]:
        return validate_sha256_hex(v, field_label="operation_hash_id", allow_none=True)

    @field_validator("date_id", mode="before")
    @classmethod
    def validate_calendar_date_id(cls, v: Any) -> int:
        return validate_date_id(v)

    @field_validator("country_code", "modal_code", "urf_code", mode="before")
    @classmethod
    def validate_positive_codes(cls, v: Any) -> int:
        parsed = reject_bool_int(
            v,
            field_label="Customs numeric code",
            min_value=1,
            max_value=MAX_PG_INT,
            allow_none=False,
        )
        assert parsed is not None
        return parsed

    @field_validator(
        "statistical_quantity",
        "net_weight_kg",
        "fob_amount_usd",
        "freight_amount_usd",
        "insurance_amount_usd",
        "cif_amount_usd",
        "landed_cost_brl",
        mode="before",
    )
    @classmethod
    def reject_float_and_quantize(cls, v: Any) -> Decimal:
        parsed = parse_strict_decimal(
            v,
            scale=Decimal("0.0001"),
            max_value=MAX_NUMERIC_19_4,
            min_value=Decimal("0.0000"),
            allow_none=False,
            field_label="Comex operation amount",
        )
        assert parsed is not None
        return parsed

    @field_validator("operation_type")
    @classmethod
    def validate_op_type(cls, v: str) -> str:
        op = v.upper().strip()
        if op not in VALID_OPERATION_TYPES:
            raise ValueError(f"Invalid operation_type '{v}'. Permitted values: {sorted(VALID_OPERATION_TYPES)}")
        return op

    @field_validator("uf_code")
    @classmethod
    def validate_uf(cls, v: str) -> str:
        uf = v.upper().strip()
        if not UF_CODE_PATTERN.match(uf):
            raise ValueError(f"Invalid uf_code '{v}': must be 2 uppercase ASCII letters.")
        return uf

    @model_validator(mode="after")
    def validate_cif_greater_fob_and_hash(self) -> "ComexOperationInput":
        if self.cif_amount_usd < self.fob_amount_usd:
            raise ValueError(
                f"cif_amount_usd ({self.cif_amount_usd}) cannot be less than fob_amount_usd ({self.fob_amount_usd})."
            )

        ano = self.date_id // 10000
        mes = (self.date_id % 10000) // 100
        computed_hash = compute_operation_hash_id(
            ano=ano,
            mes=mes,
            tipo_op=self.operation_type,
            co_ncm=self.ncm_code,
            co_pais=self.country_code,
            sg_uf=self.uf_code,
            co_via=self.modal_code,
            co_urf=self.urf_code,
        )

        if self.operation_hash_id is None:
            self.operation_hash_id = computed_hash
        elif self.operation_hash_id != computed_hash:
            raise ValueError(
                f"operation_hash_id mismatch: declared {self.operation_hash_id} != computed {computed_hash}"
            )

        return self


class MerchandiseNCMInput(BaseModel):
    """
    Mercosul Common Nomenclature (NCM) merchandise dimension conforming to
    dim_merchandise_ncm (DDL & ODCS v3.2.0).
    """

    ncm_code: str = Field(..., pattern=r"^\d{8}$", description="8-digit Mercosul NCM tariff code")
    hs_code: str = Field(..., pattern=r"^\d{6}$", description="6-digit Harmonized System code")
    description: str = Field(..., min_length=1, description="Official Portuguese description of commodity")
    unit_measure: str = Field(default="KG", min_length=1, max_length=30, description="Standard statistical unit")
    created_at: Optional[datetime] = Field(default=None, description="UTC system creation timestamp")

    @field_validator("ncm_code", "hs_code", mode="before")
    @classmethod
    def strip_codes(cls, v: Any) -> str:
        cleaned = validate_non_blank_str(v, field_label="NCM/HS code")
        assert cleaned is not None
        return cleaned

    @field_validator("description", mode="before")
    @classmethod
    def validate_description(cls, v: Any) -> str:
        cleaned = validate_non_blank_str(v, field_label="description")
        assert cleaned is not None
        return cleaned

    @field_validator("unit_measure", mode="before")
    @classmethod
    def validate_unit_measure(cls, v: Any) -> str:
        cleaned = validate_non_blank_str(v, field_label="unit_measure", max_length=30)
        assert cleaned is not None
        return cleaned

    @model_validator(mode="after")
    def validate_hs_prefix_consistency(self) -> "MerchandiseNCMInput":
        if self.hs_code != self.ncm_code[:6]:
            raise ValueError(
                f"hs_code ({self.hs_code}) must match the first 6 digits of ncm_code ({self.ncm_code})."
            )
        return self


class PartnerCountryInput(BaseModel):
    """
    Partner country dimension conforming to dim_partner_country (DDL & ODCS v3.2.0).
    """

    country_code: int = Field(..., ge=1, le=MAX_PG_INT, description="Official MDIC partner country numeric ID")
    country_iso3: str = Field(..., min_length=3, max_length=3, description="ISO 3166-1 alpha-3 country code")
    country_name: str = Field(..., min_length=1, max_length=100, description="Standard Portuguese country name")
    economic_block: Optional[str] = Field(default=None, max_length=50, description="Economic or trade bloc")
    created_at: Optional[datetime] = Field(default=None, description="UTC system creation timestamp")

    @field_validator("country_code", mode="before")
    @classmethod
    def validate_country_code(cls, v: Any) -> int:
        parsed = reject_bool_int(
            v,
            field_label="country_code",
            min_value=1,
            max_value=MAX_PG_INT,
            allow_none=False,
        )
        assert parsed is not None
        return parsed

    @field_validator("country_iso3")
    @classmethod
    def validate_iso3(cls, v: str) -> str:
        code = v.upper().strip()
        if not ISO3_COUNTRY_PATTERN.match(code):
            raise ValueError(
                f"Invalid country_iso3 '{v}': must be 3 uppercase ASCII letters (ISO 3166-1 alpha-3)."
            )
        return code

    @field_validator("country_name", mode="before")
    @classmethod
    def validate_country_name(cls, v: Any) -> str:
        cleaned = validate_non_blank_str(v, field_label="country_name", max_length=100)
        assert cleaned is not None
        return cleaned

    @field_validator("economic_block", mode="before")
    @classmethod
    def validate_economic_block(cls, v: Any) -> Optional[str]:
        if v is None:
            return None
        if not isinstance(v, str):
            raise ValueError("economic_block must be a string or None.")
        cleaned = v.strip()
        if not cleaned:
            return None
        if len(cleaned) > 50:
            raise ValueError("economic_block exceeds maximum length of 50 characters.")
        return cleaned


class JournalLineInput(BaseModel):
    """
    Immutable double-entry ledger line item in functional currency (BRL).
    Enforces frozen immutability, DECIMAL(19,4) amount precision, DECIMAL(19,6) FX rates,
    non-negativity, and mutually exclusive debit or credit.
    """

    model_config = ConfigDict(frozen=True)

    line_id: Optional[UUID] = None
    entry_id: Optional[UUID] = None
    account_code: str = Field(..., min_length=1, max_length=50, description="Chart of Accounts canonical code")
    debit_amount: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"))
    credit_amount: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"))
    currency_code: str = Field(..., min_length=3, max_length=3, description="Original transaction currency")
    original_currency_amount: Decimal = Field(..., ge=Decimal("0.0000"))
    exchange_rate_applied: Decimal = Field(default=Decimal("1.000000"), gt=Decimal("0.000000"))
    line_order: int = Field(..., ge=1, le=MAX_PG_INT)

    @field_validator("account_code", mode="before")
    @classmethod
    def validate_account_code(cls, v: Any) -> str:
        cleaned = validate_non_blank_str(v, field_label="account_code", max_length=50)
        assert cleaned is not None
        return cleaned

    @field_validator("currency_code")
    @classmethod
    def validate_currency_code(cls, v: str) -> str:
        return validate_iso_currency_3(v, field_label="currency_code")

    @field_validator("line_order", mode="before")
    @classmethod
    def validate_line_order(cls, v: Any) -> int:
        parsed = reject_bool_int(
            v,
            field_label="line_order",
            min_value=1,
            max_value=MAX_PG_INT,
            allow_none=False,
        )
        assert parsed is not None
        return parsed

    @field_validator("debit_amount", "credit_amount", "original_currency_amount", mode="before")
    @classmethod
    def reject_float_and_quantize_amount(cls, v: Any) -> Decimal:
        parsed = parse_strict_decimal(
            v,
            scale=Decimal("0.0001"),
            max_value=MAX_NUMERIC_19_4,
            min_value=Decimal("0.0000"),
            allow_none=False,
            field_label="Ledger line amount",
        )
        assert parsed is not None
        return parsed

    @field_validator("exchange_rate_applied", mode="before")
    @classmethod
    def reject_float_and_quantize_rate(cls, v: Any) -> Decimal:
        parsed = parse_strict_decimal(
            v,
            scale=Decimal("0.000001"),
            max_value=MAX_NUMERIC_19_6,
            min_value=Decimal("0.000000"),
            strictly_positive=True,
            allow_none=False,
            field_label="exchange_rate_applied",
        )
        assert parsed is not None
        return parsed

    @model_validator(mode="after")
    def validate_exclusive_debit_credit(self) -> "JournalLineInput":
        is_debit = self.debit_amount > Decimal("0.0000")
        is_credit = self.credit_amount > Decimal("0.0000")

        if is_debit and is_credit:
            raise ValueError(
                f"Ledger line cannot have both debit ({self.debit_amount}) and credit ({self.credit_amount}) > 0."
            )
        if not is_debit and not is_credit:
            raise ValueError("Ledger line must have either debit > 0 or credit > 0.")
        if self.original_currency_amount <= Decimal("0.0000"):
            raise ValueError("original_currency_amount must be strictly positive (> 0.0000).")
        return self


class JournalEntryInput(BaseModel):
    """
    Immutable Append-Only Canonical Journal Entry Header (PostgreSQL 16 ACID Fiduciary Ledger).
    Enforces frozen immutability, unique line_order, minimum 2 lines, zero-sum invariant,
    and bidirectional REVERSED <-> reversed_from_entry_id consistency.
    """

    model_config = ConfigDict(frozen=True)

    entry_id: UUID = Field(default_factory=uuid4)
    transaction_date: date = Field(..., description="Date financial event executed")
    competence_date: date = Field(..., description="Accounting competence date matching invoice")
    reference_document_id: Optional[str] = Field(None, max_length=100)
    description: str = Field(..., min_length=1)
    status: str = Field(default="POSTED", description="Posting lifecycle status: POSTED or REVERSED")
    reversed_from_entry_id: Optional[UUID] = None
    lines: List[JournalLineInput] = Field(..., min_length=2)

    @model_validator(mode="before")
    @classmethod
    def default_none_entry_id(cls, data: Any) -> Any:
        if isinstance(data, dict) and data.get("entry_id") is None and "entry_id" in data:
            copied = dict(data)
            copied["entry_id"] = uuid4()
            return copied
        return data

    @field_validator("reference_document_id", mode="before")
    @classmethod
    def validate_ref_doc_id(cls, v: Any) -> Optional[str]:
        return validate_non_blank_str(
            v,
            field_label="reference_document_id",
            max_length=100,
            allow_none=True,
        )

    @field_validator("description", mode="before")
    @classmethod
    def validate_description(cls, v: Any) -> str:
        cleaned = validate_non_blank_str(v, field_label="description")
        assert cleaned is not None
        return cleaned

    @field_validator("status", mode="before")
    @classmethod
    def validate_status(cls, v: Any) -> str:
        cleaned = validate_non_blank_str(v, field_label="status", max_length=20)
        assert cleaned is not None
        st = cleaned.upper()
        if st not in VALID_ENTRY_STATUSES:
            raise ValueError(f"Invalid status '{v}'. Permitted values: {sorted(VALID_ENTRY_STATUSES)}")
        return st

    @model_validator(mode="after")
    def validate_zero_sum_invariant(self) -> "JournalEntryInput":
        if self.status == "REVERSED":
            if self.reversed_from_entry_id is None:
                raise ValueError(
                    "REVERSED journal entry requires reversed_from_entry_id to reference the original entry."
                )
            if self.reversed_from_entry_id == self.entry_id:
                raise ValueError(
                    f"REVERSED journal entry cannot reference itself (entry_id={self.entry_id})."
                )
        elif self.reversed_from_entry_id is not None:
            raise ValueError(
                f"Journal entry with status '{self.status}' cannot set reversed_from_entry_id."
            )

        if len(self.lines) < 2:
            raise ValueError(f"Double-entry bookkeeping requires at least 2 lines; got {len(self.lines)}.")

        seen_orders: set[int] = set()
        for line in self.lines:
            if line.line_order in seen_orders:
                raise ValueError(
                    f"Duplicate line_order {line.line_order} detected in JournalEntryInput lines."
                )
            seen_orders.add(line.line_order)

        total_debits = sum(
            (line.debit_amount for line in self.lines),
            Decimal("0.0000"),
        ).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        total_credits = sum(
            (line.credit_amount for line in self.lines),
            Decimal("0.0000"),
        ).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

        imbalance = abs(total_debits - total_credits)
        if imbalance > Decimal("0.0001"):
            raise ValueError(
                f"Double-entry zero-sum invariant violated: SUM(debits) = {total_debits} != "
                f"SUM(credits) = {total_credits} (drift: {imbalance} > 0.0001)."
            )

        updated_lines: List[JournalLineInput] = []
        changed = False
        for line in self.lines:
            if line.entry_id is None:
                updated_lines.append(line.model_copy(update={"entry_id": self.entry_id}))
                changed = True
            elif line.entry_id != self.entry_id:
                raise ValueError(
                    f"Child line entry_id ({line.entry_id}) does not match parent entry_id ({self.entry_id})."
                )
            else:
                updated_lines.append(line)

        if changed:
            object.__setattr__(self, "lines", updated_lines)

        return self


def create_reversal_entry(
    original_entry: JournalEntryInput,
    reversal_date: date,
    reason: str,
) -> JournalEntryInput:
    """
    Generates an auditable statutory compensating entry (Reversal Entry)
    with inverted debit/credit lines, preserving append-only ledger immutability.
    Forbids reversing an entry whose status is already REVERSED.
    """
    if original_entry.status == "REVERSED":
        raise ValueError(
            f"Cannot reverse journal entry {original_entry.entry_id}: entry is already REVERSED."
        )
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("Reversal reason must be a non-empty string.")

    new_entry_id = uuid4()
    reversed_lines: List[JournalLineInput] = []

    for idx, line in enumerate(original_entry.lines, start=1):
        reversed_line = JournalLineInput(
            line_id=uuid4(),
            entry_id=new_entry_id,
            account_code=line.account_code,
            debit_amount=line.credit_amount,
            credit_amount=line.debit_amount,
            currency_code=line.currency_code,
            original_currency_amount=line.original_currency_amount,
            exchange_rate_applied=line.exchange_rate_applied,
            line_order=idx,
        )
        reversed_lines.append(reversed_line)

    return JournalEntryInput(
        entry_id=new_entry_id,
        transaction_date=reversal_date,
        competence_date=original_entry.competence_date,
        reference_document_id=original_entry.reference_document_id,
        description=f"REVERSAL: {reason.strip()} (Ref Entry: {original_entry.entry_id})",
        status="REVERSED",
        reversed_from_entry_id=original_entry.entry_id,
        lines=reversed_lines,
    )


def build_fx_liquidation_entry(
    reference_document_id: str,
    settlement_date: date,
    competence_date: date,
    foreign_amount: Decimal,
    currency_code: str,
    competence_rate: Decimal,
    spot_rate: Decimal,
    cash_account: str = "1.1.1.01",
    receivable_account: str = "1.1.2.01",
    fx_gain_account: str = "3.1.2.01",
    fx_loss_account: str = "4.1.3.01",
) -> JournalEntryInput:
    """
    Builds a balanced double-entry settlement entry segregating Competence (t1 PTAX)
    and Liquidation (t2 Spot), booking realized exchange variance to FX Gain or FX Loss.
    """
    v_estrang = parse_strict_decimal(
        foreign_amount,
        scale=Decimal("0.0001"),
        max_value=MAX_NUMERIC_19_4,
        min_value=Decimal("0.0000"),
        strictly_positive=True,
        allow_none=False,
        field_label="foreign_amount",
    )
    rate_t1 = parse_strict_decimal(
        competence_rate,
        scale=Decimal("0.000001"),
        max_value=MAX_NUMERIC_19_6,
        min_value=Decimal("0.000000"),
        strictly_positive=True,
        allow_none=False,
        field_label="competence_rate",
    )
    rate_t2 = parse_strict_decimal(
        spot_rate,
        scale=Decimal("0.000001"),
        max_value=MAX_NUMERIC_19_6,
        min_value=Decimal("0.000000"),
        strictly_positive=True,
        allow_none=False,
        field_label="spot_rate",
    )
    assert v_estrang is not None and rate_t1 is not None and rate_t2 is not None
    curr = validate_iso_currency_3(currency_code, field_label="currency_code")

    cash_received = (v_estrang * rate_t2).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    receivable_closed = (v_estrang * rate_t1).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    fx_variance = cash_received - receivable_closed

    new_entry_id = uuid4()
    lines: List[JournalLineInput] = []

    # Line 1: Debit Cash with net received at Spot
    lines.append(
        JournalLineInput(
            line_id=uuid4(),
            entry_id=new_entry_id,
            account_code=cash_account,
            debit_amount=cash_received,
            credit_amount=Decimal("0.0000"),
            currency_code=curr,
            original_currency_amount=v_estrang,
            exchange_rate_applied=rate_t2,
            line_order=1,
        )
    )

    if fx_variance > Decimal("0.0000"):
        lines.append(
            JournalLineInput(
                line_id=uuid4(),
                entry_id=new_entry_id,
                account_code=receivable_account,
                debit_amount=Decimal("0.0000"),
                credit_amount=receivable_closed,
                currency_code=curr,
                original_currency_amount=v_estrang,
                exchange_rate_applied=rate_t1,
                line_order=2,
            )
        )
        lines.append(
            JournalLineInput(
                line_id=uuid4(),
                entry_id=new_entry_id,
                account_code=fx_gain_account,
                debit_amount=Decimal("0.0000"),
                credit_amount=fx_variance,
                currency_code="BRL",
                original_currency_amount=fx_variance,
                exchange_rate_applied=Decimal("1.000000"),
                line_order=3,
            )
        )
    elif fx_variance < Decimal("0.0000"):
        loss_amount = abs(fx_variance)
        lines.append(
            JournalLineInput(
                line_id=uuid4(),
                entry_id=new_entry_id,
                account_code=fx_loss_account,
                debit_amount=loss_amount,
                credit_amount=Decimal("0.0000"),
                currency_code="BRL",
                original_currency_amount=loss_amount,
                exchange_rate_applied=Decimal("1.000000"),
                line_order=2,
            )
        )
        lines.append(
            JournalLineInput(
                line_id=uuid4(),
                entry_id=new_entry_id,
                account_code=receivable_account,
                debit_amount=Decimal("0.0000"),
                credit_amount=receivable_closed,
                currency_code=curr,
                original_currency_amount=v_estrang,
                exchange_rate_applied=rate_t1,
                line_order=3,
            )
        )
    else:
        lines.append(
            JournalLineInput(
                line_id=uuid4(),
                entry_id=new_entry_id,
                account_code=receivable_account,
                debit_amount=Decimal("0.0000"),
                credit_amount=receivable_closed,
                currency_code=curr,
                original_currency_amount=v_estrang,
                exchange_rate_applied=rate_t1,
                line_order=2,
            )
        )

    return JournalEntryInput(
        entry_id=new_entry_id,
        transaction_date=settlement_date,
        competence_date=competence_date,
        reference_document_id=reference_document_id,
        description=f"Liquidação de Câmbio Fatura {reference_document_id} (Spot={rate_t2}, PTAX={rate_t1})",
        status="POSTED",
        lines=lines,
    )


def build_swift_fee_split_entry(
    reference_document_id: str,
    transaction_date: date,
    competence_date: date,
    gross_amount: Decimal,
    fee_amount: Decimal,
    currency_code: str,
    exchange_rate: Decimal = Decimal("1.000000"),
    cash_account: str = "1.1.1.01",
    fee_account: str = "4.1.2.01",
    ar_account: str = "1.1.2.01",
) -> JournalEntryInput:
    """
    Generates a Split Journal Entry for SWIFT wire reconciliation under SHA/BEN/OUR clauses:
    Debits Cash (net received) and optional SWIFT Bank Fees (when > 0) against
    Accounts Receivable (gross amount). Cleanly supports zero-fee OUR wires (fee_amount == 0)
    by omitting the zero-value fee line while maintaining sequential line_order and balance.
    """
    gross = parse_strict_decimal(
        gross_amount,
        scale=Decimal("0.0001"),
        max_value=MAX_NUMERIC_19_4,
        min_value=Decimal("0.0000"),
        strictly_positive=True,
        allow_none=False,
        field_label="gross_amount",
    )
    fee = parse_strict_decimal(
        fee_amount,
        scale=Decimal("0.0001"),
        max_value=MAX_NUMERIC_19_4,
        min_value=Decimal("0.0000"),
        allow_none=False,
        field_label="fee_amount",
    )
    rate = parse_strict_decimal(
        exchange_rate,
        scale=Decimal("0.000001"),
        max_value=MAX_NUMERIC_19_6,
        min_value=Decimal("0.000000"),
        strictly_positive=True,
        allow_none=False,
        field_label="exchange_rate",
    )
    assert gross is not None and fee is not None and rate is not None
    curr = validate_iso_currency_3(currency_code, field_label="currency_code")

    if fee > gross:
        raise ValueError(f"SWIFT fee ({fee}) cannot exceed gross amount ({gross}).")

    net = gross - fee

    net_brl = (net * rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    fee_brl = (fee * rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    gross_brl = (gross * rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    if gross_brl <= Decimal("0.0000"):
        raise ValueError("Functional currency gross_brl must be strictly positive (> 0.0000).")

    # Prevent rounding drift
    drift = gross_brl - (net_brl + fee_brl)
    if drift != Decimal("0.0000"):
        if fee > Decimal("0.0000") and fee_brl + drift >= Decimal("0.0000"):
            fee_brl += drift
        else:
            net_brl += drift

    new_entry_id = uuid4()
    lines: List[JournalLineInput] = []
    order = 1

    if net_brl > Decimal("0.0000"):
        lines.append(
            JournalLineInput(
                line_id=uuid4(),
                entry_id=new_entry_id,
                account_code=cash_account,
                debit_amount=net_brl,
                credit_amount=Decimal("0.0000"),
                currency_code=curr,
                original_currency_amount=net,
                exchange_rate_applied=rate,
                line_order=order,
            )
        )
        order += 1

    if fee_brl > Decimal("0.0000"):
        lines.append(
            JournalLineInput(
                line_id=uuid4(),
                entry_id=new_entry_id,
                account_code=fee_account,
                debit_amount=fee_brl,
                credit_amount=Decimal("0.0000"),
                currency_code=curr,
                original_currency_amount=fee,
                exchange_rate_applied=rate,
                line_order=order,
            )
        )
        order += 1

    lines.append(
        JournalLineInput(
            line_id=uuid4(),
            entry_id=new_entry_id,
            account_code=ar_account,
            debit_amount=Decimal("0.0000"),
            credit_amount=gross_brl,
            currency_code=curr,
            original_currency_amount=gross,
            exchange_rate_applied=rate,
            line_order=order,
        )
    )

    return JournalEntryInput(
        entry_id=new_entry_id,
        transaction_date=transaction_date,
        competence_date=competence_date,
        reference_document_id=reference_document_id,
        description=f"SWIFT Settlement & Fee Deduction {reference_document_id} (Gross={gross}, Fee={fee})",
        status="POSTED",
        lines=lines,
    )


# ==============================================================================
# Brazilian Customs Cascading Tax Engine & CPC 16 Inventory Valuation
# ==============================================================================

VALID_TAX_REGIMES = {"LUCRO_REAL", "LUCRO_PRESUMIDO", "SIMPLES_NACIONAL", "CUSTO_INTEGRAL"}
VALID_CUSTOMS_CURRENCIES = {"USD", "EUR", "CHF", "BRL"}


class CustomsNationalizationCalcInput(BaseModel):
    """
    Input model for calculating Brazilian customs nationalization cascading taxes:
    II, IPI, PIS-Import, COFINS-Import, Siscomex, AFRMM, and ICMS por dentro.
    Enforces Modulo-11 CNPJ validation, CIF closure, clearance >= registration date,
    and strict anti-float validation across all monetary amounts and rates.
    """

    declaration_number: str = Field(..., min_length=1, max_length=50, description="DI or Duimp number (e.g. 26/1948201-3)")
    registration_date: date = Field(..., description="Official SISCOMEX registration date")
    clearance_date: date = Field(..., description="Customs clearance date (Desembaraço Aduaneiro)")
    importer_tax_id: str = Field(..., min_length=1, max_length=50, description="Corporate CNPJ of importer")
    invoice_id: Optional[UUID] = Field(default=None, description="FK linking to commercial_invoices.invoice_id")
    tax_regime: str = Field(default="LUCRO_REAL", description="Tax regime: LUCRO_REAL, LUCRO_PRESUMIDO, SIMPLES_NACIONAL, CUSTO_INTEGRAL")
    currency_code: str = Field(default="USD", min_length=3, max_length=3, description="Foreign currency code")

    fob_amount: Decimal = Field(..., ge=Decimal("0.0000"), description="FOB merchandise value in foreign currency")
    freight_amount: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"), description="International freight")
    insurance_amount: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"), description="International insurance")
    cif_amount: Optional[Decimal] = Field(default=None, ge=Decimal("0.0000"), description="CIF value (auto-computed if omitted)")
    ptax_exchange_rate: Decimal = Field(..., gt=Decimal("0.000000"), description="SISCOMEX official exchange rate")

    ii_rate: Decimal = Field(..., ge=Decimal("0.0000"), description="Import Duty nominal rate (II)")
    ipi_rate: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"), description="Excise Tax nominal rate (IPI)")
    pis_rate: Decimal = Field(default=Decimal("0.0210"), ge=Decimal("0.0000"), description="PIS-Importação rate (standard 0.0210)")
    cofins_rate: Decimal = Field(default=Decimal("0.0965"), ge=Decimal("0.0000"), description="COFINS-Importação rate (standard 0.0965)")
    siscomex_fee_brl: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"), description="Siscomex fee BRL")
    afrmm_brl: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"), description="AFRMM fee BRL")
    icms_rate: Decimal = Field(default=Decimal("0.1800"), ge=Decimal("0.0000"), lt=Decimal("1.0000"), description="State ICMS rate (e.g. 0.1800)")

    @field_validator("declaration_number", mode="before")
    @classmethod
    def validate_declaration_number(cls, v: Any) -> str:
        cleaned = validate_non_blank_str(v, field_label="declaration_number", max_length=50)
        assert cleaned is not None
        return cleaned

    @field_validator("importer_tax_id", mode="before")
    @classmethod
    def normalize_importer_tax_id(cls, v: Any) -> str:
        cleaned = validate_non_blank_str(
            v,
            field_label="importer_tax_id",
            max_length=MAX_TAX_ID_LENGTH,
        )
        assert cleaned is not None
        return cleaned

    @field_validator("importer_tax_id")
    @classmethod
    def validate_importer_cnpj(cls, v: str) -> str:
        if not validate_cnpj(v):
            raise ValueError(
                f"Invalid importer_tax_id '{v}'. Must be a valid 14-digit Brazilian CNPJ (Modulo 11)."
            )
        return v.strip()

    @field_validator(
        "fob_amount",
        "freight_amount",
        "insurance_amount",
        "siscomex_fee_brl",
        "afrmm_brl",
        mode="before",
    )
    @classmethod
    def reject_float_and_quantize_4(cls, v: Any) -> Decimal:
        parsed = parse_strict_decimal(
            v,
            scale=Decimal("0.0001"),
            max_value=MAX_NUMERIC_19_4,
            min_value=Decimal("0.0000"),
            allow_none=False,
            field_label="Customs monetary amount",
        )
        assert parsed is not None
        return parsed

    @field_validator("cif_amount", mode="before")
    @classmethod
    def reject_float_and_quantize_cif_4(cls, v: Any) -> Optional[Decimal]:
        return parse_strict_decimal(
            v,
            scale=Decimal("0.0001"),
            max_value=MAX_NUMERIC_19_4,
            min_value=Decimal("0.0000"),
            allow_none=True,
            field_label="cif_amount",
        )

    @field_validator("ptax_exchange_rate", mode="before")
    @classmethod
    def reject_float_and_quantize_6(cls, v: Any) -> Decimal:
        parsed = parse_strict_decimal(
            v,
            scale=Decimal("0.000001"),
            max_value=MAX_NUMERIC_19_6,
            min_value=Decimal("0.000000"),
            strictly_positive=True,
            allow_none=False,
            field_label="ptax_exchange_rate",
        )
        assert parsed is not None
        return parsed

    @field_validator("ii_rate", "ipi_rate", "pis_rate", "cofins_rate", mode="before")
    @classmethod
    def reject_float_and_quantize_rates(cls, v: Any) -> Decimal:
        parsed = parse_strict_decimal(
            v,
            scale=Decimal("0.0001"),
            max_value=MAX_NUMERIC_7_4,
            min_value=Decimal("0.0000"),
            allow_none=False,
            field_label="Tax rate",
        )
        assert parsed is not None
        return parsed

    @field_validator("icms_rate", mode="before")
    @classmethod
    def reject_float_and_quantize_icms_rate(cls, v: Any) -> Decimal:
        parsed = parse_strict_decimal(
            v,
            scale=Decimal("0.0001"),
            max_value=MAX_NUMERIC_7_4,
            min_value=Decimal("0.0000"),
            strictly_less_than=Decimal("1.0000"),
            allow_none=False,
            field_label="icms_rate",
        )
        assert parsed is not None
        return parsed

    @field_validator("tax_regime", mode="before")
    @classmethod
    def validate_tax_regime(cls, v: Any) -> str:
        cleaned = validate_non_blank_str(v, field_label="tax_regime", max_length=20)
        assert cleaned is not None
        regime = cleaned.upper()
        if regime not in VALID_TAX_REGIMES:
            raise ValueError(f"Invalid tax_regime '{v}'. Permitted values: {sorted(VALID_TAX_REGIMES)}")
        return regime

    @field_validator("currency_code", mode="before")
    @classmethod
    def validate_currency_code(cls, v: Any) -> str:
        cleaned = validate_non_blank_str(v, field_label="currency_code", max_length=3)
        assert cleaned is not None
        code = cleaned.upper()
        if code not in VALID_CUSTOMS_CURRENCIES:
            raise ValueError(f"Invalid currency_code '{v}'. Permitted values: {sorted(VALID_CUSTOMS_CURRENCIES)}")
        return code

    @model_validator(mode="after")
    def validate_cif_and_icms(self) -> "CustomsNationalizationCalcInput":
        if self.registration_date is not None and self.clearance_date is not None:
            if self.clearance_date < self.registration_date:
                raise ValueError(
                    f"clearance_date ({self.clearance_date}) cannot be earlier than "
                    f"registration_date ({self.registration_date})."
                )

        calculated_cif = (self.fob_amount + self.freight_amount + self.insurance_amount).quantize(
            Decimal("0.0001"), rounding=ROUND_HALF_UP
        )
        if calculated_cif > MAX_NUMERIC_19_4:
            raise ValueError(
                f"Calculated cif_amount ({calculated_cif}) exceeds NUMERIC(19,4) bound ({MAX_NUMERIC_19_4})."
            )

        if self.cif_amount is None:
            self.cif_amount = calculated_cif
        else:
            delta = abs(self.cif_amount - calculated_cif)
            if delta > Decimal("0.0001"):
                raise ValueError(
                    f"CIF closure failed: cif_amount ({self.cif_amount}) != "
                    f"fob_amount ({self.fob_amount}) + freight_amount ({self.freight_amount}) + "
                    f"insurance_amount ({self.insurance_amount}) = {calculated_cif} "
                    f"(drift: {delta} > 0.0001)."
                )

        if self.icms_rate >= Decimal("1.0000"):
            raise ValueError(f"icms_rate ({self.icms_rate}) must be strictly less than 1.0000 (100%).")

        return self


class CustomsNationalizationResult(BaseModel):
    """
    Detailed result of Brazilian customs nationalization calculation,
    including cascading tax breakdown and CPC 16 inventory valuation.
    """

    nationalization_id: Optional[UUID] = None
    declaration_number: str
    registration_date: date
    clearance_date: date
    invoice_id: Optional[UUID] = None
    importer_tax_id: str
    tax_regime: str
    currency_code: str
    fob_amount: Decimal
    freight_amount: Decimal
    insurance_amount: Decimal
    cif_amount: Decimal
    ptax_exchange_rate: Decimal
    customs_value_brl: Decimal
    ii_rate: Decimal
    ii_amount_brl: Decimal
    ipi_rate: Decimal
    ipi_amount_brl: Decimal
    pis_rate: Decimal
    pis_amount_brl: Decimal
    cofins_rate: Decimal
    cofins_amount_brl: Decimal
    siscomex_fee_brl: Decimal
    afrmm_brl: Decimal
    icms_rate: Decimal
    icms_base_brl: Decimal
    icms_amount_brl: Decimal
    total_taxes_brl: Decimal
    landed_cost_brl: Decimal
    net_inventory_cost_brl: Decimal
    recoverable_taxes_brl: Decimal
    journal_entry_id: Optional[UUID] = None

    @field_validator(
        "fob_amount",
        "freight_amount",
        "insurance_amount",
        "cif_amount",
        "customs_value_brl",
        "ii_amount_brl",
        "ipi_amount_brl",
        "pis_amount_brl",
        "cofins_amount_brl",
        "siscomex_fee_brl",
        "afrmm_brl",
        "icms_base_brl",
        "icms_amount_brl",
        "total_taxes_brl",
        "landed_cost_brl",
        "net_inventory_cost_brl",
        "recoverable_taxes_brl",
        mode="before",
    )
    @classmethod
    def reject_float_and_quantize_res_4(cls, v: Any) -> Decimal:
        parsed = parse_strict_decimal(
            v,
            scale=Decimal("0.0001"),
            max_value=MAX_NUMERIC_19_4,
            min_value=Decimal("0.0000"),
            allow_none=False,
            field_label="Customs result monetary amount",
        )
        assert parsed is not None
        return parsed

    @field_validator("ptax_exchange_rate", mode="before")
    @classmethod
    def reject_float_and_quantize_res_6(cls, v: Any) -> Decimal:
        parsed = parse_strict_decimal(
            v,
            scale=Decimal("0.000001"),
            max_value=MAX_NUMERIC_19_6,
            min_value=Decimal("0.000000"),
            strictly_positive=True,
            allow_none=False,
            field_label="ptax_exchange_rate",
        )
        assert parsed is not None
        return parsed

    @field_validator("ii_rate", "ipi_rate", "pis_rate", "cofins_rate", mode="before")
    @classmethod
    def reject_float_and_quantize_res_rates(cls, v: Any) -> Decimal:
        parsed = parse_strict_decimal(
            v,
            scale=Decimal("0.0001"),
            max_value=MAX_NUMERIC_7_4,
            min_value=Decimal("0.0000"),
            allow_none=False,
            field_label="Tax rate",
        )
        assert parsed is not None
        return parsed

    @field_validator("icms_rate", mode="before")
    @classmethod
    def reject_float_and_quantize_res_icms_rate(cls, v: Any) -> Decimal:
        parsed = parse_strict_decimal(
            v,
            scale=Decimal("0.0001"),
            max_value=MAX_NUMERIC_7_4,
            min_value=Decimal("0.0000"),
            strictly_less_than=Decimal("1.0000"),
            allow_none=False,
            field_label="icms_rate",
        )
        assert parsed is not None
        return parsed


def compute_brazilian_landed_cost(
    inp: CustomsNationalizationCalcInput,
) -> CustomsNationalizationResult:
    """
    Computes Brazilian customs cascading import duties and taxes in strict statutory order:
    1. Valor Aduaneiro (VA_BRL) = CIF_USD * PTAX
    2. II_BRL = VA_BRL * ii_rate
    3. IPI_BRL = (VA_BRL + II_BRL) * ipi_rate
    4. PIS_BRL = VA_BRL * pis_rate
    5. COFINS_BRL = VA_BRL * cofins_rate
    6. ICMS Base Grossed-up ("Cálculo por Dentro"):
       Base = (VA + II + IPI + PIS + COFINS + Siscomex + AFRMM) / (1 - icms_rate)
    7. ICMS_BRL = ICMS Base * icms_rate
    8. Total Taxes = II + IPI + PIS + COFINS + Siscomex + AFRMM + ICMS
    9. Landed Cost = VA + Total Taxes
    10. CPC 16 / IAS 2 Accounting Segregation:
        Under LUCRO_REAL:
          net_inventory_cost_brl = VA + II + Siscomex + AFRMM (capitalized non-recoverable asset)
          recoverable_taxes_brl = IPI + PIS + COFINS + ICMS (recoverable tax asset)
        Under CUSTO_INTEGRAL (or cumulative regimes without credits):
          net_inventory_cost_brl = Landed Cost (fully capitalized)
          recoverable_taxes_brl = 0.0000
    """
    cif_amount = inp.cif_amount if inp.cif_amount is not None else (
        inp.fob_amount + inp.freight_amount + inp.insurance_amount
    ).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    va_brl = (cif_amount * inp.ptax_exchange_rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    ii_brl = (va_brl * inp.ii_rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    ipi_brl = ((va_brl + ii_brl) * inp.ipi_rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    pis_brl = (va_brl * inp.pis_rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    cofins_brl = (va_brl * inp.cofins_rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    icms_divisor = Decimal("1.0000") - inp.icms_rate
    if icms_divisor <= Decimal("0.0000"):
        raise ValueError(f"icms_rate ({inp.icms_rate}) cannot be >= 1.0000")

    numerator = va_brl + ii_brl + ipi_brl + pis_brl + cofins_brl + inp.siscomex_fee_brl + inp.afrmm_brl
    icms_base_brl = (numerator / icms_divisor).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    icms_brl = (icms_base_brl * inp.icms_rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    total_taxes_brl = (
        ii_brl + ipi_brl + pis_brl + cofins_brl + inp.siscomex_fee_brl + inp.afrmm_brl + icms_brl
    ).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    landed_cost_brl = (va_brl + total_taxes_brl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    regime = inp.tax_regime.upper().strip()
    if regime == "LUCRO_REAL":
        net_inventory_cost_brl = (
            va_brl + ii_brl + inp.siscomex_fee_brl + inp.afrmm_brl
        ).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        recoverable_taxes_brl = (
            ipi_brl + pis_brl + cofins_brl + icms_brl
        ).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    else:
        net_inventory_cost_brl = landed_cost_brl
        recoverable_taxes_brl = Decimal("0.0000")

    return CustomsNationalizationResult(
        declaration_number=inp.declaration_number,
        registration_date=inp.registration_date,
        clearance_date=inp.clearance_date,
        invoice_id=inp.invoice_id,
        importer_tax_id=inp.importer_tax_id,
        tax_regime=regime,
        currency_code=inp.currency_code,
        fob_amount=inp.fob_amount,
        freight_amount=inp.freight_amount,
        insurance_amount=inp.insurance_amount,
        cif_amount=cif_amount,
        ptax_exchange_rate=inp.ptax_exchange_rate,
        customs_value_brl=va_brl,
        ii_rate=inp.ii_rate,
        ii_amount_brl=ii_brl,
        ipi_rate=inp.ipi_rate,
        ipi_amount_brl=ipi_brl,
        pis_rate=inp.pis_rate,
        pis_amount_brl=pis_brl,
        cofins_rate=inp.cofins_rate,
        cofins_amount_brl=cofins_brl,
        siscomex_fee_brl=inp.siscomex_fee_brl,
        afrmm_brl=inp.afrmm_brl,
        icms_rate=inp.icms_rate,
        icms_base_brl=icms_base_brl,
        icms_amount_brl=icms_brl,
        total_taxes_brl=total_taxes_brl,
        landed_cost_brl=landed_cost_brl,
        net_inventory_cost_brl=net_inventory_cost_brl,
        recoverable_taxes_brl=recoverable_taxes_brl,
    )


# Alias for compatibility
calculate_cascading_taxes = compute_brazilian_landed_cost


def build_import_nationalization_ledger_entry(
    result: CustomsNationalizationResult,
    entry_id: Optional[UUID] = None,
    inventory_available_account: str = "1.1.3.02",
    recoverable_taxes_account: str = "1.1.4.01",
    inventory_in_transit_account: str = "1.1.3.01",
    customs_liabilities_account: str = "2.1.2.01",
) -> JournalEntryInput:
    """
    Builds a balanced double-entry posting for customs clearance and nationalization:
    - Under LUCRO_REAL with recoverable taxes (4 legs):
        Debit 1.1.3.02 (Inventory Available) with net_inventory_cost_brl
        Debit 1.1.4.01 (Recoverable Taxes Asset) with recoverable_taxes_brl
        Credit 1.1.3.01 (Inventory in Transit) with customs_value_brl
        Credit 2.1.2.01 (Customs Duties Liabilities) with total_taxes_brl
    - Under CUSTO_INTEGRAL with taxes (3 legs):
        Debit 1.1.3.02 (Inventory Available) with landed_cost_brl
        Credit 1.1.3.01 (Inventory in Transit) with customs_value_brl
        Credit 2.1.2.01 (Customs Duties Liabilities) with total_taxes_brl
    - Under zero-tax duty-free nationalizations (2 legs):
        Skips zero-value tax/recoverable lines so all legs remain > 0 and balanced.
    """
    new_entry_id = entry_id or uuid4()
    lines: List[JournalLineInput] = []
    order = 1

    if result.net_inventory_cost_brl > Decimal("0.0000"):
        lines.append(
            JournalLineInput(
                line_id=uuid4(),
                entry_id=new_entry_id,
                account_code=inventory_available_account,
                debit_amount=result.net_inventory_cost_brl,
                credit_amount=Decimal("0.0000"),
                currency_code="BRL",
                original_currency_amount=result.net_inventory_cost_brl,
                exchange_rate_applied=Decimal("1.000000"),
                line_order=order,
            )
        )
        order += 1

    if result.recoverable_taxes_brl > Decimal("0.0000"):
        lines.append(
            JournalLineInput(
                line_id=uuid4(),
                entry_id=new_entry_id,
                account_code=recoverable_taxes_account,
                debit_amount=result.recoverable_taxes_brl,
                credit_amount=Decimal("0.0000"),
                currency_code="BRL",
                original_currency_amount=result.recoverable_taxes_brl,
                exchange_rate_applied=Decimal("1.000000"),
                line_order=order,
            )
        )
        order += 1

    if result.customs_value_brl > Decimal("0.0000"):
        lines.append(
            JournalLineInput(
                line_id=uuid4(),
                entry_id=new_entry_id,
                account_code=inventory_in_transit_account,
                debit_amount=Decimal("0.0000"),
                credit_amount=result.customs_value_brl,
                currency_code=result.currency_code,
                original_currency_amount=result.cif_amount,
                exchange_rate_applied=result.ptax_exchange_rate,
                line_order=order,
            )
        )
        order += 1

    if result.total_taxes_brl > Decimal("0.0000"):
        lines.append(
            JournalLineInput(
                line_id=uuid4(),
                entry_id=new_entry_id,
                account_code=customs_liabilities_account,
                debit_amount=Decimal("0.0000"),
                credit_amount=result.total_taxes_brl,
                currency_code="BRL",
                original_currency_amount=result.total_taxes_brl,
                exchange_rate_applied=Decimal("1.000000"),
                line_order=order,
            )
        )

    return JournalEntryInput(
        entry_id=new_entry_id,
        transaction_date=result.clearance_date,
        competence_date=result.clearance_date,
        reference_document_id=result.declaration_number,
        description=f"Desembaraço Aduaneiro e Nacionalização {result.declaration_number} ({result.tax_regime})",
        status="POSTED",
        lines=lines,
    )
