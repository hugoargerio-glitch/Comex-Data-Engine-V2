"""
Domain Models: Customs Operations, PTAX Time-Series & Double-Entry Fiduciary Ledger
(ODCS v3.2.0 & PostgreSQL 16 ACID Fiduciary Ledger / DuckDB Parquet Star Schema)

Enforces zero-float prohibition, DECIMAL(19,4) monetary precision, DECIMAL(19,6) FX rates,
LOCF forward-fill without lookahead bias, deterministic operation_hash_id, and
zero-sum append-only double-entry bookkeeping with statutory reversal and SWIFT split builders.
"""

from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import hashlib
from typing import Any, List, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator

VALID_PTAX_CURRENCIES = {"USD", "EUR", "CHF"}
VALID_OPERATION_TYPES = {"IMPORT", "EXPORT"}
VALID_ENTRY_STATUSES = {"POSTED", "REVERSED"}


class PTAXRateInput(BaseModel):
    """
    Daily official foreign exchange rate dimension conforming to dim_currency_ptax.
    Enforces DECIMAL(19,6) precision, prohibits lookahead bias, and supports LOCF.
    """
    date_id: int = Field(..., description="Integer date key in YYYYMMDD format", ge=19000101, le=29991231)
    currency_code: str = Field(..., min_length=3, max_length=3, description="ISO 4217 currency (USD, EUR, CHF)")
    ptax_buy_rate: Decimal = Field(..., gt=Decimal("0.000000"), description="Official BACEN Olinda buying rate")
    ptax_sell_rate: Decimal = Field(..., gt=Decimal("0.000000"), description="Official BACEN Olinda selling rate")
    is_business_day: bool = Field(..., description="True if BACEN quotation published on this date")
    is_interpolated_locf: bool = Field(default=False, description="True if forward-filled via LOCF from previous business day")

    @field_validator("ptax_buy_rate", "ptax_sell_rate", mode="before")
    @classmethod
    def reject_float_and_quantize_fx(cls, v: Any) -> Decimal:
        if isinstance(v, float):
            raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")
        dec = Decimal(str(v)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
        if dec <= Decimal("0.000000"):
            raise ValueError("Exchange rate must be strictly positive.")
        return dec

    @field_validator("currency_code")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        code = v.upper().strip()
        if code not in VALID_PTAX_CURRENCIES:
            raise ValueError(f"Invalid PTAX currency '{v}'. Permitted values: {sorted(VALID_PTAX_CURRENCIES)}")
        return code

    @model_validator(mode="after")
    def validate_no_lookahead_and_spread(self) -> "PTAXRateInput":
        year = self.date_id // 10000
        month = (self.date_id % 10000) // 100
        day = self.date_id % 100
        try:
            rate_date = date(year, month, day)
        except ValueError as exc:
            raise ValueError(f"Invalid date_id {self.date_id}: {exc}") from exc

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

    # Map existing rates by date_id
    existing_by_date = {r.date_id: r for r in rates if r.currency_code == code}
    sorted_known = sorted(
        [r for r in rates if r.currency_code == code],
        key=lambda x: x.date_id,
    )

    result: List[PTAXRateInput] = []
    current_date = start_date
    last_known: Optional[PTAXRateInput] = None

    # Check if there is an observation prior to start_date for initializing LOCF
    for r in sorted_known:
        r_y = r.date_id // 10000
        r_m = (r.date_id % 10000) // 100
        r_d = r.date_id % 100
        r_date = date(r_y, r_m, r_d)
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
            # Create forward-filled LOCF rate
            interpolated = PTAXRateInput(
                date_id=curr_id,
                currency_code=code,
                ptax_buy_rate=last_known.ptax_buy_rate,
                ptax_sell_rate=last_known.ptax_sell_rate,
                is_business_day=False,
                is_interpolated_locf=True,
            )
            result.append(interpolated)

        # Advance by 1 calendar day
        from datetime import timedelta
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
    Computes deterministic SHA-256 natural key hash for customs operations facts:
    SHA-256(ano || mes || tipo_op || co_ncm || co_pais || sg_uf || co_via || co_urf)
    Guarantees analytical idempotency during re-runs.
    """
    canonical_string = (
        f"{ano}{mes:02d}{tipo_op.upper().strip()}{co_ncm.strip()}{co_pais}"
        f"{sg_uf.upper().strip()}{co_via}{co_urf}"
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
    country_code: int = Field(..., ge=1, description="Partner country code (MDIC / ISO 3166)")
    uf_code: str = Field(..., min_length=2, max_length=2, description="Brazilian federated state (UF)")
    modal_code: int = Field(..., ge=1, description="Transport modal code (e.g. 1=Maritime, 4=Air, 7=Road)")
    urf_code: int = Field(..., ge=1, description="Customs Clearance Unit (URF)")

    statistical_quantity: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"))
    net_weight_kg: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"))
    fob_amount_usd: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"))
    freight_amount_usd: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"))
    insurance_amount_usd: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"))
    cif_amount_usd: Decimal = Field(..., ge=Decimal("0.0000"))
    landed_cost_brl: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"))

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
        if isinstance(v, float):
            raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")
        if v is None:
            return Decimal("0.0000")
        dec = Decimal(str(v)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        if dec < Decimal("0.0000"):
            raise ValueError("Amount cannot be negative.")
        return dec

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
        return v.upper().strip()

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


class JournalLineInput(BaseModel):
    """
    Immutable double-entry ledger line item in functional currency (BRL).
    Enforces DECIMAL(19,4) amount precision, DECIMAL(19,6) FX rates, non-negativity,
    and mutually exclusive debit or credit.
    """
    line_id: Optional[UUID] = None
    entry_id: Optional[UUID] = None
    account_code: str = Field(..., min_length=1, max_length=50, description="Chart of Accounts canonical code")
    debit_amount: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"))
    credit_amount: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0.0000"))
    currency_code: str = Field(..., min_length=3, max_length=3, description="Original transaction currency")
    original_currency_amount: Decimal = Field(..., ge=Decimal("0.0000"))
    exchange_rate_applied: Decimal = Field(default=Decimal("1.000000"), gt=Decimal("0.000000"))
    line_order: int = Field(..., ge=1)

    @field_validator("debit_amount", "credit_amount", "original_currency_amount", mode="before")
    @classmethod
    def reject_float_and_quantize_amount(cls, v: Any) -> Decimal:
        if isinstance(v, float):
            raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")
        if v is None:
            return Decimal("0.0000")
        dec = Decimal(str(v)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        if dec < Decimal("0.0000"):
            raise ValueError("Ledger line amounts cannot be negative.")
        return dec

    @field_validator("exchange_rate_applied", mode="before")
    @classmethod
    def reject_float_and_quantize_rate(cls, v: Any) -> Decimal:
        if isinstance(v, float):
            raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")
        if v is None:
            return Decimal("1.000000")
        dec = Decimal(str(v)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
        if dec <= Decimal("0.000000"):
            raise ValueError("exchange_rate_applied must be strictly positive.")
        return dec

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
        return self


class JournalEntryInput(BaseModel):
    """
    Append-Only Canonical Journal Entry Header (PostgreSQL 16 ACID Fiduciary Ledger).
    Enforces minimum 2 lines and zero-sum invariant: SUM(debit) == SUM(credit) within 0.0001.
    """
    entry_id: Optional[UUID] = None
    transaction_date: date = Field(..., description="Date financial event executed")
    competence_date: date = Field(..., description="Accounting competence date matching invoice")
    reference_document_id: Optional[str] = Field(None, max_length=100)
    description: str = Field(..., min_length=1)
    status: str = Field(default="POSTED", description="Posting lifecycle status: POSTED or REVERSED")
    reversed_from_entry_id: Optional[UUID] = None
    lines: List[JournalLineInput] = Field(..., min_length=2)

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        st = v.upper().strip()
        if st not in VALID_ENTRY_STATUSES:
            raise ValueError(f"Invalid status '{v}'. Permitted values: {sorted(VALID_ENTRY_STATUSES)}")
        return st

    @model_validator(mode="after")
    def validate_zero_sum_invariant(self) -> "JournalEntryInput":
        if self.entry_id is None:
            self.entry_id = uuid4()

        if len(self.lines) < 2:
            raise ValueError(f"Double-entry bookkeeping requires at least 2 lines; got {len(self.lines)}.")

        total_debits = sum(line.debit_amount for line in self.lines).quantize(
            Decimal("0.0001"), rounding=ROUND_HALF_UP
        )
        total_credits = sum(line.credit_amount for line in self.lines).quantize(
            Decimal("0.0001"), rounding=ROUND_HALF_UP
        )

        imbalance = abs(total_debits - total_credits)
        if imbalance > Decimal("0.0001"):
            raise ValueError(
                f"Double-entry zero-sum invariant violated: SUM(debits) = {total_debits} != "
                f"SUM(credits) = {total_credits} (drift: {imbalance} > 0.0001)."
            )

        # Set entry_id on child lines
        for line in self.lines:
            if line.entry_id is None:
                line.entry_id = self.entry_id

        return self


def create_reversal_entry(
    original_entry: JournalEntryInput,
    reversal_date: date,
    reason: str,
) -> JournalEntryInput:
    """
    Generates an auditable statutory compensating entry (Reversal Entry)
    with inverted debit/credit lines, preserving append-only ledger immutability.
    """
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
    if isinstance(foreign_amount, float) or isinstance(competence_rate, float) or isinstance(spot_rate, float):
        raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")

    v_estrang = Decimal(str(foreign_amount)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    rate_t1 = Decimal(str(competence_rate)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
    rate_t2 = Decimal(str(spot_rate)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)

    # Values in functional currency (BRL)
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
            currency_code=currency_code,
            original_currency_amount=v_estrang,
            exchange_rate_applied=rate_t2,
            line_order=1,
        )
    )

    if fx_variance > Decimal("0.0000"):
        # Realized FX Gain: Credit FX Gain, Credit Receivable
        lines.append(
            JournalLineInput(
                line_id=uuid4(),
                entry_id=new_entry_id,
                account_code=receivable_account,
                debit_amount=Decimal("0.0000"),
                credit_amount=receivable_closed,
                currency_code=currency_code,
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
        # Realized FX Loss: Debit FX Loss, Credit Receivable
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
                currency_code=currency_code,
                original_currency_amount=v_estrang,
                exchange_rate_applied=rate_t1,
                line_order=3,
            )
        )
    else:
        # Zero variance: Spot == PTAX
        lines.append(
            JournalLineInput(
                line_id=uuid4(),
                entry_id=new_entry_id,
                account_code=receivable_account,
                debit_amount=Decimal("0.0000"),
                credit_amount=receivable_closed,
                currency_code=currency_code,
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
    Debits Cash (net received) and SWIFT Bank Fees (deducted fee) against Accounts Receivable (gross amount).
    """
    if isinstance(gross_amount, float) or isinstance(fee_amount, float) or isinstance(exchange_rate, float):
        raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")

    gross = Decimal(str(gross_amount)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    fee = Decimal(str(fee_amount)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    rate = Decimal(str(exchange_rate)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)

    if fee > gross:
        raise ValueError(f"SWIFT fee ({fee}) cannot exceed gross amount ({gross}).")

    net = gross - fee

    net_brl = (net * rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    fee_brl = (fee * rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    gross_brl = (gross * rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    # Prevent rounding drift
    drift = gross_brl - (net_brl + fee_brl)
    if drift != Decimal("0.0000"):
        fee_brl += drift

    new_entry_id = uuid4()
    lines = [
        JournalLineInput(
            line_id=uuid4(),
            entry_id=new_entry_id,
            account_code=cash_account,
            debit_amount=net_brl,
            credit_amount=Decimal("0.0000"),
            currency_code=currency_code,
            original_currency_amount=net,
            exchange_rate_applied=rate,
            line_order=1,
        ),
        JournalLineInput(
            line_id=uuid4(),
            entry_id=new_entry_id,
            account_code=fee_account,
            debit_amount=fee_brl,
            credit_amount=Decimal("0.0000"),
            currency_code=currency_code,
            original_currency_amount=fee,
            exchange_rate_applied=rate,
            line_order=2,
        ),
        JournalLineInput(
            line_id=uuid4(),
            entry_id=new_entry_id,
            account_code=ar_account,
            debit_amount=Decimal("0.0000"),
            credit_amount=gross_brl,
            currency_code=currency_code,
            original_currency_amount=gross,
            exchange_rate_applied=rate,
            line_order=3,
        ),
    ]

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
    Enforces anti-float rule across all monetary amounts and rates.
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

    @field_validator(
        "fob_amount",
        "freight_amount",
        "insurance_amount",
        "cif_amount",
        "siscomex_fee_brl",
        "afrmm_brl",
        mode="before",
    )
    @classmethod
    def reject_float_and_quantize_4(cls, v: Any) -> Optional[Decimal]:
        if isinstance(v, float):
            raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")
        if v is None:
            return None
        dec = Decimal(str(v)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        if dec < Decimal("0.0000"):
            raise ValueError("Amount cannot be negative.")
        return dec

    @field_validator("ptax_exchange_rate", mode="before")
    @classmethod
    def reject_float_and_quantize_6(cls, v: Any) -> Decimal:
        if isinstance(v, float):
            raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")
        dec = Decimal(str(v)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
        if dec <= Decimal("0.000000"):
            raise ValueError("ptax_exchange_rate must be strictly positive.")
        return dec

    @field_validator("ii_rate", "ipi_rate", "pis_rate", "cofins_rate", "icms_rate", mode="before")
    @classmethod
    def reject_float_and_quantize_rates(cls, v: Any) -> Decimal:
        if isinstance(v, float):
            raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")
        dec = Decimal(str(v)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        if dec < Decimal("0.0000"):
            raise ValueError("Tax rates cannot be negative.")
        return dec

    @field_validator("tax_regime")
    @classmethod
    def validate_tax_regime(cls, v: str) -> str:
        regime = v.upper().strip()
        if regime not in VALID_TAX_REGIMES:
            raise ValueError(f"Invalid tax_regime '{v}'. Permitted values: {sorted(VALID_TAX_REGIMES)}")
        return regime

    @field_validator("currency_code")
    @classmethod
    def validate_currency_code(cls, v: str) -> str:
        code = v.upper().strip()
        if code not in VALID_CUSTOMS_CURRENCIES:
            raise ValueError(f"Invalid currency_code '{v}'. Permitted values: {sorted(VALID_CUSTOMS_CURRENCIES)}")
        return code

    @model_validator(mode="after")
    def validate_cif_and_icms(self) -> "CustomsNationalizationCalcInput":
        calculated_cif = (self.fob_amount + self.freight_amount + self.insurance_amount).quantize(
            Decimal("0.0001"), rounding=ROUND_HALF_UP
        )
        if self.cif_amount is None:
            self.cif_amount = calculated_cif
        elif self.cif_amount < self.fob_amount:
            raise ValueError(
                f"cif_amount ({self.cif_amount}) cannot be less than fob_amount ({self.fob_amount})."
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
        if isinstance(v, float):
            raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")
        if v is None:
            return Decimal("0.0000")
        return Decimal(str(v)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    @field_validator("ptax_exchange_rate", mode="before")
    @classmethod
    def reject_float_and_quantize_res_6(cls, v: Any) -> Decimal:
        if isinstance(v, float):
            raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")
        return Decimal(str(v)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)

    @field_validator("ii_rate", "ipi_rate", "pis_rate", "cofins_rate", "icms_rate", mode="before")
    @classmethod
    def reject_float_and_quantize_res_rates(cls, v: Any) -> Decimal:
        if isinstance(v, float):
            raise ValueError("IEEE 754 float is strictly prohibited; pass str, int, or Decimal.")
        return Decimal(str(v)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


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

    # 1. Customs Value in BRL (Valor Aduaneiro)
    va_brl = (cif_amount * inp.ptax_exchange_rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    # 2. II (Imposto de Importação)
    ii_brl = (va_brl * inp.ii_rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    # 3. IPI (Imposto sobre Produtos Industrializados) on (VA + II)
    ipi_brl = ((va_brl + ii_brl) * inp.ipi_rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    # 4. PIS-Importação on VA
    pis_brl = (va_brl * inp.pis_rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    # 5. COFINS-Importação on VA
    cofins_brl = (va_brl * inp.cofins_rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    # 6. ICMS Grossed-up Tax Base ("Cálculo por Dentro")
    icms_divisor = Decimal("1.0000") - inp.icms_rate
    if icms_divisor <= Decimal("0.0000"):
        raise ValueError(f"icms_rate ({inp.icms_rate}) cannot be >= 1.0000")

    numerator = va_brl + ii_brl + ipi_brl + pis_brl + cofins_brl + inp.siscomex_fee_brl + inp.afrmm_brl
    icms_base_brl = (numerator / icms_divisor).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    # 7. ICMS Amount
    icms_brl = (icms_base_brl * inp.icms_rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    # 8. Total Customs Taxes & Duties
    total_taxes_brl = (
        ii_brl + ipi_brl + pis_brl + cofins_brl + inp.siscomex_fee_brl + inp.afrmm_brl + icms_brl
    ).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    # 9. Gross Landed Cost
    landed_cost_brl = (va_brl + total_taxes_brl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    # 10. CPC 16 / IAS 2 Inventory Cost vs Recoverable Taxes
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
    - Under LUCRO_REAL (4 legs):
        Debit 1.1.3.02 (Inventory Available) with net_inventory_cost_brl
        Debit 1.1.4.01 (Recoverable Taxes Asset) with recoverable_taxes_brl
        Credit 1.1.3.01 (Inventory in Transit) with customs_value_brl
        Credit 2.1.2.01 (Customs Duties Liabilities) with total_taxes_brl
    - Under CUSTO_INTEGRAL (3 legs):
        Debit 1.1.3.02 (Inventory Available) with landed_cost_brl
        Credit 1.1.3.01 (Inventory in Transit) with customs_value_brl
        Credit 2.1.2.01 (Customs Duties Liabilities) with total_taxes_brl
    Guarantees SUM(Debits) - SUM(Credits) == 0.0000.
    """
    new_entry_id = entry_id or uuid4()
    lines: List[JournalLineInput] = []

    if result.recoverable_taxes_brl > Decimal("0.0000"):
        # 4-legged entry (LUCRO_REAL)
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
                line_order=1,
            )
        )
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
                line_order=2,
            )
        )
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
                line_order=3,
            )
        )
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
                line_order=4,
            )
        )
    else:
        # 3-legged entry (CUSTO_INTEGRAL)
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
                line_order=1,
            )
        )
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
                line_order=2,
            )
        )
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
                line_order=3,
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

