"""
Unit Hardening Tests for Wave 1 Invariants:
- Strict Anti-Float / Anti-None / Anti-Bool / Overflow Validation (_validators.py)
- CustomsNationalizationCalcInput Modulo-11 CNPJ, CIF closure & clearance date checks
- Frozen Ledger DTOs, REVERSED state machine, original_currency_amount > 0
- Zero-fee SWIFT split & zero-tax duty-free nationalization balanced entries
- QuarantineRecordInput (REJECTED_HITL), MerchandiseNCMInput, PartnerCountryInput
- Salted PII hashing & pipe-delimited operation_hash_id
- Lakehouse Parquet Hive partition merge/dedup non-clobbering
- MDIC DuckDB PTAX no-lookahead resolution & HTTP 4xx fail-fast
"""

from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import math
from pathlib import Path
from uuid import uuid4

import httpx
import pyarrow as pa
from pydantic import ValidationError
import pytest

from src.connectors import lakehouse
from src.connectors.lakehouse import duckdb_connection, write_records_to_parquet_hive
from src.extractors.http_client import ResilientAPIExtractor
from src.extractors.mdic_bulk_loader import DEFAULT_USD_PTAX_SELL_RATE, MDICBulkLoader
from src.models._validators import MAX_NUMERIC_7_4, MAX_NUMERIC_19_4, MAX_NUMERIC_19_6, MAX_PG_INT
from src.models.customs import (
    ComexOperationInput,
    CustomsNationalizationCalcInput,
    JournalEntryInput,
    JournalLineInput,
    MerchandiseNCMInput,
    PTAXRateInput,
    PartnerCountryInput,
    build_import_nationalization_ledger_entry,
    build_swift_fee_split_entry,
    compute_brazilian_landed_cost,
    compute_operation_hash_id,
    create_reversal_entry,
)
from src.models.invoices import (
    CommercialInvoiceInput,
    CommercialInvoiceLineInput,
    hash_natural_person_pii,
)
from src.models.quarantine import QuarantineRecordInput
from src.pipeline.comex_processor import FACT_COMEX_PARQUET_SCHEMA

VALID_CNPJ = "33.000.167/0001-01"
INVALID_CNPJ = "33.000.167/0001-99"


@pytest.mark.unit
def test_validators_reject_none_bool_nan_inf_comma_and_overflow():
    # 1. Required fields reject None cleanly with structured ValidationError
    with pytest.raises(ValidationError):
        CommercialInvoiceLineInput(
            line_number=1,
            ncm_code="84713012",
            description="Hardware",
            quantity=None,  # type: ignore[arg-type]
            unit_price="100.0000",
            total_price="100.0000",
        )

    with pytest.raises(ValidationError):
        CommercialInvoiceInput(
            issuer_tax_id=VALID_CNPJ,
            document_number="INV-NONE-01",
            issue_date="2026-11-10",
            currency_code="USD",
            incoterm="FOB",
            subtotal_amount="100.0000",
            tax_amount=None,  # type: ignore[arg-type]
            freight_amount="0.0000",
            insurance_amount="0.0000",
            total_amount="100.0000",
        )

    with pytest.raises(ValidationError):
        ComexOperationInput(
            date_id=20261101,
            operation_type="IMPORT",
            ncm_code="84713012",
            country_code=249,
            uf_code="SP",
            modal_code=1,
            urf_code=817800,
            cif_amount_usd=None,  # type: ignore[arg-type]
        )

    # 2. Rejects bools, floats, NaN, Infinity, -Infinity, and locale comma strings
    bad_numeric_samples = [
        True,
        False,
        10.5,
        float("nan"),
        float("inf"),
        float("-inf"),
        "NaN",
        "Infinity",
        "-Infinity",
        "1,250.00",
        "1,250",
        "",
        "   ",
    ]
    for bad in bad_numeric_samples:
        with pytest.raises(ValidationError):
            PTAXRateInput(
                date_id=20261110,
                currency_code="USD",
                ptax_buy_rate=bad,  # type: ignore[arg-type]
                ptax_sell_rate="5.820000",
            )

    # 3. Rejects bool on integer fields (since isinstance(True, int) in Python!)
    with pytest.raises(ValidationError):
        PartnerCountryInput(
            country_code=True,  # type: ignore[arg-type]
            country_iso3="USA",
            country_name="Estados Unidos",
        )

    # 4. Rejects SQL precision & integer overflows
    with pytest.raises(ValidationError):
        PTAXRateInput(
            date_id=20261110,
            currency_code="USD",
            ptax_buy_rate=str(MAX_NUMERIC_19_6 + Decimal("1.000000")),
            ptax_sell_rate="5.820000",
        )

    with pytest.raises(ValidationError):
        CustomsNationalizationCalcInput(
            declaration_number="26/OVERFLOW-1",
            registration_date=date(2026, 11, 10),
            clearance_date=date(2026, 11, 10),
            importer_tax_id=VALID_CNPJ,
            currency_code="USD",
            fob_amount=str(MAX_NUMERIC_19_4),
            freight_amount="100.0000",  # triggers CIF > MAX_NUMERIC_19_4
            insurance_amount="0.0000",
            ptax_exchange_rate="5.800000",
            ii_rate="0.1000",
        )

    with pytest.raises(ValidationError):
        CustomsNationalizationCalcInput(
            declaration_number="26/RATE-OVERFLOW",
            registration_date=date(2026, 11, 10),
            clearance_date=date(2026, 11, 10),
            importer_tax_id=VALID_CNPJ,
            currency_code="USD",
            fob_amount="100.0000",
            ptax_exchange_rate="5.800000",
            ii_rate=str(MAX_NUMERIC_7_4 + Decimal("1.0000")),
        )

    with pytest.raises(ValidationError):
        PartnerCountryInput(
            country_code=MAX_PG_INT + 1,
            country_iso3="USA",
            country_name="Overflow Country",
        )

    assert not math.isnan(float(MAX_NUMERIC_19_4))


@pytest.mark.unit
def test_customs_nationalization_calc_input_modulo11_cif_and_dates():
    base_kwargs = {
        "declaration_number": "26/2026001-1",
        "registration_date": date(2026, 11, 10),
        "clearance_date": date(2026, 11, 11),
        "importer_tax_id": VALID_CNPJ,
        "tax_regime": "LUCRO_REAL",
        "currency_code": "USD",
        "fob_amount": "10000.0000",
        "freight_amount": "1000.0000",
        "insurance_amount": "200.0000",
        "ptax_exchange_rate": "5.800000",
        "ii_rate": "0.1200",
    }

    # Auto-computes CIF cleanly
    valid = CustomsNationalizationCalcInput(**base_kwargs)
    assert valid.cif_amount == Decimal("11200.0000")
    assert valid.importer_tax_id == VALID_CNPJ

    # Rejects invalid CNPJ Modulo-11 check digits & repeated digits
    for bad_cnpj in (INVALID_CNPJ, "11.111.111/1111-11", "CHE-123.456.788"):
        with pytest.raises(ValidationError, match="CNPJ"):
            CustomsNationalizationCalcInput(**{**base_kwargs, "importer_tax_id": bad_cnpj})

    # Rejects CIF closure drift > 0.0001
    with pytest.raises(ValidationError, match="CIF closure failed"):
        CustomsNationalizationCalcInput(**{**base_kwargs, "cif_amount": "11500.0000"})

    # Rejects clearance_date earlier than registration_date
    with pytest.raises(ValidationError, match="clearance_date"):
        CustomsNationalizationCalcInput(**{**base_kwargs, "clearance_date": date(2026, 11, 9)})

    # Rejects icms_rate >= 1.0000
    with pytest.raises(ValidationError):
        CustomsNationalizationCalcInput(**{**base_kwargs, "icms_rate": "1.0000"})


@pytest.mark.unit
def test_ledger_dtos_frozen_reversed_and_positive_original_currency():
    entry_id = uuid4()
    line1 = JournalLineInput(
        line_id=uuid4(),
        entry_id=entry_id,
        account_code="1.1.1.01",
        debit_amount="500.0000",
        credit_amount="0.0000",
        currency_code="BRL",
        original_currency_amount="500.0000",
        exchange_rate_applied="1.000000",
        line_order=1,
    )
    line2 = JournalLineInput(
        line_id=uuid4(),
        entry_id=entry_id,
        account_code="1.1.2.01",
        debit_amount="0.0000",
        credit_amount="500.0000",
        currency_code="BRL",
        original_currency_amount="500.0000",
        exchange_rate_applied="1.000000",
        line_order=2,
    )
    posted = JournalEntryInput(
        entry_id=entry_id,
        transaction_date=date(2026, 11, 10),
        competence_date=date(2026, 11, 10),
        reference_document_id="DOC-FROZEN-01",
        description="Immutability check",
        status="POSTED",
        lines=[line1, line2],
    )

    # 1. Frozen immutability
    with pytest.raises(ValidationError):
        line1.debit_amount = Decimal("999.0000")  # type: ignore[misc]
    with pytest.raises(ValidationError):
        posted.status = "REVERSED"  # type: ignore[misc]

    # 2. original_currency_amount must be strictly > 0.0000
    with pytest.raises(ValidationError):
        JournalLineInput(
            line_id=uuid4(),
            entry_id=entry_id,
            account_code="1.1.1.01",
            debit_amount="500.0000",
            credit_amount="0.0000",
            currency_code="BRL",
            original_currency_amount="0.0000",
            exchange_rate_applied="1.000000",
            line_order=1,
        )

    # 3. Duplicate line_order rejected
    with pytest.raises(ValidationError, match="Duplicate line_order"):
        JournalEntryInput(
            entry_id=entry_id,
            transaction_date=date(2026, 11, 10),
            competence_date=date(2026, 11, 10),
            description="Dup order",
            status="POSTED",
            lines=[
                line1,
                line2.model_copy(update={"line_order": 1}),
            ],
        )

    # 4. REVERSED state consistency rules
    with pytest.raises(ValidationError, match="requires reversed_from_entry_id"):
        JournalEntryInput(
            entry_id=uuid4(),
            transaction_date=date(2026, 11, 10),
            competence_date=date(2026, 11, 10),
            description="Missing ref",
            status="REVERSED",
            lines=[line1.model_copy(update={"entry_id": None}), line2.model_copy(update={"entry_id": None})],
        )

    with pytest.raises(ValidationError, match="cannot reference itself"):
        JournalEntryInput(
            entry_id=entry_id,
            reversed_from_entry_id=entry_id,
            transaction_date=date(2026, 11, 10),
            competence_date=date(2026, 11, 10),
            description="Self ref",
            status="REVERSED",
            lines=[line1, line2],
        )

    with pytest.raises(ValidationError, match="cannot set reversed_from_entry_id"):
        JournalEntryInput(
            entry_id=uuid4(),
            reversed_from_entry_id=entry_id,
            transaction_date=date(2026, 11, 10),
            competence_date=date(2026, 11, 10),
            description="Posted with reverse ref",
            status="POSTED",
            lines=[line1.model_copy(update={"entry_id": None}), line2.model_copy(update={"entry_id": None})],
        )

    # 5. Reversing a POSTED succeeds; reversing a REVERSED entry raises ValueError
    reversal = create_reversal_entry(posted, date(2026, 11, 11), "Correction")
    assert reversal.status == "REVERSED"
    assert reversal.reversed_from_entry_id == posted.entry_id
    with pytest.raises(ValueError, match="already REVERSED"):
        create_reversal_entry(reversal, date(2026, 11, 12), "Double reversal")


@pytest.mark.unit
def test_zero_fee_swift_and_zero_tax_nationalization_entries():
    # 1. Zero-fee SWIFT settlement creates 2 zero-sum legs cleanly (no zero-amount fee leg)
    swift_entry = build_swift_fee_split_entry(
        reference_document_id="SWIFT-NO-FEE-01",
        transaction_date=date(2026, 11, 10),
        competence_date=date(2026, 11, 10),
        gross_amount=Decimal("10000.0000"),
        fee_amount=Decimal("0.0000"),
        currency_code="USD",
        exchange_rate=Decimal("5.750000"),
    )
    assert len(swift_entry.lines) == 2
    assert swift_entry.lines[0].debit_amount == Decimal("57500.0000")
    assert swift_entry.lines[1].credit_amount == Decimal("57500.0000")

    # 2. Duty-free zero-tax nationalization creates 2 balanced legs cleanly
    calc_input = CustomsNationalizationCalcInput(
        declaration_number="26/ZERO-TAX-01",
        registration_date=date(2026, 11, 10),
        clearance_date=date(2026, 11, 11),
        importer_tax_id=VALID_CNPJ,
        tax_regime="CUSTO_INTEGRAL",
        currency_code="USD",
        fob_amount="20000.0000",
        freight_amount="1000.0000",
        insurance_amount="100.0000",
        ptax_exchange_rate="5.500000",
        ii_rate="0.0000",
        ipi_rate="0.0000",
        pis_rate="0.0000",
        cofins_rate="0.0000",
        siscomex_fee_brl="0.0000",
        afrmm_brl="0.0000",
        icms_rate="0.0000",
    )
    nat_result = compute_brazilian_landed_cost(calc_input)
    assert nat_result.total_taxes_brl == Decimal("0.0000")
    assert nat_result.customs_value_brl == nat_result.landed_cost_brl == Decimal("116050.0000")

    nat_entry = build_import_nationalization_ledger_entry(nat_result)
    assert len(nat_entry.lines) == 2
    assert nat_entry.lines[0].debit_amount == Decimal("116050.0000")
    assert nat_entry.lines[1].credit_amount == Decimal("116050.0000")


@pytest.mark.unit
def test_quarantine_ncm_country_pii_and_operation_hash_invariants():
    sha_sample = hashlib.sha256(b"payload").hexdigest()
    now_utc = datetime.now(timezone.utc)

    # 1. QuarantineRecordInput: REJECTED_HITL canonical enum + reviewer invariants
    rec = QuarantineRecordInput(
        source_type="INVOICE",
        raw_payload_hash=sha_sample,
        validation_errors=[{"msg": "Bad math"}],
        self_correction_attempts=3,
        review_status="REJECTED_HITL",
        reviewed_by="auditor@comex.br",
        reviewed_at=now_utc,
    )
    assert rec.review_status == "REJECTED_HITL"

    with pytest.raises(ValidationError, match="Invalid review_status"):
        QuarantineRecordInput(
            source_type="INVOICE",
            raw_payload_hash=sha_sample,
            validation_errors=["error"],
            review_status="REJECTED",
            reviewed_by="auditor@comex.br",
            reviewed_at=now_utc,
        )

    with pytest.raises(ValidationError, match="PENDING_REVIEW"):
        QuarantineRecordInput(
            source_type="INVOICE",
            raw_payload_hash=sha_sample,
            validation_errors=["error"],
            review_status="PENDING_REVIEW",
            reviewed_by="auditor@comex.br",
        )

    with pytest.raises(ValidationError, match="requires both reviewed_by and reviewed_at"):
        QuarantineRecordInput(
            source_type="INVOICE",
            raw_payload_hash=sha_sample,
            validation_errors=["error"],
            review_status="APPROVED_OVERRIDE",
            reviewed_by="auditor@comex.br",
            reviewed_at=None,
        )

    # 2. MerchandiseNCMInput & PartnerCountryInput
    ncm = MerchandiseNCMInput(
        ncm_code="84713012",
        hs_code="847130",
        description="Unidade de processamento digital",
    )
    assert ncm.unit_measure == "KG"
    with pytest.raises(ValidationError, match="hs_code"):
        MerchandiseNCMInput(
            ncm_code="84713012",
            hs_code="851713",
            description="Mismatch HS",
        )

    country = PartnerCountryInput(
        country_code=249,
        country_iso3="usa",
        country_name="Estados Unidos",
        economic_block="NAFTA",
    )
    assert country.country_iso3 == "USA"
    with pytest.raises(ValidationError, match="ISO 3166-1 alpha-3"):
        PartnerCountryInput(country_code=249, country_iso3="US1", country_name="Bad ISO")

    # 3. Salted PII hashing rejects blank/empty salt
    for bad_salt in ("", "   "):
        with pytest.raises(ValueError, match="(?i)salt"):
            hash_natural_person_pii("12345678909", salt=bad_salt)
    digest = hash_natural_person_pii("12345678909", salt="test-salt-2026")
    assert len(digest) == 64

    # 4. Delimiter-safe pipe hashing prevents boundary collisions
    h1 = compute_operation_hash_id(2026, 1, "IMPORT", "84713012", 24, "SP", 1, 817800)
    h2 = compute_operation_hash_id(2026, 1, "IMPORT", "84713012", 2, "SP", 41, 817800)
    assert h1 != h2
    expected_canonical = "2026|01|IMPORT|84713012|24|SP|1|817800"
    assert h1 == hashlib.sha256(expected_canonical.encode("utf-8")).hexdigest()


@pytest.mark.unit
def test_lakehouse_parquet_partition_merge_non_clobbering(tmp_path, monkeypatch):
    monkeypatch.setattr(lakehouse, "PARQUET_BASE_DIR", str(tmp_path / "lakehouse"))
    monkeypatch.setattr(lakehouse, "DUCKDB_PATH", str(tmp_path / "merge.duckdb"))

    imp_hash = compute_operation_hash_id(2026, 11, "IMPORT", "84713012", 249, "SP", 1, 817800)
    exp_hash = compute_operation_hash_id(2026, 11, "EXPORT", "10019900", 160, "RS", 1, 1017700)

    def _op_row(hash_id: str, op_type: str, ncm: str, cif: Decimal) -> dict:
        return {
            "year": 2026,
            "month": 11,
            "operation_hash_id": hash_id,
            "date_id": 20261101,
            "operation_type": op_type,
            "ncm_code": ncm,
            "country_code": 249,
            "uf_code": "SP",
            "modal_code": 1,
            "urf_code": 817800,
            "statistical_quantity": Decimal("100.0000"),
            "net_weight_kg": Decimal("200.0000"),
            "fob_amount_usd": cif,
            "freight_amount_usd": Decimal("0.0000"),
            "insurance_amount_usd": Decimal("0.0000"),
            "cif_amount_usd": cif,
            "landed_cost_brl": Decimal("0.0000"),
        }

    # 1. Write IMPORT into year=2026/month=11
    write_records_to_parquet_hive(
        [_op_row(imp_hash, "IMPORT", "84713012", Decimal("10000.0000"))],
        table_name="fact_comex_operations",
        partition_cols=["year", "month"],
        schema=FACT_COMEX_PARQUET_SCHEMA,
    )
    # 2. Write EXPORT into SAME year=2026/month=11 partition -> must NOT clobber IMPORT
    write_records_to_parquet_hive(
        [_op_row(exp_hash, "EXPORT", "10019900", Decimal("25000.0000"))],
        table_name="fact_comex_operations",
        partition_cols=["year", "month"],
        schema=FACT_COMEX_PARQUET_SCHEMA,
    )
    # 3. Re-upsert updated IMPORT row -> replaces existing IMPORT key without duplication
    write_records_to_parquet_hive(
        [_op_row(imp_hash, "IMPORT", "84713012", Decimal("12000.0000"))],
        table_name="fact_comex_operations",
        partition_cols=["year", "month"],
        schema=FACT_COMEX_PARQUET_SCHEMA,
    )

    parquet_glob = (Path(tmp_path) / "lakehouse" / "fact_comex_operations" / "**" / "*.parquet").as_posix()
    with duckdb_connection() as con:
        rows = con.execute(
            "SELECT operation_type, cif_amount_usd FROM read_parquet(?, hive_partitioning=1) ORDER BY operation_type",
            [parquet_glob],
        ).fetchall()

    assert rows == [
        ("EXPORT", Decimal("25000.0000")),
        ("IMPORT", Decimal("12000.0000")),
    ]


@pytest.mark.unit
def test_mdic_duckdb_ptax_no_lookahead_and_4xx_fail_fast(tmp_path, monkeypatch):
    monkeypatch.setattr(lakehouse, "PARQUET_BASE_DIR", str(tmp_path / "lakehouse"))
    monkeypatch.setattr(lakehouse, "DUCKDB_PATH", str(tmp_path / "ptax_nolookahead.duckdb"))

    # 1. Write observed rate for 20261106 (5.80) and FUTURE rate for 20261110 (6.50)
    ptax_schema = pa.schema(
        [
            ("date_id", pa.int32()),
            ("currency_code", pa.string()),
            ("ptax_buy_rate", pa.decimal128(19, 6)),
            ("ptax_sell_rate", pa.decimal128(19, 6)),
            ("is_business_day", pa.bool_()),
            ("is_interpolated_locf", pa.bool_()),
            ("year", pa.int32()),
            ("month", pa.int32()),
        ]
    )
    write_records_to_parquet_hive(
        [
            {
                "year": 2026,
                "month": 11,
                "date_id": 20261106,
                "currency_code": "USD",
                "ptax_buy_rate": Decimal("5.790000"),
                "ptax_sell_rate": Decimal("5.800000"),
                "is_business_day": True,
                "is_interpolated_locf": False,
            },
            {
                "year": 2026,
                "month": 11,
                "date_id": 20261110,
                "currency_code": "USD",
                "ptax_buy_rate": Decimal("6.490000"),
                "ptax_sell_rate": Decimal("6.500000"),
                "is_business_day": True,
                "is_interpolated_locf": False,
            },
        ],
        table_name="dim_currency_ptax",
        partition_cols=["year", "month"],
        schema=ptax_schema,
    )

    loader = MDICBulkLoader(offline=True, engine=None)
    # Querying 20261108 must use <= 20261108 (5.800000), NEVER look ahead to 20261110 (6.500000)
    assert loader.resolve_ptax_sell_rate(20261108) == Decimal("5.800000")
    # Exact date match
    assert loader.resolve_ptax_sell_rate(20261110) == Decimal("6.500000")
    # Before any recorded date falls back to statutory default instead of peeking into future
    assert loader.resolve_ptax_sell_rate(20261101) == DEFAULT_USD_PTAX_SELL_RATE

    # 2. HTTP 4xx (400 / 404) must fail fast without retry
    for status_code in (400, 404):
        attempts = {"count": 0}

        def handler(request: httpx.Request, code: int = status_code, counter: dict = attempts) -> httpx.Response:
            counter["count"] += 1
            return httpx.Response(status_code=code, json={"error": f"Client error {code}"})

        extractor = ResilientAPIExtractor(
            source_system="TEST_4XX",
            base_url="https://api.example.test",
            client=httpx.Client(transport=httpx.MockTransport(handler)),
        )
        with pytest.raises(httpx.HTTPStatusError):
            extractor.fetch_json("/resource")
        assert attempts["count"] == 1
