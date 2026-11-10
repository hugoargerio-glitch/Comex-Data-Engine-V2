"""
Comprehensive Test Suite: Comex HTAP & Fiduciary Engine V2 (ODCS v3.2.0)
Validates all Pydantic V2 models, anti-float guards, B2B tax ID verification (CNPJ Modulo 11 & Swiss UID),
vertical math closure, Incoterms, LOCF forward-fill, customs hash determinism,
append-only double-entry ledger invariants, statutory reversals, SWIFT split entries,
agentic self-correction loop, quarantine routing, DuckDB/Parquet OLAP, and FastAPI endpoints.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
from typing import Any, Dict
from uuid import uuid4

from fastapi.testclient import TestClient
from pydantic import ValidationError
import pytest

from src.api.main import app
from src.connectors.lakehouse import get_duckdb_connection, write_records_to_parquet_hive
from src.models.customs import (
    ComexOperationInput,
    CustomsNationalizationCalcInput,
    CustomsNationalizationResult,
    JournalEntryInput,
    JournalLineInput,
    PTAXRateInput,
    apply_locf_forward_fill,
    build_fx_liquidation_entry,
    build_import_nationalization_ledger_entry,
    build_swift_fee_split_entry,
    calculate_cascading_taxes,
    compute_brazilian_landed_cost,
    compute_operation_hash_id,
    create_reversal_entry,
)
from src.models.invoices import (
    CommercialInvoiceInput,
    CommercialInvoiceLineInput,
    hash_natural_person_pii,
    validate_cnpj,
    validate_corporate_tax_id,
)
from src.pipeline.comex_processor import ComexHTAPProcessor

client = TestClient(app)

# Known valid tax IDs for testing
VALID_CNPJ_FORMATTED = "33.000.167/0001-01"  # Petrobras
VALID_CNPJ_UNFORMATTED = "33000167000101"
INVALID_CNPJ_CHECK_DIGIT = "33.000.167/0001-99"
INVALID_CNPJ_ALL_SAME = "11.111.111/1111-11"
VALID_SWISS_UID = "CHE-123.456.788"
VALID_SWISS_UID_MWST = "CHE-123.456.788 MWST"
VALID_SWISS_UID_TVA = "CHE-123.456.788 TVA"
INVALID_SWISS_UID = "CHE-123.45"


# ==============================================================================
# Gate 1 & 2: Anti-Float & Monetary Precision Tests
# ==============================================================================

@pytest.mark.unit
def test_invoice_line_rejects_float_and_verifies_math():
    # 1. Reject float in quantity
    with pytest.raises(ValidationError) as exc:
        CommercialInvoiceLineInput(
            line_number=1,
            description="Steel Tubes",
            quantity=10.5,  # FLOAT!
            unit_price="100.0000",
            total_price="1050.0000",
        )
    assert "IEEE 754 float is strictly prohibited" in str(exc.value)

    # 2. Reject float in unit_price
    with pytest.raises(ValidationError) as exc:
        CommercialInvoiceLineInput(
            line_number=1,
            description="Steel Tubes",
            quantity="10.0000",
            unit_price=100.5,  # FLOAT!
            total_price="1005.0000",
        )
    assert "IEEE 754 float is strictly prohibited" in str(exc.value)

    # 3. Horizontal math failure (quantity * price != total)
    with pytest.raises(ValidationError) as exc:
        CommercialInvoiceLineInput(
            line_number=1,
            description="Steel Tubes",
            quantity="10.0000",
            unit_price="100.0000",
            total_price="999.0000",  # Errado! Deveria ser 1000.0000
        )
    assert "horizontal math failed" in str(exc.value)

    # 4. Valid line item
    line = CommercialInvoiceLineInput(
        line_number=1,
        description="Steel Tubes",
        quantity="10.0000",
        unit_price="100.0000",
        total_price="1000.0000",
    )
    assert line.total_price == Decimal("1000.0000")


@pytest.mark.unit
def test_commercial_invoice_rejects_float_across_all_monetary_fields():
    fields_to_test = [
        "subtotal_amount",
        "tax_amount",
        "freight_amount",
        "insurance_amount",
        "total_amount",
    ]
    for field in fields_to_test:
        kwargs: Dict[str, Any] = {
            "issuer_tax_id": VALID_CNPJ_FORMATTED,
            "document_number": "INV-2026-001",
            "issue_date": "2026-11-09",
            "currency_code": "USD",
            "incoterm": "FOB",
            "subtotal_amount": "1000.0000",
            "tax_amount": "0.0000",
            "freight_amount": "0.0000",
            "insurance_amount": "0.0000",
            "total_amount": "1000.0000",
        }
        kwargs[field] = 1000.50  # FLOAT!
        with pytest.raises(ValidationError) as exc:
            CommercialInvoiceInput(**kwargs)
        assert "IEEE 754 float is strictly prohibited" in str(exc.value)


@pytest.mark.unit
def test_invoice_scaled_cents_and_vertical_closure():
    # Valid invoice calculation
    inv = CommercialInvoiceInput(
        issuer_tax_id=VALID_CNPJ_FORMATTED,
        document_number="INV-2026-002",
        issue_date="2026-11-09",
        currency_code="USD",
        incoterm="EXW",
        subtotal_amount="1250.7500",
        tax_amount="50.2500",
        freight_amount="0.0000",
        insurance_amount="0.0000",
        total_amount="1301.0000",
    )
    # Scaled integer cents (* 10000)
    assert inv.subtotal_amount_cents == 12507500
    assert inv.tax_amount_cents == 502500
    assert inv.total_amount_cents == 13010000

    # Arithmetic mismatch (> 0.0001)
    with pytest.raises(ValidationError) as exc:
        CommercialInvoiceInput(
            issuer_tax_id=VALID_CNPJ_FORMATTED,
            document_number="INV-2026-003",
            issue_date="2026-11-09",
            currency_code="USD",
            incoterm="EXW",
            subtotal_amount="1000.0000",
            tax_amount="50.0000",
            freight_amount="0.0000",
            insurance_amount="0.0000",
            total_amount="1050.5000",  # Divergência de 0.5000!
        )
    assert "Invoice vertical closure failed" in str(exc.value)


@pytest.mark.unit
def test_cif_incoterm_requires_explicit_freight_and_insurance():
    # CIF missing freight
    with pytest.raises(ValidationError) as exc:
        CommercialInvoiceInput(
            issuer_tax_id=VALID_CNPJ_FORMATTED,
            document_number="INV-CIF-001",
            issue_date="2026-11-09",
            currency_code="EUR",
            incoterm="CIF",
            subtotal_amount="1000.0000",
            tax_amount="0.0000",
            freight_amount="0.0000",  # Zero freight in CIF!
            insurance_amount="50.0000",
            total_amount="1050.0000",
        )
    assert "CIF Incoterm requires explicit freight_amount and insurance_amount separation" in str(exc.value)

    # CIF with explicit freight and insurance -> Valid
    cif_inv = CommercialInvoiceInput(
        issuer_tax_id=VALID_CNPJ_FORMATTED,
        document_number="INV-CIF-002",
        issue_date="2026-11-09",
        currency_code="EUR",
        incoterm="CIF",
        subtotal_amount="1000.0000",
        tax_amount="0.0000",
        freight_amount="150.0000",
        insurance_amount="50.0000",
        total_amount="1200.0000",
    )
    assert cif_inv.total_amount == Decimal("1200.0000")


# ==============================================================================
# Gate 3: Corporate Tax ID (Cleartext) vs Natural Person PII Salted Hash
# ==============================================================================

@pytest.mark.unit
def test_corporate_tax_id_validation():
    # 1. Valid CNPJs
    assert validate_cnpj(VALID_CNPJ_FORMATTED) is True
    assert validate_cnpj(VALID_CNPJ_UNFORMATTED) is True
    assert validate_corporate_tax_id(VALID_CNPJ_FORMATTED) is True

    # 2. Invalid CNPJs
    assert validate_cnpj(INVALID_CNPJ_CHECK_DIGIT) is False
    assert validate_cnpj(INVALID_CNPJ_ALL_SAME) is False
    assert validate_corporate_tax_id(INVALID_CNPJ_CHECK_DIGIT) is False

    # 3. Swiss UIDs
    assert validate_corporate_tax_id(VALID_SWISS_UID) is True
    assert validate_corporate_tax_id(VALID_SWISS_UID_MWST) is True
    assert validate_corporate_tax_id(VALID_SWISS_UID_TVA) is True
    assert validate_corporate_tax_id(INVALID_SWISS_UID) is False

    # 4. Model rejects invalid corporate tax id
    with pytest.raises(ValidationError) as exc:
        CommercialInvoiceInput(
            issuer_tax_id="INVALID_TAX_ID_123",
            document_number="INV-BAD-TAX",
            issue_date="2026-11-09",
            currency_code="USD",
            incoterm="FOB",
            subtotal_amount="100.0000",
            total_amount="100.0000",
        )
    assert "Invalid corporate B2B tax ID" in str(exc.value)


@pytest.mark.unit
def test_salted_pii_hashing_and_corporate_cleartext():
    salt = "fiscal_audit_salt_2026"
    natural_person_cpf = "123.456.789-00"

    hashed_pii = hash_natural_person_pii(natural_person_cpf, salt)
    assert len(hashed_pii) == 64
    assert hashed_pii != natural_person_cpf

    # Deterministic with same salt
    assert hashed_pii == hash_natural_person_pii(natural_person_cpf, salt)

    # Different salt produces different hash
    assert hashed_pii != hash_natural_person_pii(natural_person_cpf, "different_salt")

    # Corporate tax ID must NOT be hashed
    inv = CommercialInvoiceInput(
        issuer_tax_id=VALID_SWISS_UID,
        document_number="INV-CHE-001",
        issue_date="2026-11-09",
        currency_code="CHF",
        incoterm="FOB",
        subtotal_amount="50000.0000",
        total_amount="50000.0000",
    )
    assert inv.issuer_tax_id == VALID_SWISS_UID  # Retained in cleartext!


# ==============================================================================
# Gate 4: PTAX Rates, No-Lookahead Bias & LOCF Forward-Fill
# ==============================================================================

@pytest.mark.unit
def test_ptax_rejects_float_and_future_dates():
    # 1. Rejects float
    with pytest.raises(ValidationError) as exc:
        PTAXRateInput(
            date_id=20261106,
            currency_code="USD",
            ptax_buy_rate=5.8245,  # FLOAT!
            ptax_sell_rate="5.825100",
            is_business_day=True,
        )
    assert "IEEE 754 float is strictly prohibited" in str(exc.value)

    # 2. Rejects future date (lookahead bias)
    future_date = datetime.now(timezone.utc).date() + timedelta(days=30)
    future_date_id = future_date.year * 10000 + future_date.month * 100 + future_date.day

    with pytest.raises(ValidationError) as exc:
        PTAXRateInput(
            date_id=future_date_id,
            currency_code="USD",
            ptax_buy_rate="5.800000",
            ptax_sell_rate="5.801000",
            is_business_day=True,
        )
    assert "Lookahead bias prohibited" in str(exc.value)

    # 3. Valid PTAX rate
    ptax = PTAXRateInput(
        date_id=20240105,
        currency_code="USD",
        ptax_buy_rate="4.862000",
        ptax_sell_rate="4.862600",
        is_business_day=True,
    )
    assert ptax.ptax_buy_rate == Decimal("4.862000")


@pytest.mark.unit
def test_apply_locf_forward_fill_weekend_and_no_lookahead():
    # Friday Jan 5, 2024 (Business day)
    rate_fri = PTAXRateInput(
        date_id=20240105,
        currency_code="USD",
        ptax_buy_rate="4.862000",
        ptax_sell_rate="4.862600",
        is_business_day=True,
        is_interpolated_locf=False,
    )

    start_date = date(2024, 1, 5)  # Friday
    end_date = date(2024, 1, 8)    # Monday

    filled = apply_locf_forward_fill([rate_fri], start_date, end_date, "USD")
    assert len(filled) == 4

    # Friday (Jan 5)
    assert filled[0].date_id == 20240105
    assert filled[0].is_business_day is True
    assert filled[0].is_interpolated_locf is False

    # Saturday (Jan 6) - LOCF from Friday
    assert filled[1].date_id == 20240106
    assert filled[1].is_business_day is False
    assert filled[1].is_interpolated_locf is True
    assert filled[1].ptax_buy_rate == Decimal("4.862000")

    # Sunday (Jan 7) - LOCF from Friday
    assert filled[2].date_id == 20240107
    assert filled[2].is_business_day is False
    assert filled[2].is_interpolated_locf is True
    assert filled[2].ptax_buy_rate == Decimal("4.862000")

    # Lookahead violation when no prior rate exists
    with pytest.raises(ValueError) as exc:
        apply_locf_forward_fill([], date(2024, 1, 6), date(2024, 1, 7), "USD")
    assert "Lookahead violation" in str(exc.value)


# ==============================================================================
# Gate 5: Customs Facts & Deterministic operation_hash_id
# ==============================================================================

@pytest.mark.unit
def test_comex_operation_hash_id_determinism_and_cif_greater_fob():
    # 1. Deterministic hash computation
    h1 = compute_operation_hash_id(2026, 11, "IMPORT", "10019900", 249, "SP", 1, 817800)
    h2 = compute_operation_hash_id(2026, 11, "IMPORT", "10019900", 249, "SP", 1, 817800)
    assert h1 == h2
    assert len(h1) == 64

    # 2. CIF < FOB raises ValidationError
    with pytest.raises(ValidationError) as exc:
        ComexOperationInput(
            date_id=20261101,
            operation_type="IMPORT",
            ncm_code="10019900",
            country_code=249,
            uf_code="SP",
            modal_code=1,
            urf_code=817800,
            fob_amount_usd="50000.0000",
            cif_amount_usd="40000.0000",  # CIF menor que FOB!
        )
    assert "cif_amount_usd" in str(exc.value) and "cannot be less than fob" in str(exc.value)

    # 3. Valid ComexOperationInput auto-computes operation_hash_id
    op = ComexOperationInput(
        date_id=20261101,
        operation_type="IMPORT",
        ncm_code="10019900",
        country_code=249,
        uf_code="SP",
        modal_code=1,
        urf_code=817800,
        statistical_quantity="1000.0000",
        net_weight_kg="1000.0000",
        fob_amount_usd="50000.0000",
        freight_amount_usd="5000.0000",
        insurance_amount_usd="1000.0000",
        cif_amount_usd="56000.0000",
        landed_cost_brl="313600.0000",
    )
    assert op.operation_hash_id == h1


# ==============================================================================
# Gate 6: Append-Only Double-Entry Fiduciary Ledger & Zero-Sum ACID
# ==============================================================================

@pytest.mark.unit
def test_journal_lines_exclusive_debit_credit_and_precision():
    # 1. Line with both debit and credit raises ValidationError
    with pytest.raises(ValidationError) as exc:
        JournalLineInput(
            account_code="1.1.1.01",
            debit_amount="100.0000",
            credit_amount="50.0000",  # Both > 0!
            currency_code="BRL",
            original_currency_amount="100.0000",
            line_order=1,
        )
    assert "cannot have both debit" in str(exc.value)

    # 2. Line with neither debit nor credit raises ValidationError
    with pytest.raises(ValidationError) as exc:
        JournalLineInput(
            account_code="1.1.1.01",
            debit_amount="0.0000",
            credit_amount="0.0000",  # Both 0!
            currency_code="BRL",
            original_currency_amount="0.0000",
            line_order=1,
        )
    assert "must have either debit > 0 or credit > 0" in str(exc.value)


@pytest.mark.unit
def test_journal_entry_zero_sum_invariant_and_reversal():
    entry_id = uuid4()
    line1 = JournalLineInput(
        account_code="1.1.3.01",  # Estoques em trânsito
        debit_amount="55000.0000",
        credit_amount="0.0000",
        currency_code="USD",
        original_currency_amount="10000.0000",
        exchange_rate_applied="5.500000",
        line_order=1,
    )
    line2 = JournalLineInput(
        account_code="2.1.1.01",  # Fornecedores
        debit_amount="0.0000",
        credit_amount="55000.0000",
        currency_code="USD",
        original_currency_amount="10000.0000",
        exchange_rate_applied="5.500000",
        line_order=2,
    )

    # Valid balanced entry
    entry = JournalEntryInput(
        entry_id=entry_id,
        transaction_date=date(2026, 11, 9),
        competence_date=date(2026, 11, 9),
        reference_document_id="INV-2026-999",
        description="Fatura Comercial de Importação",
        lines=[line1, line2],
    )
    assert entry.status == "POSTED"

    # Unbalanced entry raises ValidationError
    line_unbalanced = JournalLineInput(
        account_code="2.1.1.01",
        debit_amount="0.0000",
        credit_amount="50000.0000",  # Desequilíbrio de 5.000!
        currency_code="USD",
        original_currency_amount="10000.0000",
        line_order=2,
    )
    with pytest.raises(ValidationError) as exc:
        JournalEntryInput(
            transaction_date=date(2026, 11, 9),
            competence_date=date(2026, 11, 9),
            description="Lançamento Inválido",
            lines=[line1, line_unbalanced],
        )
    assert "zero-sum invariant violated" in str(exc.value)

    # Statutory Reversal Entry creation
    reversal = create_reversal_entry(
        original_entry=entry,
        reversal_date=date(2026, 11, 10),
        reason="Cancelamento de despacho aduaneiro",
    )
    assert reversal.status == "REVERSED"
    assert reversal.reversed_from_entry_id == entry.entry_id
    assert reversal.lines[0].credit_amount == Decimal("55000.0000")
    assert reversal.lines[1].debit_amount == Decimal("55000.0000")


# ==============================================================================
# Gate 7: FX Liquidation & SWIFT Fee Split Entry Builders
# ==============================================================================

@pytest.mark.unit
def test_build_fx_liquidation_gain_and_loss():
    # 1. Scenario: Realized FX Gain (Spot 5.65 > PTAX 5.50)
    entry_gain = build_fx_liquidation_entry(
        reference_document_id="EXP-INV-100",
        settlement_date=date(2026, 11, 9),
        competence_date=date(2026, 11, 1),
        foreign_amount=Decimal("10000.0000"),
        currency_code="USD",
        competence_rate=Decimal("5.500000"),
        spot_rate=Decimal("5.650000"),
    )
    assert len(entry_gain.lines) == 3
    # Cash Debit = 56,500.00
    assert entry_gain.lines[0].account_code == "1.1.1.01"
    assert entry_gain.lines[0].debit_amount == Decimal("56500.0000")
    # AR Credit = 55,000.00
    assert entry_gain.lines[1].account_code == "1.1.2.01"
    assert entry_gain.lines[1].credit_amount == Decimal("55000.0000")
    # FX Gain Credit = 1,500.00
    assert entry_gain.lines[2].account_code == "3.1.2.01"
    assert entry_gain.lines[2].credit_amount == Decimal("1500.0000")

    # 2. Scenario: Realized FX Loss (Spot 5.40 < PTAX 5.50)
    entry_loss = build_fx_liquidation_entry(
        reference_document_id="EXP-INV-101",
        settlement_date=date(2026, 11, 9),
        competence_date=date(2026, 11, 1),
        foreign_amount=Decimal("10000.0000"),
        currency_code="USD",
        competence_rate=Decimal("5.500000"),
        spot_rate=Decimal("5.400000"),
    )
    assert len(entry_loss.lines) == 3
    # Cash Debit = 54,000.00
    assert entry_loss.lines[0].debit_amount == Decimal("54000.0000")
    # FX Loss Debit = 1,000.00
    assert entry_loss.lines[1].account_code == "4.1.3.01"
    assert entry_loss.lines[1].debit_amount == Decimal("1000.0000")
    # AR Credit = 55,000.00
    assert entry_loss.lines[2].account_code == "1.1.2.01"
    assert entry_loss.lines[2].credit_amount == Decimal("55000.0000")


@pytest.mark.unit
def test_build_swift_fee_split_entry():
    # SWIFT wire of $10,000 USD with $30 intermediary fee at rate 5.50
    split_entry = build_swift_fee_split_entry(
        reference_document_id="SWIFT-WIRE-001",
        transaction_date=date(2026, 11, 9),
        competence_date=date(2026, 11, 2),
        gross_amount=Decimal("10000.0000"),
        fee_amount=Decimal("30.0000"),
        currency_code="USD",
        exchange_rate=Decimal("5.500000"),
    )
    assert len(split_entry.lines) == 3

    # Line 1: Cash Net Received = (10,000 - 30) * 5.50 = 54,835.00 BRL
    assert split_entry.lines[0].account_code == "1.1.1.01"
    assert split_entry.lines[0].debit_amount == Decimal("54835.0000")

    # Line 2: SWIFT Fee = 30 * 5.50 = 165.00 BRL
    assert split_entry.lines[1].account_code == "4.1.2.01"
    assert split_entry.lines[1].debit_amount == Decimal("165.0000")

    # Line 3: AR Gross Closed = 10,000 * 5.50 = 55,000.00 BRL
    assert split_entry.lines[2].account_code == "1.1.2.01"
    assert split_entry.lines[2].credit_amount == Decimal("55000.0000")


# ==============================================================================
# Gate 8: Agentic Self-Correction Loop & HITL Quarantine Routing
# ==============================================================================

@pytest.mark.unit
def test_agentic_self_correction_fixes_floats_and_whitespace():
    processor = ComexHTAPProcessor()

    # Payload with fixable defects (float numbers and whitespace strings)
    imperfect_payload = {
        "issuer_tax_id": f"  {VALID_CNPJ_FORMATTED}  ",
        "document_number": " INV-AUTO-FIX-01 ",
        "issue_date": "2026-11-09",
        "currency_code": " usd ",
        "incoterm": " fob ",
        "subtotal_amount": 2500.50,  # Float!
        "tax_amount": 0.0,           # Float!
        "freight_amount": 0.0,       # Float!
        "insurance_amount": 0.0,     # Float!
        "total_amount": 2500.50,     # Float!
    }

    validated, errors, attempts = processor.run_self_correction_loop(imperfect_payload, max_attempts=3)
    assert validated is not None
    assert attempts >= 1
    assert validated.issuer_tax_id == VALID_CNPJ_FORMATTED
    assert validated.currency_code == "USD"
    assert validated.incoterm == "FOB"
    assert validated.total_amount == Decimal("2500.5000")


@pytest.mark.unit
def test_agentic_self_correction_deflects_arithmetic_mismatch_to_quarantine():
    processor = ComexHTAPProcessor()

    # Fundamental arithmetic discrepancy ($200 drift) -> cannot be auto-coerced!
    mismatched_payload = {
        "issuer_tax_id": VALID_CNPJ_FORMATTED,
        "document_number": "INV-QUARANTINE-01",
        "issue_date": "2026-11-09",
        "currency_code": "USD",
        "incoterm": "FOB",
        "subtotal_amount": "5000.0000",
        "tax_amount": "0.0000",
        "freight_amount": "0.0000",
        "insurance_amount": "0.0000",
        "total_amount": "4800.0000",  # Divergência arimética real!
    }

    res = processor.process_invoice(mismatched_payload)
    assert res["status"] == "QUARANTINED"
    assert "quarantine_id" in res
    assert res["self_correction_attempts"] == 3


# ==============================================================================
# Gate 9: DuckDB / Parquet Columnar Lakehouse Persistence
# ==============================================================================

@pytest.mark.unit
def test_duckdb_parquet_comex_operations_roundtrip(tmp_path):
    import src.connectors.lakehouse as lakehouse
    old_base = lakehouse.PARQUET_BASE_DIR
    old_duckdb = lakehouse.DUCKDB_PATH
    lakehouse.PARQUET_BASE_DIR = str(tmp_path / "lakehouse")
    lakehouse.DUCKDB_PATH = str(tmp_path / "lakehouse.duckdb")

    try:
        processor = ComexHTAPProcessor()
        op = ComexOperationInput(
            date_id=20261101,
            operation_type="IMPORT",
            ncm_code="10019900",
            country_code=249,
            uf_code="SP",
            modal_code=1,
            urf_code=817800,
            statistical_quantity="500.0000",
            net_weight_kg="500.0000",
            fob_amount_usd="25000.0000",
            freight_amount_usd="2000.0000",
            insurance_amount_usd="500.0000",
            cif_amount_usd="27500.0000",
            landed_cost_brl="154000.0000",
        )

        processor.persist_comex_operations([op])

        parquet_glob = str(tmp_path / "lakehouse" / "fact_comex_operations" / "**" / "*.parquet")
        con = get_duckdb_connection()
        query = f"SELECT operation_type, SUM(cif_amount_usd) as total_cif FROM read_parquet('{parquet_glob}', hive_partitioning=1) GROUP BY operation_type"
        result = con.execute(query).fetchall()

        assert len(result) == 1
        assert result[0][0] == "IMPORT"
        assert result[0][1] == 27500.0
    finally:
        lakehouse.PARQUET_BASE_DIR = old_base
        lakehouse.DUCKDB_PATH = old_duckdb


# ==============================================================================
# Gate 10: FastAPI Comex V2 Endpoints
# ==============================================================================

@pytest.mark.unit
def test_api_v2_invoices_ingest_and_quarantine():
    # 1. Valid invoice
    valid_payload = {
        "issuer_tax_id": VALID_CNPJ_FORMATTED,
        "document_number": f"API-INV-{uuid4().hex[:8]}",
        "issue_date": "2026-11-09",
        "currency_code": "USD",
        "incoterm": "FOB",
        "subtotal_amount": "10000.0000",
        "tax_amount": "0.0000",
        "freight_amount": "0.0000",
        "insurance_amount": "0.0000",
        "total_amount": "10000.0000",
    }
    response = client.post("/api/v2/invoices/ingest", json=valid_payload)
    assert response.status_code == 201
    data = response.json()
    assert data["status"] in {"COMMITTED_ACID", "PROCESSED_MEM"}
    assert data["total_amount"] == "10000.0000"

    # 2. Arithmetic mismatch routes to Quarantine and returns 422
    bad_payload = {
        "issuer_tax_id": VALID_CNPJ_FORMATTED,
        "document_number": "API-INV-BAD",
        "issue_date": "2026-11-09",
        "currency_code": "USD",
        "incoterm": "FOB",
        "subtotal_amount": "10000.0000",
        "tax_amount": "0.0000",
        "freight_amount": "0.0000",
        "insurance_amount": "0.0000",
        "total_amount": "9500.0000",  # Divergent math!
    }
    response_bad = client.post("/api/v2/invoices/ingest", json=bad_payload)
    assert response_bad.status_code == 422
    detail = response_bad.json()["detail"]
    assert "HITL Quarantine" in detail["message"]
    assert "quarantine_id" in detail


@pytest.mark.unit
def test_api_v2_ledger_reconcile_swift():
    swift_request = {
        "reference_document_id": "DUIMP-2026-SWIFT-01",
        "transaction_date": "2026-11-09",
        "competence_date": "2026-11-01",
        "gross_amount": "20000.0000",
        "fee_amount": "50.0000",
        "currency_code": "USD",
        "exchange_rate": "5.600000",
    }
    response = client.post("/api/v2/ledger/reconcile-swift", json=swift_request)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "RECONCILED"
    assert data["lines_count"] == 3
    # Cash Net BRL = (20000 - 50) * 5.60 = 19950 * 5.60 = 111,720.00
    assert data["lines"][0]["debit_amount"] == "111720.0000"
    # SWIFT Fee BRL = 50 * 5.60 = 280.00
    assert data["lines"][1]["debit_amount"] == "280.0000"
    # AR Gross Closed BRL = 20000 * 5.60 = 112,000.00
    assert data["lines"][2]["credit_amount"] == "112000.0000"


# ==============================================================================
# Gate 11: Live BACEN Olinda PTAX Connector & LOCF Lakehouse Persistence
# ==============================================================================

@pytest.mark.unit
def test_bacen_ptax_parse_olinda_payload_fechamento_and_locf():
    import httpx
    from src.extractors.bacen_ptax_client import parse_olinda_ptax_items

    raw_payload_items = [
        # Friday bulletins: Abertura, Intermediário, Fechamento
        {
            "cotacaoCompra": 5.8201,
            "cotacaoVenda": 5.8207,
            "dataHoraCotacao": "2026-11-06 10:11:00.000",
            "tipoBoletim": "Abertura",
        },
        {
            "cotacaoCompra": 5.8215,
            "cotacaoVenda": 5.8221,
            "dataHoraCotacao": "2026-11-06 11:30:00.000",
            "tipoBoletim": "Intermediário",
        },
        {
            "cotacaoCompra": 5.8234,
            "cotacaoVenda": 5.8240,
            "dataHoraCotacao": "2026-11-06 13:08:12.123",
            "tipoBoletim": "Fechamento",
        },
        {
            "cotacaoCompra": 5.8230,
            "cotacaoVenda": 5.8236,
            "dataHoraCotacao": "2026-11-06 14:00:00.000",
            "tipoBoletim": "Intermediário",
        },
        # Monday bulletins without explicit tipoBoletim (latest dataHoraCotacao)
        {
            "cotacaoCompra": 5.8400,
            "cotacaoVenda": 5.8415,
            "dataHoraCotacao": "2026-11-09 10:00:00.000",
        },
        {
            "cotacaoCompra": 5.8450,
            "cotacaoVenda": 5.8465,
            "dataHoraCotacao": "2026-11-09 13:10:00.000",
        },
    ]

    start_date = date(2026, 11, 6)
    end_date = date(2026, 11, 9)

    rates = parse_olinda_ptax_items(raw_payload_items, "USD", start_date, end_date)
    assert len(rates) == 4

    # 1. Friday 2026-11-06: Fechamento chosen, quantized to Decimal("0.000001") without float drift
    r_fri = rates[0]
    assert r_fri.date_id == 20261106
    assert r_fri.currency_code == "USD"
    assert r_fri.is_business_day is True
    assert r_fri.is_interpolated_locf is False
    assert isinstance(r_fri.ptax_buy_rate, Decimal)
    assert isinstance(r_fri.ptax_sell_rate, Decimal)
    assert r_fri.ptax_buy_rate == Decimal("5.823400")
    assert r_fri.ptax_sell_rate == Decimal("5.824000")

    # 2. Saturday 2026-11-07: LOCF forward-fill across weekend
    r_sat = rates[1]
    assert r_sat.date_id == 20261107
    assert r_sat.is_business_day is False
    assert r_sat.is_interpolated_locf is True
    assert r_sat.ptax_buy_rate == Decimal("5.823400")
    assert r_sat.ptax_sell_rate == Decimal("5.824000")

    # 3. Sunday 2026-11-08: LOCF forward-fill across weekend
    r_sun = rates[2]
    assert r_sun.date_id == 20261108
    assert r_sun.is_business_day is False
    assert r_sun.is_interpolated_locf is True
    assert r_sun.ptax_buy_rate == Decimal("5.823400")
    assert r_sun.ptax_sell_rate == Decimal("5.824000")

    # 4. Monday 2026-11-09: Latest bulletin chosen
    r_mon = rates[3]
    assert r_mon.date_id == 20261109
    assert r_mon.is_business_day is True
    assert r_mon.is_interpolated_locf is False
    assert r_mon.ptax_buy_rate == Decimal("5.845000")
    assert r_mon.ptax_sell_rate == Decimal("5.846500")


@pytest.mark.unit
def test_bacen_ptax_resilient_retry_and_offline_fallback():
    import httpx
    from src.extractors.bacen_ptax_client import BacenPTAXClient

    attempts = {"count": 0}

    def failing_handler(request: httpx.Request) -> httpx.Response:
        attempts["count"] += 1
        raise httpx.ConnectError("Simulated Olinda network unreachable", request=request)

    mock_client = httpx.Client(transport=httpx.MockTransport(failing_handler))

    client = BacenPTAXClient(
        client=mock_client,
        timeout_seconds=0.01,
        offline_fallback=False,  # Not forced offline, verifies automatic fallback on network error
    )

    rates = client.fetch_ptax_for_currency(
        currency_code="USD",
        start_date=date(2026, 11, 6),
        end_date=date(2026, 11, 9),
    )

    assert attempts["count"] >= 1
    assert len(rates) == 4
    assert rates[0].date_id == 20261106
    assert rates[0].currency_code == "USD"
    assert rates[0].ptax_buy_rate == Decimal("5.823400")
    assert rates[0].ptax_sell_rate == Decimal("5.824000")
    assert rates[1].date_id == 20261107
    assert rates[1].is_interpolated_locf is True


@pytest.mark.unit
def test_sync_ptax_period_lakehouse_decimal19_6(tmp_path):
    import src.connectors.lakehouse as lakehouse
    from src.extractors.bacen_ptax_client import BacenPTAXClient

    old_base = lakehouse.PARQUET_BASE_DIR
    old_duckdb = lakehouse.DUCKDB_PATH
    lakehouse.PARQUET_BASE_DIR = str(tmp_path / "lakehouse")
    lakehouse.DUCKDB_PATH = str(tmp_path / "test_ptax.duckdb")

    try:
        client = BacenPTAXClient(offline_fallback=True)

        rates = client.sync_ptax_period(
            start_date=date(2026, 11, 6),
            end_date=date(2026, 11, 9),
            currencies=["USD", "EUR", "CHF"],
        )

        assert len(rates) == 12  # 4 days * 3 currencies

        parquet_glob = str(tmp_path / "lakehouse" / "dim_currency_ptax" / "**" / "*.parquet")
        con = get_duckdb_connection()

        # 1. Verify schema types in DuckDB: ptax_buy_rate & ptax_sell_rate are DECIMAL(19,6)
        schema_rows = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=1)"
        ).fetchall()
        schema_dict = {row[0]: row[1] for row in schema_rows}
        assert schema_dict["ptax_buy_rate"] == "DECIMAL(19,6)"
        assert schema_dict["ptax_sell_rate"] == "DECIMAL(19,6)"

        # 2. Verify all 3 currencies and row counts
        curr_counts = con.execute(
            f"SELECT currency_code, COUNT(*), MIN(ptax_buy_rate), MAX(ptax_sell_rate) "
            f"FROM read_parquet('{parquet_glob}', hive_partitioning=1) "
            f"GROUP BY currency_code ORDER BY currency_code"
        ).fetchall()
        assert len(curr_counts) == 3
        for row in curr_counts:
            assert row[1] == 4

        # 3. Verify LOCF rows are preserved in Parquet
        locf_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{parquet_glob}', hive_partitioning=1) WHERE is_interpolated_locf = true"
        ).fetchone()[0]
        assert locf_count == 6  # 2 weekend days * 3 currencies
    finally:
        lakehouse.PARQUET_BASE_DIR = old_base
        lakehouse.DUCKDB_PATH = old_duckdb


# ==============================================================================
# Gate 8: MDIC Bulk Ingestion, Pre-Aggregation & Analytical OLAP Tests
# ==============================================================================

@pytest.mark.unit
def test_mdic_imp_semicolon_preaggregation_and_valuation():
    from src.extractors.mdic_bulk_loader import MDICBulkLoader
    from pathlib import Path

    sample_imp = Path("samples/mdic_imp_sample.csv")
    assert sample_imp.exists()

    loader = MDICBulkLoader(offline=True)
    ops = loader.parse_and_preaggregate_mdic_csv(csv_path=sample_imp, flow="IMPORT", month=1)

    # 4 rows in sample: 2 for 84713012 in month 1, 1 for 30049099 in month 1, 1 in month 2
    # Month 1 filter -> 2 pre-aggregated operations
    assert len(ops) == 2

    op_laptop = next((op for op in ops if op.ncm_code == "84713012"), None)
    assert op_laptop is not None

    # Pre-aggregation sum across CO_UNID=10 and CO_UNID=11
    # Row 1: QT=300, KG=800, FOB=250000, FREIGHT=15000, INSURANCE=2000
    # Row 2: QT=200, KG=450, FOB=200000, FREIGHT=10000, INSURANCE=1500
    assert op_laptop.statistical_quantity == Decimal("500.0000")
    assert op_laptop.net_weight_kg == Decimal("1250.0000")
    assert op_laptop.fob_amount_usd == Decimal("450000.0000")
    assert op_laptop.freight_amount_usd == Decimal("25000.0000")
    assert op_laptop.insurance_amount_usd == Decimal("3500.0000")
    assert op_laptop.cif_amount_usd == Decimal("478500.0000")

    # Anti-float assertion
    for field in [
        "statistical_quantity",
        "net_weight_kg",
        "fob_amount_usd",
        "freight_amount_usd",
        "insurance_amount_usd",
        "cif_amount_usd",
        "landed_cost_brl",
    ]:
        val = getattr(op_laptop, field)
        assert isinstance(val, Decimal)
        assert not isinstance(val, float)

    # Landed cost calculation: cif * ptax_sell_rate
    expected_landed = (op_laptop.cif_amount_usd * loader.resolve_ptax_sell_rate(op_laptop.date_id)).quantize(
        Decimal("0.0001")
    )
    assert op_laptop.landed_cost_brl == expected_landed

    # Deterministic SHA-256 hash
    expected_hash = compute_operation_hash_id(
        ano=2024,
        mes=1,
        tipo_op="IMPORT",
        co_ncm="84713012",
        co_pais=249,
        sg_uf="SP",
        co_via=1,
        co_urf=817800,
    )
    assert op_laptop.operation_hash_id == expected_hash
    assert len(op_laptop.operation_hash_id) == 64


@pytest.mark.unit
def test_mdic_exp_semicolon_defaults_freight_and_insurance():
    from src.extractors.mdic_bulk_loader import MDICBulkLoader
    from pathlib import Path

    sample_exp = Path("samples/mdic_exp_sample.csv")
    assert sample_exp.exists()

    loader = MDICBulkLoader(offline=True)
    ops = loader.parse_and_preaggregate_mdic_csv(csv_path=sample_exp, flow="EXPORT", month=1)

    # Month 1 filter -> 2 operations (09011110 and 85171300)
    assert len(ops) == 2

    for op in ops:
        assert op.operation_type == "EXPORT"
        # Export declarations default freight & insurance to Decimal("0.0000")
        assert op.freight_amount_usd == Decimal("0.0000")
        assert op.insurance_amount_usd == Decimal("0.0000")
        # cif_amount_usd == fob_amount_usd
        assert op.cif_amount_usd == op.fob_amount_usd
        assert op.landed_cost_brl == (op.cif_amount_usd * loader.resolve_ptax_sell_rate(op.date_id)).quantize(
            Decimal("0.0001")
        )
        assert len(op.operation_hash_id) == 64


@pytest.mark.unit
def test_mdic_filters_and_duckdb_parquet_persistence(tmp_path):
    from pathlib import Path
    import src.connectors.lakehouse as lakehouse
    from src.extractors.mdic_bulk_loader import MDICBulkLoader

    old_base = lakehouse.PARQUET_BASE_DIR
    old_duckdb = lakehouse.DUCKDB_PATH
    lakehouse.PARQUET_BASE_DIR = str(tmp_path / "lakehouse")
    lakehouse.DUCKDB_PATH = str(tmp_path / "test_mdic.duckdb")

    try:
        loader = MDICBulkLoader(offline=True)
        sample_imp = Path("samples/mdic_imp_sample.csv")

        # 1. Test filtering by NCM prefix
        ops_ncm = loader.parse_and_preaggregate_mdic_csv(csv_path=sample_imp, ncm_prefix="84")
        assert all(op.ncm_code.startswith("84") for op in ops_ncm)
        assert len(ops_ncm) >= 1

        # 2. Test filtering by UF
        ops_uf = loader.parse_and_preaggregate_mdic_csv(csv_path=sample_imp, uf="RJ")
        assert all(op.uf_code == "RJ" for op in ops_uf)
        assert len(ops_uf) == 1
        assert ops_uf[0].ncm_code == "30049099"

        # 3. Test filtering by month
        ops_m2 = loader.parse_and_preaggregate_mdic_csv(csv_path=sample_imp, month=2)
        assert len(ops_m2) == 1
        assert ops_m2[0].date_id == 20240201

        # 4. Test Hybrid Persistence into Parquet Hive Lakehouse
        all_ops = loader.parse_and_preaggregate_mdic_csv(csv_path=sample_imp)
        persist_stats = loader.sync_mdic_operations(all_ops, limit_pg=10)
        assert persist_stats["parquet_persisted"] == len(all_ops)

        # 5. Verify Parquet Lakehouse schema & types in DuckDB: all monetary columns are DECIMAL(19,4)
        con = get_duckdb_connection()
        parquet_glob = str(tmp_path / "lakehouse" / "fact_comex_operations" / "**" / "*.parquet")
        schema_rows = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=1)"
        ).fetchall()
        schema_dict = {row[0]: row[1] for row in schema_rows}

        decimal_fields = [
            "statistical_quantity",
            "net_weight_kg",
            "fob_amount_usd",
            "freight_amount_usd",
            "insurance_amount_usd",
            "cif_amount_usd",
            "landed_cost_brl",
        ]
        for field in decimal_fields:
            assert schema_dict[field] == "DECIMAL(19,4)", f"{field} type is {schema_dict[field]}"

        # 6. Test offline fallback via fetch_and_load
        result = loader.fetch_and_load(year=2026, month=11, flow="BOTH")
        assert result["persistence"]["total_operations"] == 4
        assert result["persistence"]["parquet_persisted"] == 4
    finally:
        lakehouse.PARQUET_BASE_DIR = old_base
        lakehouse.DUCKDB_PATH = old_duckdb


# ==============================================================================
# Gate 12: Sprint Real 3 — Brazilian Customs Cascading Tax Engine & CPC 16
# ==============================================================================

@pytest.mark.unit
def test_brazilian_cascading_taxes_lucro_real_and_custo_integral():
    # 1. Anti-float validation checks
    float_test_cases = [
        {"fob_amount": 50000.0},
        {"ptax_exchange_rate": 5.8234},
        {"ii_rate": 0.12},
        {"ipi_rate": 0.10},
        {"pis_rate": 0.021},
        {"cofins_rate": 0.0965},
        {"icms_rate": 0.18},
    ]
    base_kwargs = {
        "declaration_number": "26/1948201-3",
        "registration_date": date(2026, 11, 6),
        "clearance_date": date(2026, 11, 7),
        "importer_tax_id": VALID_CNPJ_FORMATTED,
        "tax_regime": "LUCRO_REAL",
        "currency_code": "USD",
        "fob_amount": Decimal("50000.0000"),
        "freight_amount": Decimal("3000.0000"),
        "insurance_amount": Decimal("500.0000"),
        "ptax_exchange_rate": Decimal("5.823400"),
        "ii_rate": Decimal("0.1200"),
        "ipi_rate": Decimal("0.1000"),
        "pis_rate": Decimal("0.0210"),
        "cofins_rate": Decimal("0.0965"),
        "siscomex_fee_brl": Decimal("115.0000"),
        "afrmm_brl": Decimal("0.0000"),
        "icms_rate": Decimal("0.1800"),
    }

    for override in float_test_cases:
        test_kwargs = base_kwargs.copy()
        test_kwargs.update(override)
        with pytest.raises(ValidationError) as exc:
            CustomsNationalizationCalcInput(**test_kwargs)
        assert "IEEE 754 float is strictly prohibited" in str(exc.value)

    # 2. Strict Lucro Real Cascading Tax Math
    calc_in = CustomsNationalizationCalcInput(**base_kwargs)
    res = compute_brazilian_landed_cost(calc_in)

    # CIF = 50,000 + 3,000 + 500 = 53,500.0000
    assert res.cif_amount == Decimal("53500.0000")

    # VA = 53,500 * 5.8234 = 311,551.9000
    assert res.customs_value_brl == Decimal("311551.9000")

    # II = 311,551.9000 * 0.12 = 37,386.2280
    assert res.ii_amount_brl == Decimal("37386.2280")

    # IPI = (311,551.9000 + 37,386.2280) * 0.10 = 348,938.1280 * 0.10 = 34,893.8128
    assert res.ipi_amount_brl == Decimal("34893.8128")

    # PIS = 311,551.9000 * 0.0210 = 6,542.5899
    assert res.pis_amount_brl == Decimal("6542.5899")

    # COFINS = 311,551.9000 * 0.0965 = 30,064.7584
    assert res.cofins_amount_brl == Decimal("30064.7584")

    # Siscomex & AFRMM
    assert res.siscomex_fee_brl == Decimal("115.0000")
    assert res.afrmm_brl == Decimal("0.0000")

    # ICMS Base "Cálculo por Dentro"
    # Numerator = 311,551.9000 + 37,386.2280 + 34,893.8128 + 6,542.5899 + 30,064.7584 + 115.0000 = 420,554.2891
    # Divisor = 1 - 0.18 = 0.82
    # Base = 420,554.2891 / 0.82 = 512,871.0843
    assert res.icms_base_brl == Decimal("512871.0843")

    # ICMS = 512,871.0843 * 0.18 = 92,316.7952
    assert res.icms_amount_brl == Decimal("92316.7952")

    # Total Taxes = II + IPI + PIS + COFINS + Siscomex + AFRMM + ICMS
    # = 37386.2280 + 34893.8128 + 6542.5899 + 30064.7584 + 115.0000 + 0 + 92316.7952 = 201,319.1843
    assert res.total_taxes_brl == Decimal("201319.1843")

    # Landed Cost = VA + Total Taxes = 311,551.9000 + 201,319.1843 = 512,871.0843
    assert res.landed_cost_brl == Decimal("512871.0843")

    # CPC 16 Segregation under LUCRO_REAL
    # Net Inventory Cost = VA + II + Siscomex + AFRMM = 311,551.9000 + 37,386.2280 + 115.0000 = 349,053.1280
    assert res.net_inventory_cost_brl == Decimal("349053.1280")

    # Recoverable Taxes = IPI + PIS + COFINS + ICMS = 34,893.8128 + 6,542.5899 + 30,064.7584 + 92,316.7952 = 163,817.9563
    assert res.recoverable_taxes_brl == Decimal("163817.9563")

    # Critical Invariant: Net Inventory + Recoverable Taxes == Landed Cost
    assert res.net_inventory_cost_brl + res.recoverable_taxes_brl == res.landed_cost_brl

    # Test alias
    assert calculate_cascading_taxes(calc_in).landed_cost_brl == res.landed_cost_brl

    # 3. CUSTO_INTEGRAL (All taxes capitalized, zero recoverable tax asset)
    custo_kwargs = base_kwargs.copy()
    custo_kwargs["tax_regime"] = "CUSTO_INTEGRAL"
    calc_in_custo = CustomsNationalizationCalcInput(**custo_kwargs)
    res_custo = compute_brazilian_landed_cost(calc_in_custo)

    assert res_custo.tax_regime == "CUSTO_INTEGRAL"
    assert res_custo.landed_cost_brl == Decimal("512871.0843")
    assert res_custo.net_inventory_cost_brl == res_custo.landed_cost_brl
    assert res_custo.recoverable_taxes_brl == Decimal("0.0000")


@pytest.mark.unit
def test_import_nationalization_ledger_entry_zero_sum():
    # 1. LUCRO_REAL: 4-legged entry
    calc_lr = CustomsNationalizationCalcInput(
        declaration_number="26/1948201-3",
        registration_date=date(2026, 11, 6),
        clearance_date=date(2026, 11, 7),
        importer_tax_id=VALID_CNPJ_FORMATTED,
        tax_regime="LUCRO_REAL",
        currency_code="USD",
        fob_amount=Decimal("50000.0000"),
        freight_amount=Decimal("3000.0000"),
        insurance_amount=Decimal("500.0000"),
        ptax_exchange_rate=Decimal("5.823400"),
        ii_rate=Decimal("0.1200"),
        ipi_rate=Decimal("0.1000"),
        pis_rate=Decimal("0.0210"),
        cofins_rate=Decimal("0.0965"),
        siscomex_fee_brl=Decimal("115.0000"),
        afrmm_brl=Decimal("0.0000"),
        icms_rate=Decimal("0.1800"),
    )
    res_lr = compute_brazilian_landed_cost(calc_lr)
    entry_lr = build_import_nationalization_ledger_entry(res_lr)

    assert entry_lr.status == "POSTED"
    assert len(entry_lr.lines) == 4

    # Line 1: Debit 1.1.3.02 (Inventory Available)
    assert entry_lr.lines[0].account_code == "1.1.3.02"
    assert entry_lr.lines[0].debit_amount == Decimal("349053.1280")
    assert entry_lr.lines[0].credit_amount == Decimal("0.0000")

    # Line 2: Debit 1.1.4.01 (Recoverable Taxes Asset)
    assert entry_lr.lines[1].account_code == "1.1.4.01"
    assert entry_lr.lines[1].debit_amount == Decimal("163817.9563")
    assert entry_lr.lines[1].credit_amount == Decimal("0.0000")

    # Line 3: Credit 1.1.3.01 (Inventory in Transit)
    assert entry_lr.lines[2].account_code == "1.1.3.01"
    assert entry_lr.lines[2].debit_amount == Decimal("0.0000")
    assert entry_lr.lines[2].credit_amount == Decimal("311551.9000")

    # Line 4: Credit 2.1.2.01 (Customs Duties Liabilities)
    assert entry_lr.lines[3].account_code == "2.1.2.01"
    assert entry_lr.lines[3].debit_amount == Decimal("0.0000")
    assert entry_lr.lines[3].credit_amount == Decimal("201319.1843")

    total_debits_lr = sum(l.debit_amount for l in entry_lr.lines)
    total_credits_lr = sum(l.credit_amount for l in entry_lr.lines)
    assert total_debits_lr == total_credits_lr == Decimal("512871.0843")
    assert abs(total_debits_lr - total_credits_lr) == Decimal("0.0000")

    # 2. CUSTO_INTEGRAL: 3-legged entry
    calc_ci = CustomsNationalizationCalcInput(
        declaration_number="26BR00008491",
        registration_date=date(2026, 11, 7),
        clearance_date=date(2026, 11, 9),
        importer_tax_id=VALID_CNPJ_FORMATTED,
        tax_regime="CUSTO_INTEGRAL",
        currency_code="USD",
        fob_amount=Decimal("75000.0000"),
        freight_amount=Decimal("4000.0000"),
        insurance_amount=Decimal("800.0000"),
        ptax_exchange_rate=Decimal("5.823400"),
        ii_rate=Decimal("0.1600"),
        ipi_rate=Decimal("0.1200"),
        pis_rate=Decimal("0.0210"),
        cofins_rate=Decimal("0.0965"),
        siscomex_fee_brl=Decimal("115.0000"),
        afrmm_brl=Decimal("0.0000"),
        icms_rate=Decimal("0.1800"),
    )
    res_ci = compute_brazilian_landed_cost(calc_ci)
    entry_ci = build_import_nationalization_ledger_entry(res_ci)

    assert entry_ci.status == "POSTED"
    assert len(entry_ci.lines) == 3

    # Line 1: Debit 1.1.3.02 with full landed cost
    assert entry_ci.lines[0].account_code == "1.1.3.02"
    assert entry_ci.lines[0].debit_amount == res_ci.landed_cost_brl
    assert entry_ci.lines[0].credit_amount == Decimal("0.0000")

    # Line 2: Credit 1.1.3.01 with customs value (transit clearance)
    assert entry_ci.lines[1].account_code == "1.1.3.01"
    assert entry_ci.lines[1].debit_amount == Decimal("0.0000")
    assert entry_ci.lines[1].credit_amount == res_ci.customs_value_brl

    # Line 3: Credit 2.1.2.01 with total taxes
    assert entry_ci.lines[2].account_code == "2.1.2.01"
    assert entry_ci.lines[2].debit_amount == Decimal("0.0000")
    assert entry_ci.lines[2].credit_amount == res_ci.total_taxes_brl

    total_debits_ci = sum(l.debit_amount for l in entry_ci.lines)
    total_credits_ci = sum(l.credit_amount for l in entry_ci.lines)
    assert total_debits_ci == total_credits_ci == res_ci.landed_cost_brl
    assert abs(total_debits_ci - total_credits_ci) == Decimal("0.0000")


@pytest.mark.unit
def test_api_v2_landed_cost_simulation_endpoint():
    # 1. Valid Simulation via FastAPI TestClient
    payload = {
        "declaration_number": "26/1948201-3",
        "registration_date": "2026-11-06",
        "clearance_date": "2026-11-07",
        "importer_tax_id": VALID_CNPJ_FORMATTED,
        "tax_regime": "LUCRO_REAL",
        "currency_code": "USD",
        "fob_amount": "50000.0000",
        "freight_amount": "3000.0000",
        "insurance_amount": "500.0000",
        "ptax_exchange_rate": "5.823400",
        "ii_rate": "0.1200",
        "ipi_rate": "0.1000",
        "pis_rate": "0.0210",
        "cofins_rate": "0.0965",
        "siscomex_fee_brl": "115.0000",
        "afrmm_brl": "0.0000",
        "icms_rate": "0.1800",
    }
    response = client.post("/api/v2/analytics/landed-cost-sim", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "SIMULATED"
    calc = data["calculation"]
    assert calc["declaration_number"] == "26/1948201-3"
    assert calc["customs_value_brl"] == "311551.9000"
    assert calc["landed_cost_brl"] == "512871.0843"
    assert calc["net_inventory_cost_brl"] == "349053.1280"
    assert calc["recoverable_taxes_brl"] == "163817.9563"

    journal = data["journal_entry"]
    assert journal["status"] == "POSTED"
    assert journal["lines_count"] == 4
    assert journal["lines"][0]["account_code"] == "1.1.3.02"
    assert journal["lines"][0]["debit_amount"] == "349053.1280"
    assert journal["lines"][1]["account_code"] == "1.1.4.01"
    assert journal["lines"][1]["debit_amount"] == "163817.9563"
    assert journal["lines"][2]["account_code"] == "1.1.3.01"
    assert journal["lines"][2]["credit_amount"] == "311551.9000"
    assert journal["lines"][3]["account_code"] == "2.1.2.01"
    assert journal["lines"][3]["credit_amount"] == "201319.1843"

    # 2. Reject float inputs via API
    bad_float_payload = payload.copy()
    bad_float_payload["fob_amount"] = 50000.50  # Float!
    response_bad_float = client.post("/api/v2/analytics/landed-cost-sim", json=bad_float_payload)
    assert response_bad_float.status_code == 422

    # 3. Reject invalid tax regime
    bad_regime_payload = payload.copy()
    bad_regime_payload["tax_regime"] = "INVALID_REGIME"
    response_bad_regime = client.post("/api/v2/analytics/landed-cost-sim", json=bad_regime_payload)
    assert response_bad_regime.status_code == 422


# ==============================================================================
# Gate 13: Sprint Real 4 — HITL Quarantine Resolution Workflow & API
# ==============================================================================

@pytest.mark.unit
def test_hitl_quarantine_resolution_approved_override_and_rejected():
    processor = ComexHTAPProcessor()

    # 1. Deflect two bad invoices to quarantine
    # Bad Invoice 1: Vertical arithmetic mismatch ($100 drift)
    bad_inv_1 = {
        "issuer_tax_id": VALID_CNPJ_FORMATTED,
        "document_number": f"INV-HITL-ERR1-{uuid4().hex[:6]}",
        "issue_date": "2026-11-09",
        "currency_code": "USD",
        "incoterm": "FOB",
        "subtotal_amount": "10000.0000",
        "tax_amount": "0.0000",
        "freight_amount": "0.0000",
        "insurance_amount": "0.0000",
        "total_amount": "9900.0000",  # Math drift!
    }
    # Bad Invoice 2: CIF missing explicit freight & insurance separation
    bad_inv_2 = {
        "issuer_tax_id": VALID_SWISS_UID,
        "document_number": f"INV-HITL-ERR2-{uuid4().hex[:6]}",
        "issue_date": "2026-11-09",
        "currency_code": "CHF",
        "incoterm": "CIF",
        "subtotal_amount": "30000.0000",
        "tax_amount": "0.0000",
        "freight_amount": "0.0000",  # CIF requires freight > 0!
        "insurance_amount": "0.0000",  # CIF requires insurance > 0!
        "total_amount": "30000.0000",
    }

    res1 = processor.process_invoice(bad_inv_1)
    res2 = processor.process_invoice(bad_inv_2)

    assert res1["status"] == "QUARANTINED"
    assert res2["status"] == "QUARANTINED"
    qid1 = res1["quarantine_id"]
    qid2 = res2["quarantine_id"]

    # 2. Verify both pending in quarantine queue
    pending_items = processor.get_pending_quarantine_items(status_filter="PENDING_REVIEW")
    pending_ids = [item["quarantine_id"] for item in pending_items]
    assert qid1 in pending_ids
    assert qid2 in pending_ids

    # 3. Test invalid resolution parameters & guards
    # A. Invalid resolution action
    with pytest.raises(ValueError) as exc:
        processor.resolve_quarantined_invoice(
            quarantine_id=qid1,
            resolution_action="INVALID_ACTION",
            reviewed_by="auditor@comex.br",
        )
    assert "Invalid resolution_action" in str(exc.value)

    # B. Empty reviewed_by
    with pytest.raises(ValueError) as exc:
        processor.resolve_quarantined_invoice(
            quarantine_id=qid1,
            resolution_action="APPROVED_OVERRIDE",
            reviewed_by="",
            corrected_payload={},
        )
    assert "reviewed_by must be a non-empty string" in str(exc.value)

    # C. APPROVED_OVERRIDE with None corrected_payload
    with pytest.raises(ValueError) as exc:
        processor.resolve_quarantined_invoice(
            quarantine_id=qid1,
            resolution_action="APPROVED_OVERRIDE",
            reviewed_by="auditor@comex.br",
            corrected_payload=None,
        )
    assert "corrected_payload is required" in str(exc.value)

    # D. APPROVED_OVERRIDE with still-invalid payload (bad math) raises ValidationError without modifying status!
    still_broken_payload = {
        "issuer_tax_id": VALID_CNPJ_FORMATTED,
        "document_number": bad_inv_1["document_number"],
        "issue_date": "2026-11-09",
        "currency_code": "USD",
        "incoterm": "FOB",
        "subtotal_amount": "10000.0000",
        "tax_amount": "0.0000",
        "freight_amount": "0.0000",
        "insurance_amount": "0.0000",
        "total_amount": "8888.0000",  # Still wrong!
    }
    with pytest.raises((ValidationError, ValueError)):
        processor.resolve_quarantined_invoice(
            quarantine_id=qid1,
            resolution_action="APPROVED_OVERRIDE",
            reviewed_by="auditor@comex.br",
            corrected_payload=still_broken_payload,
        )

    # Verify qid1 status was not altered by the failed validation
    items_after_fail = processor.get_pending_quarantine_items(status_filter="PENDING_REVIEW")
    assert qid1 in [it["quarantine_id"] for it in items_after_fail]

    # 4. Resolve qid1 via APPROVED_OVERRIDE with valid corrected payload
    valid_corrected_payload_1 = {
        "issuer_tax_id": VALID_CNPJ_FORMATTED,
        "document_number": bad_inv_1["document_number"],
        "issue_date": "2026-11-09",
        "currency_code": "USD",
        "incoterm": "FOB",
        "subtotal_amount": "10000.0000",
        "tax_amount": "0.0000",
        "freight_amount": "0.0000",
        "insurance_amount": "0.0000",
        "total_amount": "10000.0000",  # Reconciled math!
    }
    resolved_1 = processor.resolve_quarantined_invoice(
        quarantine_id=qid1,
        resolution_action="APPROVED_OVERRIDE",
        reviewed_by="auditor.lead@comex.br",
        corrected_payload=valid_corrected_payload_1,
    )
    assert resolved_1["quarantine_id"] == qid1
    assert resolved_1["review_status"] == "APPROVED_OVERRIDE"
    assert resolved_1["reviewed_by"] == "auditor.lead@comex.br"
    assert resolved_1["invoice_id"] is not None
    assert resolved_1["journal_entry_id"] is not None
    assert resolved_1["document_number"] == bad_inv_1["document_number"]

    # 5. Resolve qid2 via REJECTED
    resolved_2 = processor.resolve_quarantined_invoice(
        quarantine_id=qid2,
        resolution_action="REJECTED",
        reviewed_by="auditor.lead@comex.br",
    )
    assert resolved_2["quarantine_id"] == qid2
    assert resolved_2["review_status"] == "REJECTED"
    assert resolved_2["reviewed_by"] == "auditor.lead@comex.br"
    assert resolved_2["invoice_id"] is None
    assert resolved_2["journal_entry_id"] is None

    # 6. Verify neither is pending anymore
    remaining_pending = processor.get_pending_quarantine_items(status_filter="PENDING_REVIEW")
    remaining_ids = [it["quarantine_id"] for it in remaining_pending]
    assert qid1 not in remaining_ids
    assert qid2 not in remaining_ids

    # 7. Immutability verification: attempting to re-resolve finalized items raises ValueError
    with pytest.raises(ValueError) as exc:
        processor.resolve_quarantined_invoice(
            quarantine_id=qid1,
            resolution_action="APPROVED_OVERRIDE",
            reviewed_by="auditor.lead@comex.br",
            corrected_payload=valid_corrected_payload_1,
        )
    assert "already finalized" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        processor.resolve_quarantined_invoice(
            quarantine_id=qid2,
            resolution_action="REJECTED",
            reviewed_by="auditor.lead@comex.br",
        )
    assert "already finalized" in str(exc.value)

    # 8. Non-existent quarantine_id raises KeyError
    with pytest.raises(KeyError) as exc:
        processor.resolve_quarantined_invoice(
            quarantine_id=uuid4(),
            resolution_action="REJECTED",
            reviewed_by="auditor.lead@comex.br",
        )
    assert "not found" in str(exc.value)


@pytest.mark.unit
def test_api_v2_quarantine_pending_and_resolve_endpoints():
    # 1. Deflect an invalid invoice to quarantine via API
    bad_api_payload = {
        "issuer_tax_id": VALID_CNPJ_FORMATTED,
        "document_number": f"API-QUAR-{uuid4().hex[:6]}",
        "issue_date": "2026-11-09",
        "currency_code": "USD",
        "incoterm": "CIF",
        "subtotal_amount": "40000.0000",
        "tax_amount": "0.0000",
        "freight_amount": "0.0000",  # CIF requires explicit freight!
        "insurance_amount": "0.0000",  # CIF requires explicit insurance!
        "total_amount": "40000.0000",
    }
    ingest_resp = client.post("/api/v2/invoices/ingest", json=bad_api_payload)
    assert ingest_resp.status_code == 422
    quarantine_id = ingest_resp.json()["detail"]["quarantine_id"]
    assert quarantine_id is not None

    # 2. Test GET /api/v2/quarantine/pending
    pending_resp = client.get("/api/v2/quarantine/pending")
    assert pending_resp.status_code == 200
    pending_data = pending_resp.json()
    assert pending_data["count"] >= 1
    found_ids = [it["quarantine_id"] for it in pending_data["items"]]
    assert quarantine_id in found_ids

    # 3. Test POST /api/v2/quarantine/{quarantine_id}/resolve with bad math in corrected_payload (Expect 422)
    bad_math_payload = {
        "issuer_tax_id": VALID_CNPJ_FORMATTED,
        "document_number": bad_api_payload["document_number"],
        "issue_date": "2026-11-09",
        "currency_code": "USD",
        "incoterm": "CIF",
        "subtotal_amount": "40000.0000",
        "tax_amount": "0.0000",
        "freight_amount": "2000.0000",
        "insurance_amount": "500.0000",
        "total_amount": "42000.0000",  # Math error: 40000 + 2000 + 500 = 42500 != 42000!
    }
    resolve_bad_math_req = {
        "resolution_action": "APPROVED_OVERRIDE",
        "reviewed_by": "hugo.auditor@comex.br",
        "corrected_payload": bad_math_payload,
    }
    bad_math_resp = client.post(f"/api/v2/quarantine/{quarantine_id}/resolve", json=resolve_bad_math_req)
    assert bad_math_resp.status_code == 422
    assert "validation" in str(bad_math_resp.json()).lower() or "closure" in str(bad_math_resp.json()).lower()

    # Verify item is still in PENDING_REVIEW queue after failed resolution attempt
    pending_check = client.get("/api/v2/quarantine/pending")
    assert quarantine_id in [it["quarantine_id"] for it in pending_check.json()["items"]]

    # 4. Test POST /api/v2/quarantine/{quarantine_id}/resolve with valid corrected_payload (Expect 200 OK)
    valid_corrected_payload = {
        "issuer_tax_id": VALID_CNPJ_FORMATTED,
        "document_number": bad_api_payload["document_number"],
        "issue_date": "2026-11-09",
        "currency_code": "USD",
        "incoterm": "CIF",
        "subtotal_amount": "40000.0000",
        "tax_amount": "0.0000",
        "freight_amount": "2000.0000",
        "insurance_amount": "500.0000",
        "total_amount": "42500.0000",  # Exact math!
    }
    resolve_ok_req = {
        "resolution_action": "APPROVED_OVERRIDE",
        "reviewed_by": "hugo.auditor@comex.br",
        "corrected_payload": valid_corrected_payload,
    }
    ok_resp = client.post(f"/api/v2/quarantine/{quarantine_id}/resolve", json=resolve_ok_req)
    assert ok_resp.status_code == 200
    res_payload = ok_resp.json()
    assert res_payload["quarantine_id"] == quarantine_id
    assert res_payload["review_status"] == "APPROVED_OVERRIDE"
    assert res_payload["reviewed_by"] == "hugo.auditor@comex.br"
    assert res_payload["invoice_id"] is not None
    assert res_payload["journal_entry_id"] is not None
    assert res_payload["document_number"] == bad_api_payload["document_number"]

    # 5. Test duplicate resolution on finalized item (Expect 409 Conflict)
    duplicate_resp = client.post(f"/api/v2/quarantine/{quarantine_id}/resolve", json=resolve_ok_req)
    assert duplicate_resp.status_code == 409
    assert "already finalized" in duplicate_resp.json()["detail"]["message"]

    # 6. Test resolution on non-existent quarantine_id (Expect 404 Not Found)
    non_existent_qid = str(uuid4())
    not_found_resp = client.post(f"/api/v2/quarantine/{non_existent_qid}/resolve", json=resolve_ok_req)
    assert not_found_resp.status_code == 404
    assert "not found" in not_found_resp.json()["detail"]["message"]




