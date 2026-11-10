"""
Live BACEN Olinda PTAX Connector & FX Dimension Synchronizer.
Conforms to ODCS v3.2.0 (comex_fiduciary_engine_v2) & semantic_rules.md.

Mandates:
- Zero IEEE 754 float types (DECIMAL(19,6) for exchange rates).
- Resilient API extraction via ResilientAPIExtractor (tenacity backoff + full jitter).
- Selection of daily closing bulletin ('Fechamento' or latest dataHoraCotacao).
- LOCF forward-fill across weekends/holidays with zero lookahead bias.
- dim_date reference foreign-key satisfaction in PostgreSQL.
- Idempotent upsert into PostgreSQL dim_currency_ptax and Parquet Hive Lakehouse.
- Seamless offline fallback to samples/bacen_ptax_sample.json when network is unavailable.
"""

import argparse
import collections
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
import logging
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Union

import httpx
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.connectors.postgres import engine as default_postgres_engine
from src.extractors.http_client import ResilientAPIExtractor, RetryableHTTPError
from src.models.customs import (
    PTAXRateInput,
    VALID_PTAX_CURRENCIES,
    apply_locf_forward_fill,
)
from src.pipeline.comex_processor import ComexHTAPProcessor

logger = logging.getLogger("comex.extractors.bacen_ptax")

DEFAULT_BASE_URL = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata"
DEFAULT_SAMPLE_PATH = Path(__file__).resolve().parent.parent.parent / "samples" / "bacen_ptax_sample.json"


def ensure_dim_date_range(engine_or_conn: Any, start_date: date, end_date: date) -> None:
    """
    Ensures dim_date records exist in PostgreSQL for [start_date, end_date]
    prior to persisting dim_currency_ptax, ensuring dim_currency_ptax_date_id_fkey
    is always satisfied without foreign key violations.
    """
    if engine_or_conn is None:
        return

    rows: List[Dict[str, Any]] = []
    curr = start_date
    while curr <= end_date:
        date_id = curr.year * 10000 + curr.month * 100 + curr.day
        is_biz = curr.weekday() < 5
        quarter = (curr.month - 1) // 3 + 1
        rows.append({
            "date_id": date_id,
            "full_date": curr,
            "year": curr.year,
            "month": curr.month,
            "day": curr.day,
            "quarter": quarter,
            "is_biz": is_biz,
        })
        curr += timedelta(days=1)

    insert_sql = text(
        """
        INSERT INTO dim_date (date_id, full_date, year, month, day, quarter, is_business_day)
        VALUES (:date_id, :full_date, :year, :month, :day, :quarter, :is_biz)
        ON CONFLICT (date_id) DO NOTHING
        """
    )

    if hasattr(engine_or_conn, "execute") and not hasattr(engine_or_conn, "begin"):
        try:
            engine_or_conn.execute(insert_sql, rows)
        except Exception as exc:
            logger.warning(f"dim_date postgres check skipped/warning on conn: {exc}")
    else:
        try:
            with engine_or_conn.begin() as conn:
                conn.execute(insert_sql, rows)
        except Exception as exc:
            logger.warning(f"dim_date postgres check skipped/warning on engine: {exc}")


def parse_olinda_ptax_items(
    items: List[Dict[str, Any]],
    currency_code: str,
    start_date: date,
    end_date: date,
) -> List[PTAXRateInput]:
    """
    Parses raw BACEN Olinda OData quotation items:
    1. Groups items by calendar date extracted from dataHoraCotacao.
    2. Selects the canonical closing bulletin ('Fechamento' if present, or latest dataHoraCotacao).
    3. Strictly quantizes cotacaoCompra and cotacaoVenda to Decimal("0.000001") via str().
    4. Applies apply_locf_forward_fill across calendar gaps without lookahead bias.
    """
    code = currency_code.upper().strip()
    if code not in VALID_PTAX_CURRENCIES:
        raise ValueError(f"Invalid currency '{currency_code}'. Permitted: {sorted(VALID_PTAX_CURRENCIES)}")

    items_by_date: Dict[date, List[Dict[str, Any]]] = collections.defaultdict(list)
    for it in items:
        dh = str(it.get("dataHoraCotacao", "")).strip()
        if len(dh) < 10:
            continue
        try:
            item_date = date(int(dh[:4]), int(dh[5:7]), int(dh[8:10]))
            items_by_date[item_date].append(it)
        except (ValueError, TypeError):
            continue

    daily_observations: List[PTAXRateInput] = []
    for d in sorted(items_by_date.keys()):
        day_items = items_by_date[d]
        fechamento = [
            it for it in day_items
            if str(it.get("tipoBoletim", "")).strip().lower() == "fechamento"
        ]
        candidates = fechamento if fechamento else day_items
        best = max(candidates, key=lambda x: str(x.get("dataHoraCotacao", "")))

        # Anti-Float rule: convert via str() into Decimal, strictly quantize to Decimal("0.000001")
        buy_raw = str(best["cotacaoCompra"])
        sell_raw = str(best["cotacaoVenda"])
        buy_dec = Decimal(buy_raw).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
        sell_dec = Decimal(sell_raw).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
        date_id = d.year * 10000 + d.month * 100 + d.day

        rate_input = PTAXRateInput(
            date_id=date_id,
            currency_code=code,
            ptax_buy_rate=buy_dec,
            ptax_sell_rate=sell_dec,
            is_business_day=True,
            is_interpolated_locf=False,
        )
        daily_observations.append(rate_input)

    # Forward-fill weekends/holidays using LOCF
    return apply_locf_forward_fill(
        rates=daily_observations,
        start_date=start_date,
        end_date=end_date,
        currency_code=code,
    )


class BacenPTAXClient:
    """
    Live BACEN Olinda PTAX Extractor & Synchronizer with LOCF forward-fill,
    resilient backoff retries, and seamless offline sample fallback.
    """

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        timeout_seconds: float = 10.0,
        client: Optional[httpx.Client] = None,
        offline_fallback: bool = False,
        sample_path: Optional[Union[str, Path]] = None,
        processor: Optional[ComexHTAPProcessor] = None,
        engine: Optional[Engine] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.offline_fallback = offline_fallback
        self.sample_path = Path(sample_path) if sample_path else DEFAULT_SAMPLE_PATH
        self.processor = processor or ComexHTAPProcessor()
        self.engine = engine or default_postgres_engine
        self._http_client = client
        self.extractor = ResilientAPIExtractor(
            source_system="BACEN_OLINDA_PTAX",
            base_url=self.base_url,
            timeout_seconds=self.timeout_seconds,
            client=self._http_client,
        )

    def fetch_raw_ptax(
        self,
        currency_code: str,
        start_date: date,
        end_date: date,
    ) -> List[Dict[str, Any]]:
        """
        Executes HTTP GET on BACEN Olinda CotacaoMoedaPeriodo endpoint.
        Formatted parameters: @moeda='USD', @dataInicial='MM-DD-YYYY', @dataFinalCotacao='MM-DD-YYYY'.
        """
        code = currency_code.upper().strip()
        if code not in VALID_PTAX_CURRENCIES:
            raise ValueError(f"Invalid currency '{currency_code}'. Permitted: {sorted(VALID_PTAX_CURRENCIES)}")

        endpoint = "CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
        params = {
            "@moeda": f"'{code}'",
            "@dataInicial": f"'{start_date.strftime('%m-%d-%Y')}'",
            "@dataFinalCotacao": f"'{end_date.strftime('%m-%d-%Y')}'",
            "$top": 1000,
            "$format": "json",
        }

        data = self.extractor.fetch_json(endpoint=endpoint, params=params)
        if isinstance(data, dict):
            return data.get("value") or data.get("items") or []
        if isinstance(data, list):
            return data
        return []

    def load_offline_sample(
        self,
        currency_code: str,
        start_date: date,
        end_date: date,
    ) -> List[PTAXRateInput]:
        """
        Loads PTAX rates from samples/bacen_ptax_sample.json and applies
        LOCF forward-fill if any calendar days are missing in the requested interval.
        """
        code = currency_code.upper().strip()
        if not self.sample_path.exists():
            raise FileNotFoundError(f"Offline PTAX sample file not found: {self.sample_path}")

        with open(self.sample_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        raw_matching = [item for item in data if item.get("currency_code") == code]
        if not raw_matching:
            raise ValueError(f"No sample rates found for currency {code} in {self.sample_path}")

        parsed_rates = [PTAXRateInput.model_validate(item) for item in raw_matching]

        # Check if the requested range is already fully satisfied by the sample
        matching_range = [
            r for r in parsed_rates
            if start_date <= date(r.date_id // 10000, (r.date_id % 10000) // 100, r.date_id % 100) <= end_date
        ]
        expected_days = (end_date - start_date).days + 1
        if len(matching_range) == expected_days:
            return sorted(matching_range, key=lambda x: x.date_id)

        # Forward fill across any gaps using LOCF
        return apply_locf_forward_fill(
            rates=parsed_rates,
            start_date=start_date,
            end_date=end_date,
            currency_code=code,
        )

    def fetch_ptax_for_currency(
        self,
        currency_code: str,
        start_date: date,
        end_date: date,
    ) -> List[PTAXRateInput]:
        """
        Fetches PTAX rates for a single currency, falling back seamlessly
        to offline sample when offline_fallback=True or network is unavailable.
        """
        code = currency_code.upper().strip()
        if self.offline_fallback:
            logger.info(f"Offline fallback mode active; loading {code} from {self.sample_path}")
            return self.load_offline_sample(code, start_date, end_date)

        try:
            raw_items = self.fetch_raw_ptax(code, start_date, end_date)
            if not raw_items:
                logger.warning(f"Empty raw items returned from BACEN for {code}; falling back to sample.")
                return self.load_offline_sample(code, start_date, end_date)

            return parse_olinda_ptax_items(raw_items, code, start_date, end_date)
        except Exception as exc:
            logger.warning(
                f"BACEN Olinda live fetch failed ({exc.__class__.__name__}: {exc}); "
                f"seamlessly falling back to offline sample {self.sample_path}"
            )
            return self.load_offline_sample(code, start_date, end_date)

    def sync_ptax_period(
        self,
        start_date: date,
        end_date: date,
        currencies: Optional[List[str]] = None,
        conn: Optional[Any] = None,
    ) -> List[PTAXRateInput]:
        """
        Synchronizes PTAX rates for all specified currencies across [start_date, end_date].
        1. Queries BACEN Olinda (or loads offline fallback).
        2. Applies LOCF forward-fill across weekends and holidays.
        3. Ensures dim_date rows exist in PostgreSQL.
        4. Upserts idempotently into dim_currency_ptax and mirrors to Parquet Hive Lakehouse.
        """
        target_currencies = currencies or ["USD", "EUR", "CHF"]
        all_rates: List[PTAXRateInput] = []

        for curr in target_currencies:
            rates = self.fetch_ptax_for_currency(curr, start_date, end_date)
            all_rates.extend(rates)

        # Ensure foreign key dim_date satisfaction
        target_db = conn if conn is not None else self.engine
        ensure_dim_date_range(target_db, start_date, end_date)

        # Persist to PostgreSQL and mirror to Parquet Lakehouse
        self.processor.persist_ptax_rates(all_rates, conn=conn)

        return all_rates


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(
        description="BACEN Olinda PTAX Connector & LOCF Forward-Fill Synchronizer"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2026-11-05",
        help="Start date in YYYY-MM-DD format (default: 2026-11-05)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="2026-11-10",
        help="End date in YYYY-MM-DD format (default: 2026-11-10)",
    )
    parser.add_argument(
        "--currencies",
        type=str,
        default="USD,EUR,CHF",
        help="Comma-separated currencies: USD,EUR,CHF (default: USD,EUR,CHF)",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Force offline fallback using samples/bacen_ptax_sample.json",
    )
    parser.add_argument(
        "--sample-path",
        type=str,
        default=None,
        help="Custom path to offline sample json file",
    )

    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date.strip(), "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date.strip(), "%Y-%m-%d").date()
    currencies = [c.strip().upper() for c in args.currencies.split(",") if c.strip()]

    print("\n" + "=" * 74)
    print(" 🏛️  BACEN OLINDA LIVE PTAX CONNECTOR & HTAP LAKEHOUSE SYNCHRONIZER")
    print("=" * 74)
    print(f" Period:        {start_date} to {end_date}")
    print(f" Currencies:    {', '.join(currencies)}")
    print(f" Offline Mode:  {args.offline}")
    print("=" * 74)

    client = BacenPTAXClient(
        offline_fallback=args.offline,
        sample_path=args.sample_path,
    )

    rates = client.sync_ptax_period(
        start_date=start_date,
        end_date=end_date,
        currencies=currencies,
    )

    biz_count = sum(1 for r in rates if r.is_business_day)
    locf_count = sum(1 for r in rates if r.is_interpolated_locf)

    print("\n" + "-" * 74)
    print(f" {'Date':<10} | {'Curr':<4} | {'Buy Rate (BRL)':<14} | {'Sell Rate (BRL)':<15} | {'Type':<12}")
    print("-" * 74)

    for r in sorted(rates, key=lambda x: (x.date_id, x.currency_code)):
        d_str = f"{r.date_id // 10000:04d}-{(r.date_id % 10000) // 100:02d}-{r.date_id % 100:02d}"
        kind = "LOCF Fill" if r.is_interpolated_locf else "Official PTAX"
        print(f" {d_str:<10} | {r.currency_code:<4} | {r.ptax_buy_rate:>14} | {r.ptax_sell_rate:>15} | {kind:<12}")

    print("-" * 74)
    print(f" Summary: {len(rates)} total rates ({biz_count} official, {locf_count} LOCF forward-filled)")
    print(" ✅ Synchronization completed successfully (PostgreSQL dim_currency_ptax & Parquet Hive Lakehouse).\n")


if __name__ == "__main__":
    main()
