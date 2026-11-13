"""
Live MDIC Comex Stat Real Bulk Ingestion Extractor & Dimensional Synchronizer.
Conforms to ODCS v3.2.0 (contracts/comex_fiduciary_engine_v2.odcs.yaml) & semantic_rules.md.

Key Architectural Responsibilities:
1. Resilient HTTP streaming download from official MDIC Comex Stat URLs with tenacity retry
   (strict TLS verification; retries only transient 429, 5xx, and transport/timeout errors).
2. Local disk caching in data/raw_mdic/ with atomic temp-file cleanup on failure.
3. Dimension Synchronization (NCM.csv and PAIS.csv) with UTF-8/CP1252/Latin-1 decoding
   and upserts into dim_merchandise_ncm and dim_partner_country.
4. Early-Arriving Dimensions defensive auto-upsert (ON CONFLICT DO NOTHING) for dim_date,
   dim_merchandise_ncm, and dim_partner_country prior to fact table insertion.
5. Canonical 8-key pre-aggregation across measurement units (CO_UNID):
   (ano, mes, tipo_op, co_ncm, co_pais, sg_uf, co_via, co_urf)
   summing statistical_quantity, net_weight_kg, fob_amount_usd, freight_amount_usd, and insurance_amount_usd
   using strict Decimal("0.0001") (ROUND_HALF_UP).
6. Export (EXP_YYYY.csv) freight and insurance defaulting to Decimal("0.0000") and cif_amount_usd = fob_amount_usd.
7. Internalized inventory landed_cost_brl valuation from dim_currency_ptax USD selling rates
   without temporal lookahead (date_id <= target_date_id).
8. Hybrid Storage Persistence: 100% of operations to Apache Parquet Hive Lakehouse
   (PARQUET_BASE_DIR/fact_comex_operations/year=YYYY/month=MM/) and PostgreSQL fact_comex_operations
   (sync all by default, or explicit --limit / limit_pg opt-in).
9. Explicit offline mode (--offline / offline=True) using samples/ when requested.
"""

import argparse
import codecs
import csv
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import logging
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import uuid4

import httpx
from sqlalchemy import text
from sqlalchemy.exc import InterfaceError, OperationalError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from src.connectors.lakehouse import (
    get_duckdb_connection,
    get_parquet_base_dir,
    write_records_to_parquet_hive,
)
from src.connectors.postgres import engine as default_postgres_engine
from src.extractors.http_client import RetryableHTTPError
from src.models.customs import (
    ComexOperationInput,
    compute_operation_hash_id,  # noqa: F401 - re-exported for backwards compatibility
)
from src.pipeline.comex_processor import FACT_COMEX_PARQUET_SCHEMA

logger = logging.getLogger("comex.extractors.mdic_bulk")

MDIC_BASE_URL = "https://balanca.economia.gov.br/balanca/bd"
URL_IMP_TEMPLATE = f"{MDIC_BASE_URL}/comexstat-bd/ncm/IMP_{{year}}.csv"
URL_EXP_TEMPLATE = f"{MDIC_BASE_URL}/comexstat-bd/ncm/EXP_{{year}}.csv"
URL_NCM = f"{MDIC_BASE_URL}/tabelas/NCM.csv"
URL_PAIS = f"{MDIC_BASE_URL}/tabelas/PAIS.csv"

DEFAULT_USD_PTAX_SELL_RATE = Decimal("5.846500")

_UNSET = object()


def _detect_csv_encoding(path: Path) -> str:
    """Detect UTF-8 (with/without BOM), CP1252, or Latin-1 CSV encoding in O(1) memory."""
    with open(path, "rb") as fb:
        if fb.read(3) == b"\xef\xbb\xbf":
            return "utf-8-sig"

    for encoding in ("utf-8", "cp1252"):
        decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
        try:
            with open(path, "rb") as fb:
                while True:
                    chunk = fb.read(1024 * 1024)
                    if not chunk:
                        decoder.decode(b"", final=True)
                        return encoding
                    decoder.decode(chunk, final=False)
        except UnicodeDecodeError:
            continue

    return "latin-1"


class MDICBulkLoader:
    """
    Ingestion engine for MDIC Comex Stat bulk microdata and auxiliary dimension tables.
    """

    def __init__(
        self,
        cache_dir: Optional[Union[str, Path]] = None,
        samples_dir: Optional[Union[str, Path]] = None,
        engine: Any = _UNSET,
        timeout_seconds: float = 60.0,
        offline: bool = False,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        self.project_root = Path(__file__).resolve().parent.parent.parent
        self.cache_dir = Path(cache_dir) if cache_dir else (self.project_root / "data" / "raw_mdic")
        self.samples_dir = Path(samples_dir) if samples_dir else (self.project_root / "samples")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._explicit_engine = engine is not _UNSET
        self.engine = default_postgres_engine if engine is _UNSET else engine
        self.timeout_seconds = timeout_seconds
        self.offline = offline
        self._http_client = http_client
        self._ptax_cache: Dict[int, Decimal] = {}

    def _handle_pg_write_exception(self, context: str, exc: Exception) -> None:
        """
        Allow skipping PostgreSQL writes ONLY in offline mode using the default engine
        when PostgreSQL is unreachable (e.g. isolated unit tests). Raise on all other errors.
        """
        if self.offline and not self._explicit_engine and isinstance(exc, (OperationalError, InterfaceError)):
            logger.warning("PostgreSQL unreachable during offline %s; skipping DB write: %s", context, exc)
            return
        logger.error("PostgreSQL %s failed: %s", context, exc)
        raise exc

    # -------------------------------------------------------------------------
    # 1. Resilient Download & Disk Caching
    # -------------------------------------------------------------------------

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_random_exponential(multiplier=1.0, max=10.0),
        retry=retry_if_exception_type((httpx.TransportError, RetryableHTTPError)),
    )
    def _download_stream(self, url: str, target_file: Path) -> None:
        """Streams remote file to a temporary file with strict TLS, then atomically renames."""
        tmp_file = target_file.with_name(f".{target_file.name}.tmp.{uuid4().hex}")
        headers = {"User-Agent": "ComexDataEngineV2/BulkLoader"}

        def _stream_with_client(client: httpx.Client) -> None:
            with client.stream("GET", url, headers=headers) as response:
                if response.status_code == 429 or response.status_code >= 500:
                    raise RetryableHTTPError(f"Transient HTTP {response.status_code} from {url}")
                response.raise_for_status()
                with open(tmp_file, "wb") as f:
                    for chunk in response.iter_bytes(chunk_size=65536):
                        f.write(chunk)

        try:
            if self._http_client is not None:
                _stream_with_client(self._http_client)
            else:
                with httpx.Client(timeout=self.timeout_seconds, verify=True, follow_redirects=True) as client:
                    _stream_with_client(client)
            tmp_file.replace(target_file)
        finally:
            tmp_file.unlink(missing_ok=True)

    def download_file(self, url: str, dest_path: Path, force_download: bool = False) -> Path:
        """
        Downloads a remote URL to dest_path with local disk caching, tenacity backoff
        (429/5xx/network only), and strict TLS verification.
        """
        if dest_path.exists() and not force_download:
            logger.info("Using cached file at %s", dest_path)
            return dest_path

        if self.offline:
            raise FileNotFoundError(f"Offline mode active and cached file {dest_path} not found.")

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading %s -> %s...", url, dest_path)

        try:
            self._download_stream(url, dest_path)
            logger.info("Downloaded successfully: %s", dest_path.name)
            return dest_path
        except Exception as exc:
            logger.error("Download failed for %s: %s", url, exc)
            raise

    def get_comex_file(self, year: int, flow: str, force_download: bool = False) -> Optional[Path]:
        """Locates or downloads IMP_{year}.csv or EXP_{year}.csv, using samples only in offline mode."""
        flow_upper = flow.upper()
        filename = f"{'IMP' if flow_upper == 'IMPORT' else 'EXP'}_{year}.csv"
        cached_path = self.cache_dir / filename

        if cached_path.exists() and not force_download:
            return cached_path

        if self.offline:
            return self.get_offline_fallback(year=year, flow=flow)

        url = URL_IMP_TEMPLATE.format(year=year) if flow_upper == "IMPORT" else URL_EXP_TEMPLATE.format(year=year)
        return self.download_file(url, cached_path, force_download=force_download)

    def get_offline_fallback(self, year: int, flow: str) -> Optional[Path]:
        """Returns the appropriate offline sample file."""
        flow_upper = flow.upper()
        imp_sample = self.samples_dir / "mdic_imp_sample.csv"
        exp_sample = self.samples_dir / "mdic_exp_sample.csv"
        canonical_sample = self.samples_dir / "comex_mdic_sample.csv"

        if flow_upper == "IMPORT" and imp_sample.exists() and year == 2024:
            return imp_sample
        if flow_upper == "EXPORT" and exp_sample.exists() and year == 2024:
            return exp_sample
        if canonical_sample.exists():
            return canonical_sample
        if flow_upper == "IMPORT" and imp_sample.exists():
            return imp_sample
        if flow_upper == "EXPORT" and exp_sample.exists():
            return exp_sample

        return None

    # -------------------------------------------------------------------------
    # 2. Dimension Synchronization (NCM & PAIS)
    # -------------------------------------------------------------------------

    def sync_auxiliary_dimensions(self, force_download: bool = False) -> Dict[str, int]:
        """
        Synchronizes NCM.csv and PAIS.csv from MDIC to PostgreSQL
        dim_merchandise_ncm and dim_partner_country.
        """
        ncm_path = self.cache_dir / "NCM.csv"
        pais_path = self.cache_dir / "PAIS.csv"

        if self.offline:
            if not ncm_path.exists():
                ncm_path = self.samples_dir / "mdic_ncm_sample.csv"
            if not pais_path.exists():
                pais_path = self.samples_dir / "mdic_pais_sample.csv"
        else:
            if not ncm_path.exists() or force_download:
                self.download_file(URL_NCM, ncm_path, force_download=force_download)
            if not pais_path.exists() or force_download:
                self.download_file(URL_PAIS, pais_path, force_download=force_download)

        ncm_count = self._sync_ncm_dimension(ncm_path)
        pais_count = self._sync_pais_dimension(pais_path)

        return {"ncm_count": ncm_count, "country_count": pais_count}

    def _sync_ncm_dimension(self, path: Path) -> int:
        if not path.exists():
            raise FileNotFoundError(f"NCM reference file not found at {path}")

        encoding = _detect_csv_encoding(path)
        rows: List[Dict[str, Any]] = []
        with open(path, mode="r", encoding=encoding) as f:
            first_line = f.readline()
            delimiter = ";" if ";" in first_line else ","
            f.seek(0)
            reader = csv.reader(f, delimiter=delimiter)
            header = [col.strip().strip('"').upper() for col in next(reader, [])]
            header_map = {col: idx for idx, col in enumerate(header)}

            ncm_idx = header_map.get("CO_NCM", header_map.get("NCM_CODE"))
            if ncm_idx is None:
                raise ValueError(f"CO_NCM / NCM_CODE column missing from {path}")

            sh_idx = header_map.get("CO_SH", header_map.get("HS_CODE"))
            desc_idx = header_map.get("NO_NCM_POR", header_map.get("NO_NCM", header_map.get("DESCRIPTION")))
            unid_idx = header_map.get("NO_UNID", header_map.get("CO_UNID", header_map.get("UNIT_MEASURE")))

            for line in reader:
                if not line or len(line) <= ncm_idx:
                    continue
                ncm = str(line[ncm_idx]).strip().strip('"').zfill(8)
                if not ncm.isdigit() or len(ncm) != 8:
                    continue
                hs = str(line[sh_idx]).strip().strip('"') if sh_idx is not None and len(line) > sh_idx else ""
                if not hs.isdigit() or len(hs) != 6:
                    hs = ncm[:6]
                desc = str(line[desc_idx]).strip().strip('"') if desc_idx is not None and len(line) > desc_idx else ""
                if not desc:
                    desc = f"NCM {ncm}"
                unid = str(line[unid_idx]).strip().strip('"') if unid_idx is not None and len(line) > unid_idx else ""
                if not unid:
                    unid = "KG"

                rows.append({
                    "ncm": ncm,
                    "hs": hs,
                    "desc": desc,
                    "unid": unid[:30],
                })

        if not rows or self.engine is None:
            return len(rows)

        sql = text(
            """
            INSERT INTO dim_merchandise_ncm (ncm_code, hs_code, description, unit_measure, created_at)
            VALUES (:ncm, :hs, :desc, :unid, CURRENT_TIMESTAMP)
            ON CONFLICT (ncm_code)
            DO UPDATE SET
                hs_code = EXCLUDED.hs_code,
                description = EXCLUDED.description,
                unit_measure = EXCLUDED.unit_measure
            """
        )
        try:
            with self.engine.begin() as conn:
                conn.execute(sql, rows)
            logger.info("Synchronized %d NCM dimensions into PostgreSQL.", len(rows))
        except Exception as exc:
            self._handle_pg_write_exception("dim_merchandise_ncm sync", exc)

        return len(rows)

    def _sync_pais_dimension(self, path: Path) -> int:
        if not path.exists():
            raise FileNotFoundError(f"PAIS reference file not found at {path}")

        encoding = _detect_csv_encoding(path)
        rows: List[Dict[str, Any]] = []
        with open(path, mode="r", encoding=encoding) as f:
            first_line = f.readline()
            delimiter = ";" if ";" in first_line else ","
            f.seek(0)
            reader = csv.reader(f, delimiter=delimiter)
            header = [col.strip().strip('"').upper() for col in next(reader, [])]
            header_map = {col: idx for idx, col in enumerate(header)}

            pais_idx = header_map.get("CO_PAIS", header_map.get("COUNTRY_CODE"))
            if pais_idx is None:
                raise ValueError(f"CO_PAIS / COUNTRY_CODE column missing from {path}")

            iso3_idx = header_map.get("CO_PAIS_ISOA3", header_map.get("COUNTRY_ISO3"))
            name_idx = header_map.get("NO_PAIS", header_map.get("COUNTRY_NAME"))
            block_idx = header_map.get("NO_BLOCO", header_map.get("ECONOMIC_BLOCK"))

            for line in reader:
                if not line or len(line) <= pais_idx:
                    continue
                code_raw = str(line[pais_idx]).strip().strip('"')
                if not code_raw.isdigit():
                    continue
                code = int(code_raw)
                if code < 1:
                    continue
                iso3 = str(line[iso3_idx]).strip().strip('"').upper() if iso3_idx is not None and len(line) > iso3_idx else "UNK"
                if not re.match(r"^[A-Z]{3}$", iso3):
                    iso3 = "UNK"
                name = str(line[name_idx]).strip().strip('"') if name_idx is not None and len(line) > name_idx else ""
                if not name:
                    name = f"Country {code}"
                block = str(line[block_idx]).strip().strip('"') if block_idx is not None and len(line) > block_idx else None
                if block == "":
                    block = None

                rows.append({
                    "code": code,
                    "iso3": iso3,
                    "name": name[:100],
                    "block": block[:50] if block else None,
                })

        if not rows or self.engine is None:
            return len(rows)

        sql = text(
            """
            INSERT INTO dim_partner_country (country_code, country_iso3, country_name, economic_block, created_at)
            VALUES (:code, :iso3, :name, :block, CURRENT_TIMESTAMP)
            ON CONFLICT (country_code)
            DO UPDATE SET
                country_iso3 = EXCLUDED.country_iso3,
                country_name = EXCLUDED.country_name,
                economic_block = EXCLUDED.economic_block
            """
        )
        try:
            with self.engine.begin() as conn:
                conn.execute(sql, rows)
            logger.info("Synchronized %d PAIS dimensions into PostgreSQL.", len(rows))
        except Exception as exc:
            self._handle_pg_write_exception("dim_partner_country sync", exc)

        return len(rows)

    # -------------------------------------------------------------------------
    # 3. Defensive Early-Arriving Dimensions Auto-Upsert
    # -------------------------------------------------------------------------

    def ensure_dimensions_for_operations(
        self,
        operations: List[ComexOperationInput],
        conn: Optional[Any] = None,
    ) -> None:
        """
        Defensively ensures parent dimension rows exist in dim_date, dim_merchandise_ncm,
        and dim_partner_country using ON CONFLICT DO NOTHING before fact insertion.
        Prevents foreign key violations during asynchronous bulk ingestion.
        """
        if not operations or (self.engine is None and conn is None):
            return

        date_ids: Set[int] = set()
        ncm_codes: Set[str] = set()
        country_codes: Set[int] = set()

        for op in operations:
            date_ids.add(op.date_id)
            ncm_codes.add(op.ncm_code)
            country_codes.add(op.country_code)

        date_rows = []
        for did in date_ids:
            y = did // 10000
            m = (did % 10000) // 100
            d = did % 100
            dt = date(y, m, d)
            qtr = (m - 1) // 3 + 1
            is_biz = dt.weekday() < 5
            date_rows.append({
                "date_id": did,
                "full_date": dt,
                "year": y,
                "month": m,
                "day": d,
                "quarter": qtr,
                "is_biz": is_biz,
            })

        ncm_rows = [
            {
                "ncm": ncm,
                "hs": ncm[:6],
                "desc": f"Mercadoria NCM {ncm}",
                "unid": "KG",
            }
            for ncm in ncm_codes
        ]

        country_rows = [
            {
                "code": code,
                "iso3": "UNK",
                "name": f"Country {code}",
            }
            for code in country_codes
        ]

        def _execute_upserts(active_conn: Any) -> None:
            active_conn.execute(
                text(
                    """
                    INSERT INTO dim_date (date_id, full_date, year, month, day, quarter, is_business_day)
                    VALUES (:date_id, :full_date, :year, :month, :day, :quarter, :is_biz)
                    ON CONFLICT (date_id) DO NOTHING
                    """
                ),
                date_rows,
            )
            active_conn.execute(
                text(
                    """
                    INSERT INTO dim_merchandise_ncm (ncm_code, hs_code, description, unit_measure)
                    VALUES (:ncm, :hs, :desc, :unid)
                    ON CONFLICT (ncm_code) DO NOTHING
                    """
                ),
                ncm_rows,
            )
            active_conn.execute(
                text(
                    """
                    INSERT INTO dim_partner_country (country_code, country_iso3, country_name)
                    VALUES (:code, :iso3, :name)
                    ON CONFLICT (country_code) DO NOTHING
                    """
                ),
                country_rows,
            )

        try:
            if conn is not None:
                _execute_upserts(conn)
            else:
                with self.engine.begin() as local_conn:
                    _execute_upserts(local_conn)
        except Exception as exc:
            self._handle_pg_write_exception("early-arriving dimensions upsert", exc)

    # -------------------------------------------------------------------------
    # 4. PTAX Rate Resolution (Zero Temporal Lookahead)
    # -------------------------------------------------------------------------

    def resolve_ptax_sell_rate(self, date_id: int) -> Decimal:
        """
        Resolves official USD PTAX selling rate for date_id (on or before target date_id;
        never looks ahead into future dates) from PostgreSQL dim_currency_ptax or
        DuckDB/Parquet Lakehouse, falling back to DEFAULT_USD_PTAX_SELL_RATE when absent.
        """
        if date_id in self._ptax_cache:
            return self._ptax_cache[date_id]

        # 1. Query PostgreSQL dim_currency_ptax (date_id <= target_date_id, zero lookahead)
        if self.engine is not None:
            try:
                with self.engine.connect() as conn:
                    row = conn.execute(
                        text(
                            "SELECT ptax_sell_rate FROM dim_currency_ptax "
                            "WHERE currency_code = 'USD' AND date_id <= :did "
                            "ORDER BY date_id DESC LIMIT 1"
                        ),
                        {"did": date_id},
                    ).fetchone()
                    if row and row[0] is not None:
                        rate = Decimal(str(row[0])).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
                        self._ptax_cache[date_id] = rate
                        return rate
            except (OperationalError, InterfaceError) as exc:
                logger.warning(
                    "PostgreSQL PTAX lookup unavailable for date_id=%s (%s); checking DuckDB Parquet Lakehouse.",
                    date_id,
                    exc,
                )
            except Exception as exc:
                logger.error("PostgreSQL PTAX lookup failed for date_id=%s: %s", date_id, exc)
                raise

        # 2. Query DuckDB Parquet Lakehouse honoring PARQUET_BASE_DIR (date_id <= target_date_id)
        lakehouse_dir = get_parquet_base_dir() / "dim_currency_ptax"
        if lakehouse_dir.exists() and any(lakehouse_dir.rglob("*.parquet")):
            parquet_glob = (lakehouse_dir / "**" / "*.parquet").as_posix()
            duck_conn = get_duckdb_connection()
            try:
                q = """
                    SELECT ptax_sell_rate
                    FROM read_parquet(?, hive_partitioning=1)
                    WHERE currency_code = 'USD' AND date_id <= ?
                    ORDER BY date_id DESC
                    LIMIT 1
                """
                row = duck_conn.execute(q, [parquet_glob, date_id]).fetchone()
                if row and row[0] is not None:
                    rate = Decimal(str(row[0])).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
                    self._ptax_cache[date_id] = rate
                    return rate
            except Exception as exc:
                logger.error("DuckDB Parquet PTAX lookup failed for date_id=%s: %s", date_id, exc)
                raise
            finally:
                duck_conn.close()

        # 3. Fallback statutory default when no prior observation exists
        logger.warning(
            "No USD PTAX rate found on or before date_id=%s in PostgreSQL or Parquet; using fallback %s",
            date_id,
            DEFAULT_USD_PTAX_SELL_RATE,
        )
        self._ptax_cache[date_id] = DEFAULT_USD_PTAX_SELL_RATE
        return DEFAULT_USD_PTAX_SELL_RATE

    # -------------------------------------------------------------------------
    # 5. Canonical 8-Key Pre-Aggregation
    # -------------------------------------------------------------------------

    def parse_and_preaggregate_mdic_csv(
        self,
        csv_path: Union[str, Path],
        flow: Optional[str] = None,
        month: Optional[int] = None,
        ncm_prefix: Optional[str] = None,
        uf: Optional[str] = None,
    ) -> List[ComexOperationInput]:
        """
        Parses official MDIC semicolon-delimited CSVs or canonical comma-delimited CSVs
        (supporting UTF-8, CP1252, and Latin-1), applies early filtering (month, ncm_prefix, uf),
        and pre-aggregates rows sharing the 8-tuple natural key across CO_UNID.
        """
        path = Path(csv_path)
        if not path.exists():
            raise FileNotFoundError(f"Comex microdata CSV not found at {path}")

        accum: Dict[Tuple[int, int, str, str, int, str, int, int], Dict[str, Decimal]] = {}
        encoding = _detect_csv_encoding(path)

        with open(path, mode="r", encoding=encoding) as f:
            first_line = f.readline()
            delimiter = ";" if ";" in first_line else ","
            f.seek(0)
            reader = csv.reader(f, delimiter=delimiter)
            header = [col.strip().strip('"').upper() for col in next(reader, [])]
            header_map = {col: idx for idx, col in enumerate(header)}

            ano_idx = header_map.get("CO_ANO", header_map.get("YEAR", header_map.get("ANO")))
            mes_idx = header_map.get("CO_MES", header_map.get("MONTH", header_map.get("MES")))
            ncm_idx = header_map.get("CO_NCM", header_map.get("NCM_CODE", header_map.get("NCM")))
            pais_idx = header_map.get("CO_PAIS", header_map.get("COUNTRY_CODE", header_map.get("PAIS")))
            uf_idx = header_map.get("SG_UF_NCM", header_map.get("UF_CODE", header_map.get("SG_UF", header_map.get("UF"))))
            via_idx = header_map.get("CO_VIA", header_map.get("MODAL_CODE", header_map.get("VIA")))
            urf_idx = header_map.get("CO_URF", header_map.get("URF_CODE", header_map.get("URF")))

            qt_idx = header_map.get("QT_ESTAT", header_map.get("STATISTICAL_QUANTITY"))
            kg_idx = header_map.get("KG_LIQUIDO", header_map.get("NET_WEIGHT_KG"))
            fob_idx = header_map.get("VL_FOB", header_map.get("FOB_AMOUNT_USD"))
            freight_idx = header_map.get("VL_FRETE", header_map.get("FREIGHT_AMOUNT_USD"))
            ins_idx = header_map.get("VL_SEGURO", header_map.get("INSURANCE_AMOUNT_USD"))
            op_idx = header_map.get("OPERATION_TYPE", header_map.get("TIPO_OP"))
            cif_idx = header_map.get("CIF_AMOUNT_USD", header_map.get("VL_CIF"))
            landed_idx = header_map.get("LANDED_COST_BRL")

            if None in (ano_idx, mes_idx, ncm_idx, pais_idx, uf_idx, via_idx, urf_idx):
                raise ValueError(f"Required natural key columns missing from CSV {path.name}: {header}")

            for _row_idx, line in enumerate(reader, start=2):
                if not line or len(line) <= max(ano_idx, mes_idx, ncm_idx, pais_idx, uf_idx, via_idx, urf_idx):
                    continue

                ano_str = line[ano_idx].strip().strip('"')
                mes_str = line[mes_idx].strip().strip('"')
                if not (ano_str.isdigit() and mes_str.isdigit()):
                    continue

                ano = int(ano_str)
                mes = int(mes_str)

                if month is not None and mes != month:
                    continue

                ncm = str(line[ncm_idx]).strip().strip('"').zfill(8)
                if ncm_prefix and not ncm.startswith(str(ncm_prefix).strip()):
                    continue

                sg_uf = str(line[uf_idx]).strip().strip('"').upper()
                if uf and sg_uf != uf.strip().upper():
                    continue

                co_pais = int(line[pais_idx].strip().strip('"'))
                co_via = int(line[via_idx].strip().strip('"'))
                co_urf = int(line[urf_idx].strip().strip('"'))

                if op_idx is not None and len(line) > op_idx and line[op_idx].strip().strip('"'):
                    tipo_op = str(line[op_idx]).strip().strip('"').upper()
                    if flow and flow.upper() in ("IMPORT", "EXPORT") and tipo_op != flow.upper():
                        continue
                elif flow and flow.upper() in ("IMPORT", "EXPORT"):
                    tipo_op = flow.upper()
                else:
                    path_name_upper = path.name.upper()
                    tipo_op = "EXPORT" if "EXP" in path_name_upper else "IMPORT"

                key = (ano, mes, tipo_op, ncm, co_pais, sg_uf, co_via, co_urf)

                qt_val = Decimal(str(line[qt_idx]).strip().strip('"') or "0").quantize(
                    Decimal("0.0001"), rounding=ROUND_HALF_UP
                ) if qt_idx is not None and len(line) > qt_idx else Decimal("0.0000")

                kg_val = Decimal(str(line[kg_idx]).strip().strip('"') or "0").quantize(
                    Decimal("0.0001"), rounding=ROUND_HALF_UP
                ) if kg_idx is not None and len(line) > kg_idx else Decimal("0.0000")

                fob_val = Decimal(str(line[fob_idx]).strip().strip('"') or "0").quantize(
                    Decimal("0.0001"), rounding=ROUND_HALF_UP
                ) if fob_idx is not None and len(line) > fob_idx else Decimal("0.0000")

                if tipo_op == "IMPORT":
                    freight_val = Decimal(str(line[freight_idx]).strip().strip('"') or "0").quantize(
                        Decimal("0.0001"), rounding=ROUND_HALF_UP
                    ) if freight_idx is not None and len(line) > freight_idx and line[freight_idx].strip().strip('"') else Decimal("0.0000")

                    ins_val = Decimal(str(line[ins_idx]).strip().strip('"') or "0").quantize(
                        Decimal("0.0001"), rounding=ROUND_HALF_UP
                    ) if ins_idx is not None and len(line) > ins_idx and line[ins_idx].strip().strip('"') else Decimal("0.0000")
                else:
                    freight_val = Decimal("0.0000")
                    ins_val = Decimal("0.0000")

                cif_val = Decimal(str(line[cif_idx]).strip().strip('"') or "0").quantize(
                    Decimal("0.0001"), rounding=ROUND_HALF_UP
                ) if cif_idx is not None and len(line) > cif_idx and line[cif_idx].strip().strip('"') else Decimal("0.0000")

                landed_val = Decimal(str(line[landed_idx]).strip().strip('"') or "0").quantize(
                    Decimal("0.0001"), rounding=ROUND_HALF_UP
                ) if landed_idx is not None and len(line) > landed_idx and line[landed_idx].strip().strip('"') else Decimal("0.0000")

                if key not in accum:
                    accum[key] = {
                        "statistical_quantity": Decimal("0.0000"),
                        "net_weight_kg": Decimal("0.0000"),
                        "fob_amount_usd": Decimal("0.0000"),
                        "freight_amount_usd": Decimal("0.0000"),
                        "insurance_amount_usd": Decimal("0.0000"),
                        "cif_amount_usd": Decimal("0.0000"),
                        "landed_cost_brl": Decimal("0.0000"),
                    }

                accum[key]["statistical_quantity"] += qt_val
                accum[key]["net_weight_kg"] += kg_val
                accum[key]["fob_amount_usd"] += fob_val
                accum[key]["freight_amount_usd"] += freight_val
                accum[key]["insurance_amount_usd"] += ins_val
                if cif_val > Decimal("0.0000"):
                    accum[key]["cif_amount_usd"] += cif_val
                if landed_val > Decimal("0.0000"):
                    accum[key]["landed_cost_brl"] += landed_val

        results: List[ComexOperationInput] = []
        for key, sums in accum.items():
            ano, mes, tipo_op, ncm, co_pais, sg_uf, co_via, co_urf = key
            date_id = ano * 10000 + mes * 100 + 1

            fob = sums["fob_amount_usd"]
            freight = sums["freight_amount_usd"]
            ins = sums["insurance_amount_usd"]

            if tipo_op == "EXPORT":
                cif = fob
            else:
                computed_cif = (fob + freight + ins).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                cif = max(sums["cif_amount_usd"], computed_cif) if sums["cif_amount_usd"] > Decimal("0.0000") else computed_cif

            ptax_rate = self.resolve_ptax_sell_rate(date_id)
            if sums["landed_cost_brl"] > Decimal("0.0000"):
                landed_cost = sums["landed_cost_brl"]
            else:
                landed_cost = (cif * ptax_rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

            op = ComexOperationInput(
                date_id=date_id,
                operation_type=tipo_op,
                ncm_code=ncm,
                country_code=co_pais,
                uf_code=sg_uf,
                modal_code=co_via,
                urf_code=co_urf,
                statistical_quantity=sums["statistical_quantity"],
                net_weight_kg=sums["net_weight_kg"],
                fob_amount_usd=fob,
                freight_amount_usd=freight,
                insurance_amount_usd=ins,
                cif_amount_usd=cif,
                landed_cost_brl=landed_cost,
            )
            results.append(op)

        logger.info("Pre-aggregated %s: %d distinct canonical operations generated.", path.name, len(results))
        return results

    # -------------------------------------------------------------------------
    # 6. Hybrid Storage Persistence (Lakehouse + PostgreSQL)
    # -------------------------------------------------------------------------

    def sync_mdic_operations(
        self,
        operations: List[ComexOperationInput],
        limit_pg: Optional[int] = None,
        all_pg: bool = False,
    ) -> Dict[str, int]:
        """
        Persists 100% of operations to Apache Parquet Hive Lakehouse with DECIMAL(19,4)
        and all operations (or up to explicit limit_pg when provided) into PostgreSQL
        fact_comex_operations with ON CONFLICT (operation_hash_id) DO UPDATE.
        """
        if limit_pg is not None and limit_pg < 0:
            raise ValueError(f"limit_pg must be >= 0 or None, got {limit_pg}")

        if not operations:
            return {"total_operations": 0, "parquet_persisted": 0, "postgres_persisted": 0}

        parquet_records = [
            {
                "year": op.date_id // 10000,
                "month": (op.date_id % 10000) // 100,
                "operation_hash_id": op.operation_hash_id,
                "date_id": op.date_id,
                "operation_type": op.operation_type,
                "ncm_code": op.ncm_code,
                "country_code": op.country_code,
                "uf_code": op.uf_code,
                "modal_code": op.modal_code,
                "urf_code": op.urf_code,
                "statistical_quantity": op.statistical_quantity,
                "net_weight_kg": op.net_weight_kg,
                "fob_amount_usd": op.fob_amount_usd,
                "freight_amount_usd": op.freight_amount_usd,
                "insurance_amount_usd": op.insurance_amount_usd,
                "cif_amount_usd": op.cif_amount_usd,
                "landed_cost_brl": op.landed_cost_brl,
            }
            for op in operations
        ]
        try:
            write_records_to_parquet_hive(
                records=parquet_records,
                table_name="fact_comex_operations",
                partition_cols=["year", "month"],
                schema=FACT_COMEX_PARQUET_SCHEMA,
            )
            logger.info("Persisted %d operations into Parquet Hive Lakehouse.", len(operations))
        except Exception as exc:
            logger.exception("Parquet Lakehouse persistence failed: %s", exc)
            raise

        pg_ops = operations if (all_pg or limit_pg is None) else operations[:limit_pg]
        pg_persisted = 0

        if self.engine is not None and pg_ops:
            try:
                self.ensure_dimensions_for_operations(pg_ops)

                sql = text(
                    """
                    INSERT INTO fact_comex_operations (
                        operation_hash_id, date_id, operation_type, ncm_code,
                        country_code, uf_code, modal_code, urf_code,
                        statistical_quantity, net_weight_kg, fob_amount_usd,
                        freight_amount_usd, insurance_amount_usd, cif_amount_usd,
                        landed_cost_brl, created_at
                    )
                    VALUES (
                        :hash_id, :date_id, :op_type, :ncm, :country, :uf, :modal, :urf,
                        :qty, :weight, :fob, :freight, :insurance, :cif, :landed, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (operation_hash_id)
                    DO UPDATE SET
                        statistical_quantity = EXCLUDED.statistical_quantity,
                        net_weight_kg = EXCLUDED.net_weight_kg,
                        fob_amount_usd = EXCLUDED.fob_amount_usd,
                        freight_amount_usd = EXCLUDED.freight_amount_usd,
                        insurance_amount_usd = EXCLUDED.insurance_amount_usd,
                        cif_amount_usd = EXCLUDED.cif_amount_usd,
                        landed_cost_brl = EXCLUDED.landed_cost_brl
                    """
                )
                rows = [
                    {
                        "hash_id": op.operation_hash_id,
                        "date_id": op.date_id,
                        "op_type": op.operation_type,
                        "ncm": op.ncm_code,
                        "country": op.country_code,
                        "uf": op.uf_code,
                        "modal": op.modal_code,
                        "urf": op.urf_code,
                        "qty": op.statistical_quantity,
                        "weight": op.net_weight_kg,
                        "fob": op.fob_amount_usd,
                        "freight": op.freight_amount_usd,
                        "insurance": op.insurance_amount_usd,
                        "cif": op.cif_amount_usd,
                        "landed": op.landed_cost_brl,
                    }
                    for op in pg_ops
                ]
                with self.engine.begin() as conn:
                    conn.execute(sql, rows)
                pg_persisted = len(pg_ops)
                logger.info("Persisted %d operations into PostgreSQL fact_comex_operations.", pg_persisted)
            except Exception as exc:
                self._handle_pg_write_exception("fact_comex_operations persistence", exc)

        return {
            "total_operations": len(operations),
            "parquet_persisted": len(operations),
            "postgres_persisted": pg_persisted,
        }

    # -------------------------------------------------------------------------
    # 7. Orchestrator
    # -------------------------------------------------------------------------

    def fetch_and_load(
        self,
        year: int,
        month: Optional[int] = None,
        flow: str = "BOTH",
        ncm_prefix: Optional[str] = None,
        uf: Optional[str] = None,
        limit_pg: Optional[int] = None,
        all_pg: bool = False,
        force_download: bool = False,
        csv_path: Optional[Union[str, Path]] = None,
    ) -> Dict[str, Any]:
        """
        End-to-end execution of MDIC bulk ingestion:
        1. Dimension sync (NCM.csv and PAIS.csv).
        2. Microdata download or explicit offline sample resolution.
        3. Canonical 8-key pre-aggregation with early streaming filters.
        4. Hybrid storage persistence to Parquet Lakehouse and PostgreSQL.
        """
        logger.info(
            "Starting MDIC Bulk Loader: year=%s, month=%s, flow=%s, ncm_prefix=%s, uf=%s, limit_pg=%s, all_pg=%s",
            year,
            month,
            flow,
            ncm_prefix,
            uf,
            limit_pg,
            all_pg,
        )

        dim_stats = self.sync_auxiliary_dimensions(force_download=force_download)

        operations: List[ComexOperationInput] = []

        if csv_path is not None:
            ops = self.parse_and_preaggregate_mdic_csv(
                csv_path=csv_path,
                flow=flow if flow != "BOTH" else None,
                month=month,
                ncm_prefix=ncm_prefix,
                uf=uf,
            )
            operations.extend(ops)
        else:
            flows = ["IMPORT", "EXPORT"] if flow == "BOTH" else [flow.upper()]
            for f in flows:
                file_path = self.get_comex_file(year=year, flow=f, force_download=force_download)
                if file_path is not None and file_path.exists():
                    ops = self.parse_and_preaggregate_mdic_csv(
                        csv_path=file_path,
                        flow=f,
                        month=month,
                        ncm_prefix=ncm_prefix,
                        uf=uf,
                    )
                    operations.extend(ops)
                else:
                    raise FileNotFoundError(f"No Comex microdata file found for flow={f}, year={year}")

        persist_stats = self.sync_mdic_operations(
            operations=operations,
            limit_pg=limit_pg,
            all_pg=all_pg,
        )

        return {
            "year": year,
            "month": month,
            "flow": flow,
            "dimensions_synced": dim_stats,
            "persistence": persist_stats,
        }


# -----------------------------------------------------------------------------
# CLI Entrypoint
# -----------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="MDIC Comex Stat Real Bulk Ingestion Pipeline")
    parser.add_argument("--year", type=int, default=2024, help="Statistical reference year (e.g. 2024, 2026)")
    parser.add_argument("--month", type=int, default=None, help="Reference month (1-12) for early filtering")
    parser.add_argument(
        "--flow",
        type=str,
        default="BOTH",
        choices=["IMPORT", "EXPORT", "BOTH"],
        help="Customs flow direction",
    )
    parser.add_argument("--ncm-prefix", type=str, default=None, help="Filter operations by NCM prefix (e.g. 84, 8471)")
    parser.add_argument("--uf", type=str, default=None, help="Filter operations by Brazilian state (e.g. SP, RJ, MG)")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of rows to insert into PostgreSQL (default: sync all)",
    )
    parser.add_argument("--all-pg", action="store_true", help="Insert all pre-aggregated rows into PostgreSQL")
    parser.add_argument("--offline", action="store_true", help="Run strictly offline using local cache/samples")
    parser.add_argument("--force-download", action="store_true", help="Force re-download even if cached on disk")
    parser.add_argument("--csv-path", type=str, default=None, help="Direct path to custom CSV microdata file")

    args = parser.parse_args()

    loader = MDICBulkLoader(offline=args.offline)
    result = loader.fetch_and_load(
        year=args.year,
        month=args.month,
        flow=args.flow,
        ncm_prefix=args.ncm_prefix,
        uf=args.uf,
        limit_pg=args.limit,
        all_pg=args.all_pg,
        force_download=args.force_download,
        csv_path=args.csv_path,
    )

    print("\n" + "=" * 60)
    print(" 🚀 MDIC COMEX STAT BULK INGESTION COMPLETED")
    print("=" * 60)
    print(f" Year:                  {result['year']}")
    print(f" Month:                 {result['month'] or 'ALL'}")
    print(f" Flow:                  {result['flow']}")
    print(f" Dimensions Synced:     NCM={result['dimensions_synced']['ncm_count']}, PAIS={result['dimensions_synced']['country_count']}")
    print(f" Total Operations:      {result['persistence']['total_operations']}")
    print(f" Parquet Lakehouse:     {result['persistence']['parquet_persisted']}")
    print(f" PostgreSQL Fact:       {result['persistence']['postgres_persisted']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
