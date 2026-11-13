from contextlib import contextmanager
import fcntl
import logging
import os
from pathlib import Path
import threading
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple
from uuid import uuid4

from dotenv import load_dotenv
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

load_dotenv()

logger = logging.getLogger("comex.connectors.lakehouse")

_DEFAULT_DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/comex_lakehouse.duckdb")
_DEFAULT_PARQUET_BASE_DIR = os.getenv("PARQUET_BASE_DIR", "./data/parquet_lakehouse")

DUCKDB_PATH = _DEFAULT_DUCKDB_PATH
PARQUET_BASE_DIR = _DEFAULT_PARQUET_BASE_DIR

DEFAULT_DEDUP_KEYS: Dict[str, Tuple[str, ...]] = {
    "fact_comex_operations": ("operation_hash_id",),
    "dim_currency_ptax": ("date_id", "currency_code"),
}

_write_lock = threading.RLock()


def get_duckdb_path() -> Path:
    """Resolve the DuckDB file path, honoring env config and test module overrides."""
    if DUCKDB_PATH != _DEFAULT_DUCKDB_PATH:
        return Path(DUCKDB_PATH)
    return Path(os.getenv("DUCKDB_PATH", DUCKDB_PATH))


def get_parquet_base_dir() -> Path:
    """Resolve the Parquet lakehouse base directory, honoring env config and test overrides."""
    if PARQUET_BASE_DIR != _DEFAULT_PARQUET_BASE_DIR:
        return Path(PARQUET_BASE_DIR)
    return Path(os.getenv("PARQUET_BASE_DIR", PARQUET_BASE_DIR))


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Retorna uma conexão DuckDB local para consultas analíticas rápidas."""
    db_path = get_duckdb_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


@contextmanager
def duckdb_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Context manager guaranteeing DuckDB connection closure in a finally block."""
    conn = get_duckdb_connection()
    try:
        yield conn
    finally:
        conn.close()


def _validate_safe_segment(value: str, label: str) -> str:
    """Reject path traversal or separator characters in table/partition names."""
    if not value or ".." in value or "/" in value or "\\" in value:
        raise ValueError(f"Unsafe {label} value for Parquet Hive path: {value!r}")
    return value


def write_records_to_parquet_hive(
    records: List[Dict[str, Any]],
    table_name: str,
    partition_cols: Optional[Sequence[str]] = None,
    schema: Optional[pa.Schema] = None,
    dedup_keys: Optional[Sequence[str]] = None,
) -> None:
    """
    Serializa dicionários em Apache Parquet (Snappy) com particionamento Hive
    e merge atômico por partição (read -> merge -> deduplicate -> temp write -> atomic replace),
    preservando registros existentes em partições compartilhadas ou vizinhas.
    """
    if not records:
        return

    _validate_safe_segment(table_name, "table_name")
    pcols = list(partition_cols) if partition_cols is not None else ["year", "month"]
    keys = tuple(dedup_keys) if dedup_keys is not None else DEFAULT_DEDUP_KEYS.get(table_name, ())

    output_dir = get_parquet_base_dir() / table_name
    output_dir.mkdir(parents=True, exist_ok=True)

    partitions: Dict[Tuple[Tuple[str, Any], ...], List[Dict[str, Any]]] = {}
    for rec in records:
        part_key = tuple((col, rec[col]) for col in pcols)
        partitions.setdefault(part_key, []).append(dict(rec))

    lock_file_path = output_dir / ".lakehouse.lock"

    with _write_lock:
        with open(lock_file_path, "a+", encoding="utf-8") as lock_fh:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
            try:
                for part_key, incoming_rows in partitions.items():
                    part_dir = output_dir
                    for col, val in part_key:
                        _validate_safe_segment(col, "partition column")
                        val_str = _validate_safe_segment(str(val), f"partition value ({col})")
                        if "=" in val_str:
                            raise ValueError(f"Unsafe partition value containing '=': {val_str!r}")
                        part_dir = part_dir / f"{col}={val_str}"

                    part_dir.mkdir(parents=True, exist_ok=True)
                    existing_files = sorted(part_dir.glob("*.parquet"))
                    existing_rows: List[Dict[str, Any]] = []

                    for pq_file in existing_files:
                        existing_table = pq.read_table(pq_file)
                        for row in existing_table.to_pylist():
                            for col, val in part_key:
                                row.setdefault(col, val)
                            existing_rows.append(row)

                    if keys:
                        merged_by_key: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
                        for row in existing_rows:
                            k = tuple(row[key_col] for key_col in keys)
                            merged_by_key[k] = row
                        for row in incoming_rows:
                            k = tuple(row[key_col] for key_col in keys)
                            merged_by_key[k] = row
                        merged_rows = list(merged_by_key.values())
                    else:
                        merged_rows = existing_rows + incoming_rows

                    merged_table = (
                        pa.Table.from_pylist(merged_rows, schema=schema)
                        if schema is not None
                        else pa.Table.from_pylist(merged_rows)
                    )

                    cols_to_drop = [col for col in pcols if col in merged_table.column_names]
                    write_table = merged_table.drop(cols_to_drop) if cols_to_drop else merged_table

                    target_file = part_dir / "part-0.parquet"
                    tmp_file = part_dir / f".part-0.parquet.tmp.{uuid4().hex}"
                    try:
                        pq.write_table(write_table, tmp_file, compression="snappy")
                        os.replace(tmp_file, target_file)
                        for old_file in existing_files:
                            if old_file != target_file and old_file.exists():
                                old_file.unlink()
                    except BaseException:
                        tmp_file.unlink(missing_ok=True)
                        raise
            finally:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
