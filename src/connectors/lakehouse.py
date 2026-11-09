import os
from pathlib import Path
from typing import List, Dict, Any
import duckdb
import pyarrow as pa
import pyarrow.dataset as ds

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/local_lakehouse.duckdb")
PARQUET_BASE_DIR = os.getenv("PARQUET_BASE_DIR", "./data/parquet_lakehouse")

def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Retorna uma conexão DuckDB local para consultas analíticas rápidas."""
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(DUCKDB_PATH)

def write_records_to_parquet_hive(
    records: List[Dict[str, Any]], 
    table_name: str,
    partition_cols: List[str] = ["year", "month"]
) -> None:
    """Serializa dicionários em Apache Parquet (Snappy) com particionamento Hive."""
    if not records:
        return
        
    table = pa.Table.from_pylist(records)
    output_dir = Path(PARQUET_BASE_DIR) / table_name
    output_dir.mkdir(parents=True, exist_ok=True)

    file_format = ds.ParquetFileFormat()
    file_options = file_format.make_write_options(compression="snappy")

    ds.write_dataset(
        table,
        base_dir=str(output_dir),
        format=file_format,
        partitioning=partition_cols,
        partitioning_flavor="hive",
        existing_data_behavior="overwrite_or_ignore",
        file_options=file_options
    )
