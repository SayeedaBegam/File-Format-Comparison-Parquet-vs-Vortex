# bench/ingest/generic_ingest.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, Dict, Any
import duckdb


def create_base_table_from_csv(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    csv_path: str,
    schema_sql_path: Optional[str] = None,
    read_csv_options: Optional[Dict[str, Any]] = None,
) -> None:
    """
    If schema_sql_path is provided, we create the table with it first, then COPY in.
    Otherwise we infer types from DuckDB read_csv_auto.
    """
    opts = read_csv_options or {}
    p = Path(csv_path)

    if schema_sql_path:
        schema_sql = Path(schema_sql_path).read_text(encoding="utf-8")
        con.execute(schema_sql)
        # assumes schema SQL creates table with correct name
        # if not, user should pass matching table_name in schema file
        copy_args = ["AUTO_DETECT FALSE"]
        if opts:
            # Map read_csv_auto-style options to COPY options
            if "delim" in opts or "delimiter" in opts:
                delim = opts.get("delim", opts.get("delimiter"))
                copy_args.append(f"DELIMITER '{delim}'")
            if "header" in opts:
                copy_args.append(f"HEADER {'TRUE' if opts['header'] else 'FALSE'}")
            if "nullstr" in opts:
                copy_args.append(f"NULLSTR '{opts['nullstr']}'")
            if "ignore_errors" in opts:
                copy_args.append(f"IGNORE_ERRORS {'TRUE' if opts['ignore_errors'] else 'FALSE'}")
        copy_args_sql = ", ".join(copy_args)
        con.execute(f"COPY {table_name} FROM '{str(p)}' ({copy_args_sql});")
        return

    # inference path
    csv_args = _format_kv(opts)
    if csv_args:
        csv_args = f", {csv_args}"
    if p.is_dir():
        glob = f"{str(p)}/**/*.csv"
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{glob}'{csv_args});")
    else:
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{str(p)}'{csv_args});")


def create_base_table_from_parquet(con: duckdb.DuckDBPyConnection, table_name: str, parquet_path: str) -> None:
    p = Path(parquet_path)
    if p.is_dir():
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{str(p)}/**/*.parquet');")
    else:
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{str(p)}');")


def _format_kv(opts: Dict[str, Any]) -> str:
    # DuckDB wants named args like delim=',', header=True etc.
    if not opts:
        return ""
    parts = []
    for k, v in opts.items():
        if isinstance(v, str):
            parts.append(f"{k}='{v}'")
        elif isinstance(v, bool):
            parts.append(f"{k}={'true' if v else 'false'}")
        else:
            parts.append(f"{k}={v}")
    return ", ".join(parts)
