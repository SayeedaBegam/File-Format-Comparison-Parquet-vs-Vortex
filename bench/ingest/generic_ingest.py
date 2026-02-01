# bench/ingest/generic_ingest.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, Dict, Any
import duckdb
import re


def create_base_table_from_csv(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    csv_path: str,
    schema_sql_path: Optional[str] = None,
    read_csv_options: Optional[Dict[str, Any]] = None,
) -> str:
    """
    If schema_sql_path is provided, we create the table with it first, then COPY in.
    Otherwise we infer types from DuckDB read_csv_auto.
    """
    opts = read_csv_options or {}
    p = Path(csv_path)

    if schema_sql_path:
        schema_sql = Path(schema_sql_path).read_text(encoding="utf-8")
        con.execute(schema_sql)
        # If schema SQL creates a different table name, auto-detect it.
        target_table = table_name
        if not _table_exists(con, target_table):
            detected = _extract_table_name_from_schema(schema_sql)
            if detected and _table_exists(con, detected):
                target_table = detected
            else:
                available = _list_tables(con)
                if len(available) == 1:
                    target_table = available[0]
                else:
                    hint = f"Detected table: {detected}" if detected else "No CREATE TABLE name detected."
                    raise duckdb.CatalogException(
                        f"Table '{table_name}' not found after applying schema. {hint} "
                        f"Available tables: {', '.join(available) if available else 'none'}."
                    )
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
        con.execute(f"COPY {target_table} FROM '{str(p)}' ({copy_args_sql});")
        if target_table != table_name:
            # Materialize into the requested table name so the schema is enforced there too.
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {target_table};")
            return table_name
        return target_table

    # inference path
    csv_args = _format_kv(opts)
    if csv_args:
        csv_args = f", {csv_args}"
    if p.is_dir():
        glob = f"{str(p)}/**/*.csv"
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{glob}'{csv_args});")
    else:
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{str(p)}'{csv_args});")
    return table_name


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


def _list_tables(con: duckdb.DuckDBPyConnection) -> list[str]:
    try:
        rows = con.execute("SHOW TABLES;").fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 1;")
        return True
    except Exception:
        return False


def _extract_table_name_from_schema(schema_sql: str) -> Optional[str]:
    # Best-effort: match the first CREATE TABLE statement.
    match = re.search(r"CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?([\\w\\.\\\"`]+)", schema_sql, re.IGNORECASE)
    if not match:
        return None
    raw = match.group(1)
    return raw.strip().strip('\"`')
