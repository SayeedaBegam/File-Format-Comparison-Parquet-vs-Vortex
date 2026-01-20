# bench/backends/parquet_backend.py
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Optional

import duckdb


@dataclass
class ParquetOptions:
    codec: str = "zstd"          # zstd, snappy, gzip, uncompressed
    row_group_size: int = 128_000
    compression_level: Optional[int] = None  # codec-dependent; DuckDB supports for some codecs


def _dir_size_bytes(p: Path) -> int:
    if p.is_file():
        return p.stat().st_size
    total = 0
    for f in p.rglob("*"):
        if f.is_file():
            total += f.stat().st_size
    return total


def write(con: duckdb.DuckDBPyConnection, table_name: str, out_path: str, options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Contract: Write compressed Parquet from a DuckDB table and return:
      - compression_time_s
      - output_size_bytes
      - metadata (codec, row_group_size, compression_level)
    """
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    opts = ParquetOptions(**options)

    # We'll write a single parquet file for simplicity.
    # If you prefer partitioned/folder output, change out_path and size calc still works.
    if out.suffix.lower() != ".parquet":
        out = out / f"{table_name}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)

    # DuckDB COPY supports PARQUET with compression and row_group_size.
    # Compression level support can vary; we keep it optional.
    copy_parts = [
        f"COPY (SELECT * FROM {table_name}) TO '{str(out)}' (FORMAT PARQUET",
        f", COMPRESSION '{opts.codec}'",
        f", ROW_GROUP_SIZE {opts.row_group_size}",
    ]
    if opts.compression_level is not None:
        copy_parts.append(f", COMPRESSION_LEVEL {int(opts.compression_level)}")
    copy_parts.append(");")
    sql = "".join(copy_parts)

    t0 = time.perf_counter()
    con.execute(sql)
    t1 = time.perf_counter()

    size = _dir_size_bytes(out)

    return {
        "compression_time_s": t1 - t0,
        "output_size_bytes": size,
        "codec": opts.codec,
        "row_group_size": opts.row_group_size,
        "compression_level": opts.compression_level,
        "parquet_path": str(out),
    }


def scan_expr(out_path: str) -> str:
    """
    Contract: return SQL FROM expression for reading the compressed data.
    """
    p = Path(out_path)
    if p.is_dir():
        # if output is a folder, read all parquet files in it
        return f"read_parquet('{str(p)}/**/*.parquet')"
    # single file
    return f"read_parquet('{str(p)}')"
