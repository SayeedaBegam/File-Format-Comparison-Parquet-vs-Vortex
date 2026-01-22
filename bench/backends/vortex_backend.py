"""bench/backends/vortex_backend.py

Vortex Backend for DuckDB Benchmarking (DuckDB official Vortex extension).

This backend benchmarks the *DuckDB Vortex extension* using native file scans
(`read_vortex()` + `COPY ... (FORMAT vortex)`). That means read-time
measurements include real file I/O + decompression/decoding, making them
comparable to Parquet scans.

DuckDB docs (examples):
  INSTALL vortex;
  LOAD vortex;
  COPY (SELECT * FROM t) TO 'my.vortex' (FORMAT vortex);
  SELECT * FROM read_vortex('my.vortex');
"""

from __future__ import annotations

import platform
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import duckdb


@dataclass
class VortexOptions:
    # DuckDB public docs currently show only FORMAT vortex. We keep "compact" for
    # compatibility/labeling; it is not translated into a DuckDB COPY option.
    compact: bool = False


def get_version_info() -> Dict[str, str]:
    return {"python": platform.python_version(), "duckdb": duckdb.__version__}


def _dir_size_bytes(p: Path) -> int:
    if p.is_file():
        return p.stat().st_size
    total = 0
    for f in p.rglob("*"):
        if f.is_file():
            total += f.stat().st_size
    return total


def _sql_quote_path(path: str) -> str:
    # Escape single quotes for SQL string literal
    return path.replace("'", "''")


def _ensure_vortex_loaded(con: duckdb.DuckDBPyConnection) -> None:
    """Ensure the DuckDB Vortex extension is installed and loaded."""
    # 1) Try LOAD first (works if already installed)
    try:
        con.execute("LOAD vortex;")
        return
    except Exception:
        pass

    # 2) Try INSTALL from default repo; if that fails, try community
    try:
        con.execute("INSTALL vortex;")
    except Exception:
        try:
            con.execute("INSTALL vortex FROM community;")
        except Exception:
            pass

    # 3) LOAD (will raise if still unavailable)
    con.execute("LOAD vortex;")


def _variant_name(options: Dict[str, Any]) -> str:
    return "vortex_compact" if options.get("compact") else "vortex_default"


def write_vortex(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    out_path: str,
    options: Dict[str, Any],
) -> Dict[str, Any]:
    """Write a DuckDB table to a Vortex file using DuckDB's Vortex extension."""
    _ensure_vortex_loaded(con)

    opts = VortexOptions(**{k: v for k, v in (options or {}).items() if k in {"compact"}})

    out = Path(out_path)
    # If out_path is not a .vortex file, treat it as a directory and write <table_name>.vortex inside.
    if out.suffix.lower() != ".vortex":
        out = out / f"{table_name}.vortex"
    out.parent.mkdir(parents=True, exist_ok=True)

    sql = f"COPY (SELECT * FROM {table_name}) TO '{_sql_quote_path(str(out))}' (FORMAT vortex);"

    t0 = time.perf_counter()
    con.execute(sql)
    t1 = time.perf_counter()

    size = _dir_size_bytes(out)

    return {
        "format": "vortex",
        "variant": _variant_name({"compact": bool(opts.compact)}),
        "compression_time_s": t1 - t0,
        "output_size_bytes": size,
        "vortex_path": str(out),
        "writer_options": {
            "compact_requested": bool(opts.compact),
            "note": "Writer uses DuckDB Vortex extension defaults (no explicit strategy knobs set).",
        },
    }


def scan_expr(out_path: str) -> str:
    """Return a DuckDB FROM expression to scan the written Vortex file(s)."""
    _ = out_path  # keep signature stable
    p = Path(out_path)
    if p.is_dir():
        return f"read_vortex('{_sql_quote_path(str(p))}/**/*.vortex')"
    return f"read_vortex('{_sql_quote_path(str(p))}')"