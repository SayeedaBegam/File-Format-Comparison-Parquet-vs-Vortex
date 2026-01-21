"""
Vortex Backend for DuckDB Benchmarking.

SETUP: pip install vortex-data pyarrow duckdb

This backend supports both old (0.33.x) and new (0.58.x+) Vortex APIs.
"""

from __future__ import annotations

import json
import logging
import os
import platform
import sys
import time
from pathlib import Path
from typing import Any, Dict, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

_VORTEX_AVAILABLE = False
_VORTEX_VERSION = "unknown"
_VORTEX_HAS_IO = False
_PYARROW_VERSION = pa.__version__
_DUCKDB_VERSION = "unknown"

try:
    import duckdb
    _DUCKDB_VERSION = duckdb.__version__
except ImportError:
    duckdb = None

try:
    import vortex as vx
    _VORTEX_AVAILABLE = True
    _VORTEX_HAS_IO = hasattr(vx, 'io') and hasattr(vx.io, 'write')
    try:
        from importlib.metadata import version
        _VORTEX_VERSION = version("vortex-data")
    except Exception:
        _VORTEX_VERSION = getattr(vx, "__version__", "unknown")
except ImportError:
    vx = None


def _check_dependencies() -> None:
    missing = []
    if duckdb is None:
        missing.append("duckdb")
    if not _VORTEX_AVAILABLE:
        missing.append("vortex-data")
    if missing:
        raise ImportError(f"Missing: {', '.join(missing)}. pip install them.")


def get_version_info() -> Dict[str, str]:
    return {
        "python": platform.python_version(),
        "duckdb": _DUCKDB_VERSION,
        "pyarrow": _PYARROW_VERSION,
        "vortex": _VORTEX_VERSION,
        "vortex_has_io": str(_VORTEX_HAS_IO),
    }


def _dir_size(p: Path) -> int:
    if p.is_file():
        return p.stat().st_size
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())


def _ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def _variant_name(options: Dict[str, Any]) -> str:
    if options.get("compact"):
        return "vortex_compact"
    return "vortex_default"


def write_vortex(
    con,
    table_name: str,
    out_path: str,
    options: Dict[str, Any],
) -> Dict[str, Any]:
    """Write DuckDB table to Vortex format."""
    _check_dependencies()
    import vortex as vx

    logger.info(f"write_vortex: {table_name} -> {out_path}")
    out_dir = _ensure_dir(Path(out_path))
    use_compact = options.get("compact", False)

    try:
        arrow_table = con.execute(f"SELECT * FROM {table_name}").fetch_arrow_table()
        logger.info(f"Got {arrow_table.num_rows} rows")

        try:
            vx_array = vx.array(arrow_table)
            if use_compact:
                vx_array = vx.compress(vx_array)

            t0 = time.perf_counter()

            if _VORTEX_HAS_IO:
                vfile = out_dir / f"{table_name}.vortex"
                if use_compact:
                    vx.io.VortexWriteOptions.compact().write_path(vx_array, str(vfile))
                else:
                    vx.io.write(vx_array, str(vfile))
            else:
                afile = out_dir / f"{table_name}.vortex.arrow"
                arr_out = vx_array.to_arrow_table()
                with pa.OSFile(str(afile), 'wb') as f:
                    writer = ipc.new_file(f, arr_out.schema)
                    writer.write_table(arr_out)
                    writer.close()
                meta = {"vortex": _VORTEX_VERSION, "compact": use_compact}
                (out_dir / f"{table_name}.meta").write_text(json.dumps(meta))

            compression_time = time.perf_counter() - t0
            size = _dir_size(out_dir)

            return {
                "format": "vortex",
                "variant": _variant_name(options),
                "compression_time_s": compression_time,
                "compressed_size_bytes": size,
                "output_size_bytes": size,
                "writer_options": {"compact": use_compact, "has_io": _VORTEX_HAS_IO},
            }
        except Exception:
            _log_cast_diagnostics(arrow_table)
            raise
    except Exception as e:
        raise RuntimeError(f"write_vortex failed: {e}") from e


def _log_cast_diagnostics(tbl: pa.Table) -> None:
    # Best-effort diagnostics for Vortex cast failures.
    for name in tbl.schema.names:
        col = tbl[name]
        if not pa.types.is_integer(col.type):
            continue
        try:
            min_val = pc.min(col).as_py()
            max_val = pc.max(col).as_py()
            logger.error(f"Column '{name}' type {col.type} min={min_val} max={max_val}")
            if min_val is not None and min_val < 0:
                try:
                    import numpy as np
                    vals = col.to_numpy(zero_copy_only=False)
                    neg_idx = np.where(vals < 0)[0]
                    if neg_idx.size > 0:
                        i = int(neg_idx[0])
                        logger.error(f"First negative in '{name}' at row {i}: {vals[i]}")
                except Exception as e:
                    logger.error(f"Failed to locate negative row for '{name}': {e}")
            # U16 range hint for common cast failures
            if max_val is not None and max_val > 65535:
                logger.error(f"Column '{name}' exceeds U16 max: {max_val}")
        except Exception as e:
            logger.error(f"Failed diagnostics for '{name}': {e}")


def scan_expr(out_path: str) -> str:
    """Return view name for DuckDB queries (register first)."""
    return "vortex_dataset"


def get_vortex_arrow_table(out_path: str) -> pa.Table:
    """Load Vortex data as PyArrow Table."""
    _check_dependencies()
    import vortex as vx

    p = Path(out_path)

    if _VORTEX_HAS_IO:
        if p.is_dir():
            vfiles = list(p.glob("*.vortex")) or list(p.rglob("*.vortex"))
            if not vfiles:
                raise FileNotFoundError(f"No .vortex in {out_path}")
            vpath = vfiles[0]
        else:
            vpath = p
        return vx.open(str(vpath)).scan().read_all().to_arrow_table()

    if p.is_dir():
        afiles = list(p.glob("*.vortex.arrow")) or list(p.rglob("*.vortex.arrow"))
        if not afiles:
            raise FileNotFoundError(f"No .vortex.arrow in {out_path}")
        apath = afiles[0]
    else:
        apath = p
    with pa.OSFile(str(apath), 'rb') as f:
        return ipc.open_file(f).read_all()


def register_vortex_dataset(con, out_path: str, view_name: str = "vortex_dataset") -> str:
    """Register Vortex data with DuckDB using Arrow replacement scan."""
    tbl = get_vortex_arrow_table(out_path)
    # Use arrow_scan for DuckDB to avoid dataset import issues
    con.execute(f"CREATE OR REPLACE TEMP TABLE {view_name} AS SELECT * FROM tbl")
    return view_name


def verify_readable(con, out_path: str) -> None:
    """Verify Vortex data is readable."""
    _check_dependencies()
    try:
        tbl = get_vortex_arrow_table(out_path)
        # Query directly from the arrow table
        count = duckdb.execute("SELECT COUNT(*) FROM tbl").fetchone()[0]
        logger.info(f"Verified readable: {count} rows")
    except Exception as e:
        raise RuntimeError(f"Not readable: {out_path}: {e}") from e


def compare_min(con, table_name: str, out_path: str, col: str) -> Tuple[Any, Any]:
    """Compare min(col) between table and Vortex data."""
    _check_dependencies()

    orig = con.execute(f"SELECT min({col}) FROM {table_name}").fetchone()[0]

    tbl = get_vortex_arrow_table(out_path)
    vx_min = duckdb.execute(f"SELECT min({col}) FROM tbl").fetchone()[0]

    if orig != vx_min:
        raise AssertionError(f"Mismatch min({col}): {orig} vs {vx_min}")
    return (orig, vx_min)


if __name__ == "__main__":
    print("=" * 50)
    print("Vortex Backend Test")
    print("=" * 50)

    try:
        _check_dependencies()
        print("Dependencies OK")
    except ImportError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    print(f"Versions: {get_version_info()}")

    import duckdb
    import tempfile

    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE t AS SELECT i AS id, i*2 AS val, CAST(i*0.5 AS DOUBLE) AS f
        FROM range(1,101) AS _(i)
    """)
    print(f"Created table: {con.execute('SELECT COUNT(*) FROM t').fetchone()[0]} rows")

    with tempfile.TemporaryDirectory() as td:
        out = os.path.join(td, "vx")
        stats = write_vortex(con, "t", out, {})
        print(f"Write: {stats['compression_time_s']:.3f}s, {stats['compressed_size_bytes']}B")

        verify_readable(con, out)
        print("Readable: OK")

        for c in ["id", "val", "f"]:
            o, v = compare_min(con, "t", out, c)
            print(f"min({c}): {o} == {v} OK")

        out2 = os.path.join(td, "vx_c")
        stats2 = write_vortex(con, "t", out2, {"compact": True})
        print(f"Compact: {stats2['compressed_size_bytes']}B")
        verify_readable(con, out2)

    print("All tests passed!")
