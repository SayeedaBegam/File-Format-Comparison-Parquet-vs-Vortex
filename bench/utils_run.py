# bench/utils_run.py
from __future__ import annotations

import statistics
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb


def timed_query(con: duckdb.DuckDBPyConnection, sql: str, repeats: int, warmup: int) -> Dict[str, Any]:
    for _ in range(warmup):
        con.execute(sql).fetchall()

    times_ms: List[float] = []
    result_value = None
    for _ in range(repeats):
        t0 = time.perf_counter()
        res = con.execute(sql).fetchone()
        t1 = time.perf_counter()
        times_ms.append((t1 - t0) * 1000.0)
        result_value = res[0] if res else None

    times_ms_sorted = sorted(times_ms)
    median = statistics.median(times_ms_sorted)
    p95 = times_ms_sorted[int(0.95 * (len(times_ms_sorted) - 1))]
    return {"median_ms": median, "p95_ms": p95, "runs": repeats, "result_value": result_value}


def quantile_thresholds(
    con: duckdb.DuckDBPyConnection, table_name: str, col: str, ps: List[float]
) -> List[Tuple[float, Any]]:
    select_expr = ", ".join([f"quantile_cont({col}, {p}) AS q_{str(p).replace('.','_')}" for p in ps])
    row = con.execute(f"SELECT {select_expr} FROM {table_name};").fetchone()
    return [(p, row[i]) for i, p in enumerate(ps)]


def format_value_sql(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    if hasattr(v, "isoformat"):
        return "'" + v.isoformat(sep=" ") + "'"
    return str(v)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _describe_types(con: duckdb.DuckDBPyConnection, table_name: str) -> Dict[str, str]:
    desc = con.execute(f"DESCRIBE {table_name};").fetchall()
    return {c: t.upper() for c, t, *_ in desc}


def _format_filter_value(con: duckdb.DuckDBPyConnection, table_name: str, col: str, val: Any) -> str:
    if val is None:
        return "NULL"
    col_types = _describe_types(con, table_name)
    col_type = col_types.get(col, "")
    if col_type in {"VARCHAR", "TEXT"}:
        return "'" + str(val).replace("'", "''") + "'"
    return str(val)


def _vortex_numeric_expr(con: duckdb.DuckDBPyConnection, table_name: str, col: str) -> str:
    col_types = _describe_types(con, table_name)
    col_type = col_types.get(col, "")
    qcol = _quote_ident(col)
    if col_type in {"VARCHAR", "TEXT"}:
        return f"TRY_CAST({qcol} AS DOUBLE)"
    return qcol


def _select_cols(select_col: str, select_cols: Optional[str]) -> List[str]:
    cols = [select_col]
    if select_cols:
        cols.extend([c.strip() for c in select_cols.split(",") if c.strip()])
    out: List[str] = []
    seen = set()
    for c in cols:
        if c not in seen:
            out.append(c)
            seen.add(c)
    return out


def _auto_pick_cols(con: duckdb.DuckDBPyConnection, table_name: str) -> Tuple[str, str, Any, str]:
    desc = con.execute(f"DESCRIBE {table_name};").fetchall()
    numeric_types = {
        "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
        "FLOAT", "DOUBLE", "REAL", "DECIMAL",
    }
    date_types = {"DATE", "TIMESTAMP", "TIMESTAMP_TZ", "TIME"}
    text_types = {"VARCHAR", "TEXT"}

    numeric_cols = [c for c, t, *_ in desc if t.upper() in numeric_types]
    date_cols = [c for c, t, *_ in desc if t.upper() in date_types]
    text_cols = [c for c, t, *_ in desc if t.upper() in text_types]
    all_cols = [c for c, *_ in desc]

    n_total = con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]

    # Pick filter_col by cardinality and reasonable text length.
    min_ndv = 10
    max_ndv = 100_000
    max_avg_len = 128
    target_ndv = 1_000
    filter_candidates: List[Tuple[float, str]] = []
    for col in text_cols + numeric_cols + date_cols:
        qcol = _quote_ident(col)
        ndv, n = con.execute(
            f"SELECT COUNT(DISTINCT {qcol}), COUNT(*) FROM {table_name};"
        ).fetchone()
        if ndv is None or n is None:
            continue
        if ndv < min_ndv or ndv > max_ndv:
            continue
        if col in text_cols:
            avg_len = con.execute(f"SELECT AVG(LENGTH({qcol})) FROM {table_name};").fetchone()[0]
            if avg_len is not None and avg_len > max_avg_len:
                continue
        score = abs((ndv or 0) - target_ndv)
        filter_candidates.append((score, col))
    if filter_candidates:
        filter_col = min(filter_candidates, key=lambda x: x[0])[1]
    else:
        filter_col = text_cols[0] if text_cols else (numeric_cols[0] if numeric_cols else all_cols[0])

    # Pick filter_val to match target selectivity (default ~1%).
    target_sel = 0.01
    qf = _quote_ident(filter_col)
    rows = con.execute(
        f"""
        SELECT {qf} AS v, COUNT(*) AS cnt
        FROM {table_name}
        WHERE {qf} IS NOT NULL
        GROUP BY {qf}
        ORDER BY cnt DESC
        LIMIT 50;
        """
    ).fetchall()
    if rows:
        best = min(rows, key=lambda r: abs((r[1] / n_total) - target_sel))
        filter_val = best[0]
    else:
        filter_val = con.execute(
            f"SELECT {qf} FROM {table_name} WHERE {qf} IS NOT NULL LIMIT 1;"
        ).fetchone()
        filter_val = filter_val[0] if filter_val else None

    # Pick min_col with non-null values and non-constant range.
    def _choose_min_col(candidates: List[str]) -> Optional[str]:
        best_col = None
        best_nn = -1
        for col in candidates:
            qcol = _quote_ident(col)
            nn, minv, maxv = con.execute(
                f"SELECT COUNT({qcol}), MIN({qcol}), MAX({qcol}) FROM {table_name};"
            ).fetchone()
            if nn is None or minv is None or maxv is None:
                continue
            if maxv == minv:
                continue
            if nn > best_nn:
                best_nn = nn
                best_col = col
        return best_col

    min_col = _choose_min_col(numeric_cols) or _choose_min_col(date_cols) or all_cols[0]

    # Pick select_col; default to min_col, but prefer the best numeric/date column if available.
    select_col = _choose_min_col(numeric_cols) or _choose_min_col(date_cols) or min_col

    return min_col, filter_col, filter_val, select_col


def _auto_select_cols(con: duckdb.DuckDBPyConnection, table_name: str) -> List[str]:
    desc = con.execute(f"DESCRIBE {table_name};").fetchall()
    numeric_types = {
        "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
        "FLOAT", "DOUBLE", "REAL", "DECIMAL",
    }
    date_types = {"DATE", "TIMESTAMP", "TIMESTAMP_TZ", "TIME"}
    candidates = [c for c, t, *_ in desc if t.upper() in numeric_types or t.upper() in date_types]
    out = []
    for col in candidates:
        qcol = _quote_ident(col)
        nn, minv, maxv = con.execute(
            f"SELECT COUNT({qcol}), MIN({qcol}), MAX({qcol}) FROM {table_name};"
        ).fetchone()
        if nn is None or minv is None or maxv is None:
            continue
        if maxv == minv:
            continue
        out.append(col)
    return out


def _parse_casts(spec: Optional[str]) -> Dict[str, str]:
    if not spec:
        return {}
    out: Dict[str, str] = {}
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise SystemExit(f"Invalid --vortex-cast entry '{part}', expected col:TYPE")
        col, typ = part.split(":", 1)
        col = col.strip()
        typ = typ.strip()
        if not col or not typ:
            raise SystemExit(f"Invalid --vortex-cast entry '{part}', expected col:TYPE")
        out[col] = typ
    return out


def _parse_list(spec: Optional[str]) -> List[str]:
    if not spec:
        return []
    out = []
    for part in spec.split(","):
        part = part.strip()
        if part:
            out.append(part)
    return out


def _dataset_label(input_path: str) -> str:
    p = Path(input_path)
    name = p.name if p.name else "dataset"
    if p.is_file():
        name = p.stem
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in name)


def _row(
    args,
    fmt: str,
    variant: str,
    query_name: str,
    selectivity: Optional[float],
    write_meta: Dict[str, Any],
    qmeta: Dict[str, Any],
    select_col: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "format": fmt,
        "variant": variant,
        "query_name": query_name,
        "selectivity": selectivity,
        "min_col": args.min_col,
        "filter_col": args.filter_col,
        "select_col": select_col or args.select_col,
        "rows": None,
        "compression_time_s": write_meta.get("compression_time_s"),
        "compressed_size_bytes": write_meta.get("output_size_bytes"),
        "time_ms_median": qmeta.get("median_ms"),
        "time_ms_p95": qmeta.get("p95_ms"),
        "runs": qmeta.get("runs"),
        "result_value": qmeta.get("result_value"),
    }


def _markdown_summary(report: Dict[str, Any]) -> str:
    lines = []
    lines.append("# Benchmark Report")
    lines.append("")
    ds = report["dataset"]
    lines.append(f"- Input: `{ds['input']}` ({ds['input_type']})")
    lines.append(f"- Rows: **{ds['rows']}**")
    cols = report["columns"]
    lines.append(f"- min_col: `{cols['min_col']}`")
    lines.append(f"- filter_col: `{cols['filter_col']}`")
    lines.append(f"- select_cols: `{', '.join(cols.get('select_cols', []))}`")
    recs = _recommendations(report)
    if recs:
        lines.append("- recommendations:")
        if recs.get("storage_first"):
            lines.append(f"  - storage-first: `{recs['storage_first']}`")
        if recs.get("read_latency_first"):
            lines.append(f"  - read-latency-first: `{recs['read_latency_first']}`")
        if recs.get("scan_first"):
            lines.append(f"  - scan-first: `{recs['scan_first']}`")
    lines.append("")
    for name, body in report["formats"].items():
        lines.append(f"## {name}")
        if "write" in body:
            w = body["write"]
            lines.append(f"- size_bytes: **{w.get('output_size_bytes')}**")
            lines.append(f"- compression_time_s: **{w.get('compression_time_s'):.3f}**")
            if body.get("compression_ratio") is not None:
                lines.append(f"- compression_ratio: **{body.get('compression_ratio'):.3f}**")
            q = body["queries"]
            lines.append(f"- full_scan_min median_ms: **{q['full_scan_min']['median_ms']:.2f}**")
            lines.append(f"- random_access median_ms: **{q['random_access']['median_ms']:.2f}**")
            if body.get("best_select_col"):
                lines.append(
                    f"- best_select_col: `{body.get('best_select_col')}` "
                    f"(avg median_ms **{body.get('best_select_col_avg_median_ms'):.2f}**)"
                )
            if "validation" in body:
                v = body["validation"]
                checks = [
                    v.get("count_match"),
                    v.get("min_match"),
                    v.get("filtered_count_match"),
                    v.get("min_nulls_match"),
                    v.get("filter_nulls_match"),
                ]
                passed = all(c is True for c in checks)
                lines.append(f"- validation_pass: **{str(passed)}**")
            sel = q.get("selectivity_by_col", {})
            if sel:
                lines.append("- selectivity:")
                for col, items in sel.items():
                    parts = []
                    for it in items:
                        p = it.get("p")
                        med = it.get("median_ms")
                        if p is None or med is None:
                            continue
                        parts.append(f"{int(p*100)}%: {med:.2f}ms")
                    if parts:
                        lines.append(f"  - {col}: " + ", ".join(parts))
        else:
            lines.append(f"- {body.get('note','')}")
        lines.append("")
    return "\n".join(lines)


def _null_count(con: duckdb.DuckDBPyConnection, from_expr: str, col: str) -> int:
    qcol = _quote_ident(col)
    return con.execute(f"SELECT COUNT(*) FROM {from_expr} WHERE {qcol} IS NULL;").fetchone()[0]


def _recommendations(report: Dict[str, Any]) -> Dict[str, str]:
    best_storage = None
    best_storage_ratio = None
    best_read = None
    best_read_ms = None
    best_scan = None
    best_scan_ms = None

    for name, body in report.get("formats", {}).items():
        if "write" not in body or "queries" not in body:
            continue
        ratio = body.get("compression_ratio")
        if ratio is not None and (best_storage_ratio is None or ratio > best_storage_ratio):
            best_storage_ratio = ratio
            best_storage = name

        q = body["queries"]
        read_ms = q.get("random_access", {}).get("median_ms")
        if read_ms is not None and (best_read_ms is None or read_ms < best_read_ms):
            best_read_ms = read_ms
            best_read = name

        scan_ms = q.get("full_scan_min", {}).get("median_ms")
        if scan_ms is not None and (best_scan_ms is None or scan_ms < best_scan_ms):
            best_scan_ms = scan_ms
            best_scan = name

    out: Dict[str, str] = {}
    if best_storage:
        out["storage_first"] = best_storage
    if best_read:
        out["read_latency_first"] = best_read
    if best_scan:
        out["scan_first"] = best_scan
    return out
