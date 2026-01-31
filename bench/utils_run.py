# bench/utils_run.py
from __future__ import annotations

import statistics
import time
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Tuple

import duckdb


def timed_query(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    repeats: int,
    warmup: int,
    include_cold: bool = False,
) -> Dict[str, Any]:
    cold_ms = None
    if include_cold:
        t0 = time.perf_counter()
        res = con.execute(sql).fetchone()
        t1 = time.perf_counter()
        cold_ms = (t1 - t0) * 1000.0
        result_value = res[0] if res else None
    else:
        result_value = None
    for _ in range(warmup):
        con.execute(sql).fetchall()

    times_ms: List[float] = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        res = con.execute(sql).fetchone()
        t1 = time.perf_counter()
        times_ms.append((t1 - t0) * 1000.0)
        result_value = res[0] if res else result_value

    times_ms_sorted = sorted(times_ms)
    median = statistics.median(times_ms_sorted)
    p95 = times_ms_sorted[int(0.95 * (len(times_ms_sorted) - 1))]
    return {
        "median_ms": median,
        "p95_ms": p95,
        "runs": repeats,
        "result_value": result_value,
        "cold_ms": cold_ms,
    }


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


def _pick_random_access(con: duckdb.DuckDBPyConnection, table_name: str) -> Tuple[Optional[str], Optional[Any]]:
    desc = con.execute(f"DESCRIBE {table_name};").fetchall()
    numeric_types = {
        "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
        "FLOAT", "DOUBLE", "REAL", "DECIMAL",
    }
    date_types = {"DATE", "TIMESTAMP", "TIMESTAMP_TZ", "TIME"}
    text_types = {"VARCHAR", "TEXT"}
    candidates = [c for c, t, *_ in desc if t.upper() in numeric_types | date_types | text_types]
    best_col = None
    best_ndv = -1
    for col in candidates:
        qcol = _quote_ident(col)
        ndv, nn = con.execute(
            f"SELECT COUNT(DISTINCT {qcol}), COUNT({qcol}) FROM {table_name};"
        ).fetchone()
        if ndv is None or nn is None or nn == 0:
            continue
        if col in text_types:
            avg_len = con.execute(f"SELECT AVG(LENGTH({qcol})) FROM {table_name};").fetchone()[0]
            if avg_len is not None and avg_len > 128:
                continue
        if ndv > best_ndv:
            best_ndv = ndv
            best_col = col
    if not best_col:
        return None, None
    qbest = _quote_ident(best_col)
    val = con.execute(f"SELECT {qbest} FROM {table_name} WHERE {qbest} IS NOT NULL LIMIT 1;").fetchone()
    val = val[0] if val else None
    return best_col, val


def _type_bucket(type_name: str) -> str:
    base = type_name.split("(", 1)[0].strip().upper()
    numeric_types = {
        "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
        "FLOAT", "DOUBLE", "REAL", "DECIMAL",
    }
    date_types = {"DATE", "TIMESTAMP", "TIMESTAMP_TZ", "TIME"}
    text_types = {"VARCHAR", "TEXT"}
    bool_types = {"BOOLEAN"}
    if base in numeric_types:
        return "numeric"
    if base in date_types:
        return "date"
    if base in text_types:
        return "text"
    if base in bool_types:
        return "bool"
    return "other"


def _column_type_counts(con: duckdb.DuckDBPyConnection, table_name: str) -> Dict[str, int]:
    col_types = _describe_types(con, table_name)
    counts: Dict[str, int] = {"numeric": 0, "date": 0, "text": 0, "bool": 0, "other": 0}
    for _, t in col_types.items():
        bucket = _type_bucket(t)
        counts[bucket] = counts.get(bucket, 0) + 1
    return counts


def _ndv_ratio_by_col(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    rowcount: int,
) -> List[Dict[str, Any]]:
    col_types = _describe_types(con, table_name)
    out: List[Dict[str, Any]] = []
    for col, t in col_types.items():
        qcol = _quote_ident(col)
        ndv = con.execute(f"SELECT COUNT(DISTINCT {qcol}) FROM {table_name};").fetchone()[0]
        ratio = (ndv / rowcount) if rowcount else None
        out.append(
            {
                "col": col,
                "type": _type_bucket(t),
                "ndv": ndv,
                "ndv_ratio": ratio,
            }
        )
    return out


def _ndv_ratio_top_cols(ndv_stats: List[Dict[str, Any]], top_n: int) -> List[Dict[str, Any]]:
    items = [s for s in ndv_stats if s.get("ndv_ratio") is not None]
    items.sort(key=lambda x: x["ndv_ratio"], reverse=True)
    return items[:top_n]


def _ndv_ratio_by_type(ndv_stats: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    buckets: Dict[str, List[float]] = {"numeric": [], "date": [], "text": [], "bool": [], "other": []}
    for s in ndv_stats:
        ratio = s.get("ndv_ratio")
        if ratio is None:
            continue
        bucket = s.get("type", "other")
        buckets.setdefault(bucket, []).append(ratio)
    out: Dict[str, Optional[float]] = {}
    for bucket, vals in buckets.items():
        out[bucket] = (sum(vals) / len(vals)) if vals else None
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
    extras: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    row = {
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
    if extras:
        row.update(extras)
    return row


def _iter_csv_files(p: Path) -> List[Path]:
    if p.is_dir():
        return list(p.rglob("*.csv"))
    return [p]


def _count_csv_rows_and_size(input_path: str, has_header: bool) -> Tuple[int, int]:
    p = Path(input_path)
    total_lines = 0
    total_size = 0
    for f in _iter_csv_files(p):
        if not f.exists():
            continue
        total_size += f.stat().st_size
        with f.open("rb") as fh:
            for _ in fh:
                total_lines += 1
        if has_header and total_lines > 0:
            total_lines -= 1
    return total_lines, total_size


def _markdown_summary(report: Dict[str, Any]) -> str:
    lines = []
    lines.append("# Benchmark Report")
    lines.append("")
    ds = report["dataset"]
    lines.append(f"- Input: `{ds['input']}` ({ds['input_type']})")
    lines.append(f"- Rows: **{_format_int(ds['rows'])}**")
    if ds.get("input_rows") is not None:
        lines.append(f"- Input rows: **{_format_int(ds['input_rows'])}**")
    if ds.get("dropped_rows") is not None:
        lines.append(f"- Dropped rows: **{_format_int(ds['dropped_rows'])}**")
        notes = ds.get("drop_notes") or []
        for note in notes:
            lines.append(f"- Drop note: {note}")
    if ds.get("input_size_bytes") is not None:
        lines.append(f"- Input size: **{_format_mb(ds['input_size_bytes'])} MB**")
    type_counts = ds.get("column_type_counts")
    if type_counts:
        parts = []
        for key in ["numeric", "text", "date", "bool", "other"]:
            val = type_counts.get(key)
            if val is not None:
                parts.append(f"{key}={_format_int(val)}")
        if parts:
            lines.append(f"- column_type_counts: {', '.join(parts)}")
    cols = report["columns"]
    lines.append(f"- min_col: `{cols['min_col']}`")
    lines.append(f"- filter_col: `{cols['filter_col']}`")
    lines.append(f"- select_cols: `{', '.join(cols.get('select_cols', []))}`")
    ndv_by_type = ds.get("ndv_ratio_by_type")
    if ndv_by_type:
        parts = []
        for key in ["numeric", "text", "date", "bool", "other"]:
            val = ndv_by_type.get(key)
            if val is not None:
                parts.append(f"{key}={val:.3f}")
        if parts:
            lines.append(f"- ndv_ratio_by_type: {', '.join(parts)}")
    ndv_top = cols.get("ndv_ratio_top_cols") or []
    if ndv_top:
        lines.append("- ndv_ratio_top_cols:")
        for item in ndv_top:
            col = item.get("col")
            ratio = item.get("ndv_ratio")
            if col is None or ratio is None:
                continue
            lines.append(f"  - {col}: {ratio:.3f}")
    recs = _recommendations(report)
    if recs:
        lines.append("- recommendations:")
        if recs.get("storage_first"):
            item = recs["storage_first"]
            lines.append(f"  - storage-first: `{item['name']}`")
            if item.get("reason"):
                lines.append(f"    - reason: {item['reason']}")
        if recs.get("compression_speed_first"):
            item = recs["compression_speed_first"]
            lines.append(f"  - compression-speed-first: `{item['name']}`")
            if item.get("reason"):
                lines.append(f"    - reason: {item['reason']}")
        if recs.get("decompression_speed_first"):
            item = recs["decompression_speed_first"]
            lines.append(f"  - decompression-speed-first: `{item['name']}`")
            if item.get("reason"):
                lines.append(f"    - reason: {item['reason']}")
        if recs.get("read_latency_first"):
            item = recs["read_latency_first"]
            lines.append(f"  - read-latency-first: `{item['name']}`")
            if item.get("reason"):
                lines.append(f"    - reason: {item['reason']}")
        if recs.get("scan_first"):
            item = recs["scan_first"]
            lines.append(f"  - scan-first: `{item['name']}`")
            if item.get("reason"):
                lines.append(f"    - reason: {item['reason']}")
    lines.append("")
    for name, body in report["formats"].items():
        lines.append(f"## {name}")
        if "write" in body:
            w = body["write"]
            if w.get("output_size_bytes") is not None:
                lines.append(f"- size_mb: **{_format_mb(w.get('output_size_bytes'))}**")
            lines.append(f"- compression_time_s: **{w.get('compression_time_s'):.3f}**")
            if w.get("compression_speed_mb_s") is not None:
                lines.append(f"- compression_speed_mb_s: **{w.get('compression_speed_mb_s'):.3f}**")
            if w.get("decompression_time_s") is not None:
                lines.append(f"- decompression_time_s: **{w.get('decompression_time_s'):.3f}**")
            if w.get("decompression_speed_mb_s") is not None:
                lines.append(f"- decompression_speed_mb_s: **{w.get('decompression_speed_mb_s'):.3f}**")
            if body.get("compression_ratio") is not None:
                lines.append(f"- compression_ratio: **{body.get('compression_ratio'):.3f}**")
            enc = body.get("encodings")
            if enc:
                per_col = enc.get("per_column")
                if per_col:
                    lines.append("- encodings:")
                    for col, encs in per_col.items():
                        if isinstance(encs, list):
                            lines.append(f"  - {col}: {', '.join(encs)}")
                        else:
                            lines.append(f"  - {col}: {encs}")
                elif enc.get("note"):
                    lines.append(f"- encodings: {enc.get('note')}")
            q = body["queries"]
            lines.append(
                f"- full_scan_min median_ms: **{q['full_scan_min']['median_ms']:.2f}** "
                f"(p95 **{q['full_scan_min']['p95_ms']:.2f}**"
                f"{_format_cold(q['full_scan_min'])})"
            )
            if "selective_predicate" in q:
                lines.append(
                    f"- selective_predicate median_ms: **{q['selective_predicate']['median_ms']:.2f}** "
                    f"(p95 **{q['selective_predicate']['p95_ms']:.2f}**"
                    f"{_format_cold(q['selective_predicate'])})"
                )
            if "random_access" in q:
                lines.append(
                    f"- random_access median_ms: **{q['random_access']['median_ms']:.2f}** "
                    f"(p95 **{q['random_access']['p95_ms']:.2f}**"
                    f"{_format_cold(q['random_access'])})"
                )
            elif "point_lookup" in q:
                lines.append(
                    f"- random_access median_ms: **{q['point_lookup']['median_ms']:.2f}** "
                    f"(p95 **{q['point_lookup']['p95_ms']:.2f}**"
                    f"{_format_cold(q['point_lookup'])})"
                )
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
            like = q.get("like_by_col", {})
            if like:
                lines.append("- like_predicates:")
                like_summary: Dict[str, List[float]] = {}
                for col, items in like.items():
                    for it in items:
                        p = it.get("target_selectivity")
                        p_list = it.get("target_selectivities")
                        actual = it.get("selectivity")
                        med = it.get("median_ms")
                        pattern = it.get("pattern")
                        ptype = it.get("pattern_type")
                        if med is None or pattern is None or ptype is None:
                            continue
                        like_summary.setdefault(ptype, []).append(med)
                        if isinstance(p_list, list) and p_list:
                            target = ",".join([f"{int(x*100)}%" for x in p_list])
                        else:
                            target = f"{int(p*100)}%" if p is not None else "n/a"
                        actual_s = f"{(actual*100):.2f}%" if actual is not None else "n/a"
                        lines.append(
                            f"  - {col} {ptype} target {target} (actual {actual_s}) `{pattern}`: {med:.2f}ms"
                        )
                if like_summary:
                    lines.append("- like_summary:")
                    for ptype in sorted(like_summary.keys()):
                        vals = like_summary[ptype]
                        if not vals:
                            continue
                        avg_ms = statistics.mean(vals)
                        lines.append(f"  - {ptype}: avg median_ms **{avg_ms:.2f}** (n={len(vals)})")
        else:
            lines.append(f"- {body.get('note','')}")
        lines.append("")
    return "\n".join(lines)


def _null_count(con: duckdb.DuckDBPyConnection, from_expr: str, col: str) -> int:
    qcol = _quote_ident(col)
    return con.execute(f"SELECT COUNT(*) FROM {from_expr} WHERE {qcol} IS NULL;").fetchone()[0]


def _recommendations(report: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    best_storage = None
    best_storage_ratio = None
    best_comp_speed = None
    best_comp_speed_val = None
    best_decomp_speed = None
    best_decomp_speed_val = None
    best_read = None
    best_read_ms = None
    best_read_metric = None
    best_scan = None
    best_scan_ms = None

    for name, body in report.get("formats", {}).items():
        if name == "duckdb_table":
            continue
        if "write" not in body or "queries" not in body:
            continue
        ratio = body.get("compression_ratio")
        if ratio is not None and (best_storage_ratio is None or ratio > best_storage_ratio):
            best_storage_ratio = ratio
            best_storage = name

        comp_speed = body.get("write", {}).get("compression_speed_mb_s")
        if comp_speed is not None and (best_comp_speed_val is None or comp_speed > best_comp_speed_val):
            best_comp_speed_val = comp_speed
            best_comp_speed = name

        decomp_speed = body.get("write", {}).get("decompression_speed_mb_s")
        if decomp_speed is not None and (best_decomp_speed_val is None or decomp_speed > best_decomp_speed_val):
            best_decomp_speed_val = decomp_speed
            best_decomp_speed = name

        q = body["queries"]
        read_ms = q.get("random_access", {}).get("median_ms")
        read_metric = "random_access"
        if read_ms is None:
            read_ms = q.get("point_lookup", {}).get("median_ms")
            read_metric = "point_lookup"
        if read_ms is None:
            read_ms = q.get("selective_predicate", {}).get("median_ms")
            read_metric = "selective_predicate"
        if read_ms is not None and (best_read_ms is None or read_ms < best_read_ms):
            best_read_ms = read_ms
            best_read = name
            best_read_metric = read_metric

        scan_ms = q.get("full_scan_min", {}).get("median_ms")
        if scan_ms is not None and (best_scan_ms is None or scan_ms < best_scan_ms):
            best_scan_ms = scan_ms
            best_scan = name

    out: Dict[str, Dict[str, str]] = {}
    if best_storage:
        out["storage_first"] = {
            "name": best_storage,
            "reason": f"highest compression_ratio {best_storage_ratio:.3f}",
        }
    if best_comp_speed:
        out["compression_speed_first"] = {
            "name": best_comp_speed,
            "reason": f"highest compression_speed_mb_s {best_comp_speed_val:.2f}",
        }
    if best_decomp_speed:
        out["decompression_speed_first"] = {
            "name": best_decomp_speed,
            "reason": f"highest decompression_speed_mb_s {best_decomp_speed_val:.2f}",
        }
    if best_read:
        out["read_latency_first"] = {
            "name": best_read,
            "reason": f"lowest {best_read_metric} median_ms {best_read_ms:.2f}",
        }
    if best_scan:
        out["scan_first"] = {
            "name": best_scan,
            "reason": f"lowest full_scan_min median_ms {best_scan_ms:.2f}",
        }
    return out


def _format_int(val: Any) -> str:
    try:
        return f"{int(val):,}"
    except Exception:
        return str(val)


def _format_mb(val: Any) -> str:
    try:
        return f"{(float(val) / (1024 * 1024)):.2f}"
    except Exception:
        return "n/a"


def _format_cold(qmeta: Dict[str, Any]) -> str:
    cold_ms = qmeta.get("cold_ms")
    if cold_ms is None:
        return ""
    return f", cold **{cold_ms:.2f}**"


_LIKE_ESCAPE_CHAR = "!"


def _escape_like_literal(value: str) -> str:
    return value.replace(_LIKE_ESCAPE_CHAR, _LIKE_ESCAPE_CHAR * 2).replace("%", _LIKE_ESCAPE_CHAR + "%").replace("_", _LIKE_ESCAPE_CHAR + "_")


def _string_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> List[str]:
    col_types = _describe_types(con, table_name)
    return [c for c, t in col_types.items() if t in {"VARCHAR", "TEXT"}]


def _like_pattern_specs_for_col(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    col: str,
    targets: List[float],
    total_rows: int,
    max_candidates: int = 50,
    pattern_len: int = 3,
) -> List[Dict[str, Any]]:
    if total_rows <= 0:
        return []
    qcol = _quote_ident(col)
    rows = con.execute(
        f"SELECT DISTINCT {qcol} FROM {table_name} WHERE {qcol} IS NOT NULL LIMIT {max_candidates};"
    ).fetchall()
    values = [r[0] for r in rows if r and r[0] is not None]
    if not values:
        return []

    patterns_by_type = {"prefix": set(), "suffix": set(), "contains": set()}
    for v in values:
        s = str(v)
        if not s:
            continue
        esc = _escape_like_literal(s)
        use_len = min(len(esc), pattern_len)
        if use_len <= 0:
            continue
        prefix = esc[:use_len]
        suffix = esc[-use_len:]
        mid_start = max((len(esc) - use_len) // 2, 0)
        mid = esc[mid_start:mid_start + use_len]
        if not prefix.endswith(_LIKE_ESCAPE_CHAR):
            patterns_by_type["prefix"].add(prefix + "%")
        if not suffix.endswith(_LIKE_ESCAPE_CHAR):
            patterns_by_type["suffix"].add("%" + suffix)
        if not mid.endswith(_LIKE_ESCAPE_CHAR):
            patterns_by_type["contains"].add("%" + mid + "%")

    specs_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for pattern_type, patterns in patterns_by_type.items():
        if not patterns:
            continue
        candidates = []
        for pattern in list(patterns)[:max_candidates]:
            pattern_sql = format_value_sql(pattern)
            cnt = con.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE {qcol} LIKE {pattern_sql} ESCAPE '!';"
            ).fetchone()[0]
            sel = (cnt / total_rows) if total_rows else 0.0
            candidates.append((pattern, sel))
        if not candidates:
            continue
        for target in targets:
            best = min(candidates, key=lambda x: abs(x[1] - target))
            key = (pattern_type, best[0])
            entry = specs_map.get(key)
            if entry is None:
                entry = {
                    "pattern_type": pattern_type,
                    "pattern": best[0],
                    "target_selectivities": [],
                }
                specs_map[key] = entry
            entry["target_selectivities"].append(target)
    return list(specs_map.values())


def _like_pattern_specs_by_col(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    targets: List[float],
    total_rows: int,
    max_candidates: int = 50,
    pattern_len: int = 3,
) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for col in _string_columns(con, table_name):
        specs = _like_pattern_specs_for_col(
            con,
            table_name,
            col,
            targets,
            total_rows,
            max_candidates=max_candidates,
            pattern_len=pattern_len,
        )
        if specs:
            out[col] = specs
    return out


def _iter_parquet_files(p: Path) -> List[Path]:
    if p.is_dir():
        return sorted(p.rglob("*.parquet"))
    return [p]


def _parquet_encodings(parquet_path: str) -> Dict[str, Any]:
    try:
        import pyarrow.parquet as pq
    except Exception as exc:
        return {"note": f"pyarrow not available: {exc}"}

    p = Path(parquet_path)
    files = _iter_parquet_files(p)
    if not files:
        return {"note": "no parquet files found for encoding inspection"}

    encodings: Dict[str, set] = {}
    errors: List[str] = []
    for f in files:
        try:
            pf = pq.ParquetFile(str(f))
        except Exception as exc:
            errors.append(f"{f.name}: {exc}")
            continue
        meta = pf.metadata
        if meta is None:
            continue
        schema = meta.schema
        for rg in range(meta.num_row_groups):
            rg_meta = meta.row_group(rg)
            for col_idx in range(rg_meta.num_columns):
                col_meta = rg_meta.column(col_idx)
                try:
                    col_name = schema.column(col_idx).path_in_schema
                except Exception:
                    try:
                        col_name = pf.schema_arrow.names[col_idx]
                    except Exception:
                        col_name = f"column_{col_idx}"
                encs = []
                for enc in col_meta.encodings:
                    encs.append(enc.name if hasattr(enc, "name") else str(enc))
                if col_name not in encodings:
                    encodings[col_name] = set()
                encodings[col_name].update(encs)

    if not encodings:
        note = "no encodings found in parquet metadata"
        if errors:
            note += f"; errors: {', '.join(errors)}"
        return {"note": note}

    out = {"per_column": {k: sorted(v) for k, v in encodings.items()}}
    if errors:
        out["note"] = f"errors reading some files: {', '.join(errors)}"
    return out


def _parse_vortex_display_tree(tree: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    skip_prefixes = ("root:", "metadata:", "buffer", "validity:", "children:", "nulls:")
    for line in tree.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(skip_prefixes):
            continue
        if not stripped.startswith("field "):
            continue
        match = re.match(r"field\\s+['\\\"]?([^'\\\"]+)['\\\"]?:\\s*(.+)$", stripped)
        if not match:
            continue
        name = match.group(1).strip()
        enc = match.group(2).strip()
        enc_id = enc.split()[0] if enc else enc
        if name:
            out[name] = enc_id
    return out


def _iter_vortex_files(p: Path) -> List[Path]:
    if p.is_dir():
        return sorted(p.rglob("*.vortex"))
    return [p]


def _vortex_encodings(vortex_path: str) -> Dict[str, Any]:
    try:
        import vortex as vx
    except Exception as exc:
        return {"note": f"vortex python module not available: {exc}"}

    p = Path(vortex_path)
    files = _iter_vortex_files(p)
    if not files:
        return {"note": "no vortex files found for encoding inspection"}

    tree = None
    error = None
    try:
        obj = vx.open(str(files[0]))
        arr = obj
        if hasattr(obj, "read") and callable(getattr(obj, "read")):
            try:
                arr = obj.read()
            except Exception:
                arr = obj
        if hasattr(arr, "display_tree"):
            tree = arr.display_tree()
        else:
            error = "vortex object has no display_tree()"
    except Exception as exc:
        error = str(exc)

    if tree:
        per_col = _parse_vortex_display_tree(tree)
        if per_col:
            out = {"per_column": per_col}
            out["note"] = "parsed from display_tree(); format may change between versions"
            return out
        return {"note": "display_tree available but no per-column fields parsed"}

    return {"note": f"unable to read vortex encodings: {error}" if error else "unable to read vortex encodings"}
