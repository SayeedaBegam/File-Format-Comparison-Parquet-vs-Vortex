# bench/run.py
from __future__ import annotations

import argparse
import platform
import statistics
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb

from ingest.generic_ingest import create_base_table_from_csv, create_base_table_from_parquet
from backends import parquet_backend
try:
    from backends import vortex_backend
    _VORTEX_AVAILABLE = True
except Exception:
    vortex_backend = None
    _VORTEX_AVAILABLE = False
from report.report import write_csv, write_json, write_markdown


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
    lines.append("")
    for name, body in report["formats"].items():
        lines.append(f"## {name}")
        if "write" in body:
            w = body["write"]
            lines.append(f"- size_bytes: **{w.get('output_size_bytes')}**")
            lines.append(f"- compression_time_s: **{w.get('compression_time_s'):.3f}**")
            q = body["queries"]
            lines.append(f"- full_scan_min median_ms: **{q['full_scan_min']['median_ms']:.2f}**")
            lines.append(f"- random_access median_ms: **{q['random_access']['median_ms']:.2f}**")
            if body.get("best_select_col"):
                lines.append(
                    f"- best_select_col: `{body.get('best_select_col')}` "
                    f"(avg median_ms **{body.get('best_select_col_avg_median_ms'):.2f}**)"
                )
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to input data (csv file/dir or parquet file/dir)")
    ap.add_argument("--input-type", required=True, choices=["csv", "parquet"], help="Input type")
    ap.add_argument("--table", default="base_table", help="Name of base table in DuckDB")
    ap.add_argument("--schema", default=None, help="Optional schema SQL path (mostly for CSV)")
    ap.add_argument("--csv-sample-size", type=int, default=None)
    ap.add_argument("--csv-all-varchar", action="store_true")
    ap.add_argument("--csv-ignore-errors", action="store_true")
    ap.add_argument("--csv-delimiter", default=None)
    ap.add_argument("--csv-header", default=None, choices=["true", "false"])
    ap.add_argument("--csv-nullstr", default=None)
    ap.add_argument("--min-col", default=None)
    ap.add_argument("--filter-col", default=None)
    ap.add_argument("--filter-val", default=None)
    ap.add_argument("--select-col", default=None)
    ap.add_argument("--select-cols", default=None)
    ap.add_argument("--auto-cols", action="store_true")
    ap.add_argument("--selectivities", default="0.01,0.1,0.25,0.5,0.9")
    ap.add_argument("--out", required=True)
    ap.add_argument("--repeats", type=int, default=7)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--parquet-codec", default="zstd")
    ap.add_argument("--parquet-row-group-size", type=int, default=128_000)
    ap.add_argument("--vortex-compact", action="store_true")
    ap.add_argument("--vortex-cast", default=None)
    ap.add_argument("--vortex-drop-cols", default=None)
    ap.add_argument("--threads", type=int, default=None)
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    if args.threads is not None:
        con.execute(f"PRAGMA threads={int(args.threads)};")

    if args.input_type == "csv":
        read_csv_options: Dict[str, Any] = {}
        if args.csv_sample_size is not None:
            read_csv_options["sample_size"] = args.csv_sample_size
        if args.csv_all_varchar:
            read_csv_options["all_varchar"] = True
        if args.csv_ignore_errors:
            read_csv_options["ignore_errors"] = True
        if args.csv_delimiter:
            read_csv_options["delim"] = args.csv_delimiter
        if args.csv_header is not None:
            read_csv_options["header"] = (args.csv_header == "true")
        if args.csv_nullstr is not None:
            read_csv_options["nullstr"] = args.csv_nullstr
        create_base_table_from_csv(
            con,
            args.table,
            args.input,
            schema_sql_path=args.schema,
            read_csv_options=read_csv_options if read_csv_options else None,
        )
    else:
        create_base_table_from_parquet(con, args.table, args.input)

    if args.auto_cols:
        args.min_col, args.filter_col, args.filter_val, args.select_col = _auto_pick_cols(con, args.table)
    elif not (args.min_col and args.filter_col and args.filter_val is not None and args.select_col):
        raise SystemExit("Provide --min-col, --filter-col, --filter-val, --select-col or use --auto-cols")

    rowcount = con.execute(f"SELECT COUNT(*) FROM {args.table};").fetchone()[0]
    ps = [float(x.strip()) for x in args.selectivities.split(",") if x.strip()]
    select_cols = _select_cols(args.select_col, args.select_cols)

    rows_csv: List[Dict[str, Any]] = []
    report: Dict[str, Any] = {
        "system": {"platform": platform.platform(), "python": platform.python_version(), "machine": platform.node()},
        "dataset": {"input": args.input, "input_type": args.input_type, "rows": rowcount},
        "columns": {"min_col": args.min_col, "filter_col": args.filter_col, "select_col": args.select_col, "select_cols": select_cols},
        "formats": {},
    }

    parquet_out = str(out_dir / "parquet")
    parquet_meta = parquet_backend.write(
        con,
        args.table,
        parquet_out,
        options={"codec": args.parquet_codec, "row_group_size": args.parquet_row_group_size},
    )
    parquet_scan = parquet_backend.scan_expr(parquet_meta.get("parquet_path", parquet_out))

    filter_val_sql = _format_filter_value(con, args.table, args.filter_col, args.filter_val)
    q_full = f"SELECT min({args.min_col}) FROM {parquet_scan};"
    q_rand = f"SELECT min({args.min_col}) FROM {parquet_scan} WHERE {args.filter_col} = {filter_val_sql};"
    m_full = timed_query(con, q_full, repeats=args.repeats, warmup=args.warmup)
    m_rand = timed_query(con, q_rand, repeats=args.repeats, warmup=args.warmup)

    sel_results_by_col: Dict[str, List[Dict[str, Any]]] = {}
    avg_selectivity_ms: Dict[str, float] = {}
    for sel_col in select_cols:
        thresholds = quantile_thresholds(con, args.table, sel_col, ps)
        sel_results = []
        for p, thr in thresholds:
            thr_sql = format_value_sql(thr)
            q_sel = f"SELECT min({args.min_col}) FROM {parquet_scan} WHERE {sel_col} <= {thr_sql};"
            m_sel = timed_query(con, q_sel, repeats=args.repeats, warmup=args.warmup)
            sel_results.append({"p": p, "threshold": thr, **m_sel})
            rows_csv.append(_row(args, "parquet", f"parquet_{args.parquet_codec}", "selectivity", p, parquet_meta, m_sel, select_col=sel_col))
        sel_results_by_col[sel_col] = sel_results
        ms_values = [r["median_ms"] for r in sel_results if r.get("median_ms") is not None]
        if ms_values:
            avg_selectivity_ms[sel_col] = sum(ms_values) / len(ms_values)

    rows_csv.append(_row(args, "parquet", f"parquet_{args.parquet_codec}", "full_scan_min", None, parquet_meta, m_full))
    rows_csv.append(_row(args, "parquet", f"parquet_{args.parquet_codec}", "random_access", None, parquet_meta, m_rand))

    best_select_col = None
    if avg_selectivity_ms:
        best_select_col = min(avg_selectivity_ms.items(), key=lambda kv: kv[1])

    report["formats"][f"parquet_{args.parquet_codec}"] = {
        "write": parquet_meta,
        "queries": {
            "full_scan_min": m_full,
            "random_access": m_rand,
            "selectivity_by_col": sel_results_by_col,
        },
        "best_select_col": best_select_col[0] if best_select_col else None,
        "best_select_col_avg_median_ms": best_select_col[1] if best_select_col else None,
    }

    if _VORTEX_AVAILABLE:
        try:
            vortex_out = str(out_dir / "vortex")
            vortex_table = args.table
            cast_map = _parse_casts(args.vortex_cast)
            drop_cols = _parse_list(args.vortex_drop_cols)
            if cast_map or drop_cols:
                desc = con.execute(f"DESCRIBE {args.table};").fetchall()
                select_parts = []
                for col, *_ in desc:
                    if col in drop_cols:
                        continue
                    qcol = _quote_ident(col)
                    if col in cast_map:
                        select_parts.append(f"CAST({qcol} AS {cast_map[col]}) AS {qcol}")
                    else:
                        select_parts.append(qcol)
                con.execute(
                    f"CREATE OR REPLACE TEMP VIEW vortex_source AS "
                    f"SELECT {', '.join(select_parts)} FROM {args.table};"
                )
                vortex_table = "vortex_source"

            vortex_meta = vortex_backend.write_vortex(
                con,
                vortex_table,
                vortex_out,
                options={"compact": args.vortex_compact},
            )

            vortex_expr = vortex_backend.scan_expr(vortex_meta.get("vortex_path", vortex_out))
            con.execute(f"CREATE OR REPLACE TEMP VIEW vortex_dataset AS SELECT * FROM {vortex_expr}")

            min_col_expr_vx = _vortex_numeric_expr(con, "vortex_dataset", args.min_col)
            sel_col_exprs_vx = {c: _vortex_numeric_expr(con, "vortex_dataset", c) for c in select_cols}
            filter_val_sql_vx = _format_filter_value(con, "vortex_dataset", args.filter_col, args.filter_val)

            q_full_vx = f"SELECT min({min_col_expr_vx}) FROM vortex_dataset;"
            q_rand_vx = f"SELECT min({min_col_expr_vx}) FROM vortex_dataset WHERE {args.filter_col} = {filter_val_sql_vx};"
            m_full_vx = timed_query(con, q_full_vx, repeats=args.repeats, warmup=args.warmup)
            m_rand_vx = timed_query(con, q_rand_vx, repeats=args.repeats, warmup=args.warmup)

            sel_results_by_col_vx: Dict[str, List[Dict[str, Any]]] = {}
            avg_selectivity_ms_vx: Dict[str, float] = {}
            for sel_col in select_cols:
                thresholds = quantile_thresholds(con, args.table, sel_col, ps)
                sel_results = []
                for p, thr in thresholds:
                    thr_sql = format_value_sql(thr)
                    q_sel = (
                        f"SELECT min({min_col_expr_vx}) FROM vortex_dataset "
                        f"WHERE {sel_col_exprs_vx[sel_col]} <= {thr_sql};"
                    )
                    m_sel = timed_query(con, q_sel, repeats=args.repeats, warmup=args.warmup)
                    sel_results.append({"p": p, "threshold": thr, **m_sel})
                    rows_csv.append(_row(args, "vortex", vortex_meta.get("variant", "vortex_default"), "selectivity", p, vortex_meta, m_sel, select_col=sel_col))
                sel_results_by_col_vx[sel_col] = sel_results
                ms_values = [r["median_ms"] for r in sel_results if r.get("median_ms") is not None]
                if ms_values:
                    avg_selectivity_ms_vx[sel_col] = sum(ms_values) / len(ms_values)

            rows_csv.append(_row(args, "vortex", vortex_meta.get("variant", "vortex_default"), "full_scan_min", None, vortex_meta, m_full_vx))
            rows_csv.append(_row(args, "vortex", vortex_meta.get("variant", "vortex_default"), "random_access", None, vortex_meta, m_rand_vx))

            best_select_col_vx = None
            if avg_selectivity_ms_vx:
                best_select_col_vx = min(avg_selectivity_ms_vx.items(), key=lambda kv: kv[1])

            report["formats"][vortex_meta.get("variant", "vortex_default")] = {
                "write": vortex_meta,
                "queries": {
                    "full_scan_min": m_full_vx,
                    "random_access": m_rand_vx,
                    "selectivity_by_col": sel_results_by_col_vx,
                },
                "best_select_col": best_select_col_vx[0] if best_select_col_vx else None,
                "best_select_col_avg_median_ms": best_select_col_vx[1] if best_select_col_vx else None,
            }
        except Exception as e:
            report["formats"]["vortex_error"] = {
                "note": f"Vortex run failed: {e}",
            }
    else:
        report["formats"]["vortex_default"] = {
            "note": "Vortex backend unavailable (missing dependencies or import error).",
        }

    dataset_label = _dataset_label(args.input)
    results_path = out_dir / f"results_{dataset_label}.csv"
    report_json_path = out_dir / f"report_{dataset_label}.json"
    report_md_path = out_dir / f"report_{dataset_label}.md"
    write_csv(rows_csv, str(results_path))
    write_json(report, str(report_json_path))
    write_markdown(_markdown_summary(report), str(report_md_path))

    print(f"Done. Wrote: {results_path}, {report_json_path}, {report_md_path}")


if __name__ == "__main__":
    main()
