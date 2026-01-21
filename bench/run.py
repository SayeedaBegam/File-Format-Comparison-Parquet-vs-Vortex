# bench/run.py
from __future__ import annotations

import argparse
import os
import platform
import statistics
import time
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import duckdb

from ingest.generic_ingest import create_base_table_from_csv, create_base_table_from_parquet
from backends import parquet_backend  # vortex_backend to be added by teammate
from report.report import write_csv, write_json, write_markdown


def timed_query(con: duckdb.DuckDBPyConnection, sql: str, repeats: int = 7, warmup: int = 1) -> Dict[str, Any]:
    # warmup
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


def quantile_thresholds(con: duckdb.DuckDBPyConnection, table_name: str, col: str, ps: List[float]) -> List[Tuple[float, Any]]:
    # quantile_cont works for numeric types; for dates, still okay in many cases
    select_expr = ", ".join([f"quantile_cont({col}, {p}) AS q_{str(p).replace('.','_')}" for p in ps])
    row = con.execute(f"SELECT {select_expr} FROM {table_name};").fetchone()
    out = []
    for i, p in enumerate(ps):
        out.append((p, row[i]))
    return out


def format_value_sql(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to input data (csv file/dir or parquet file/dir)")
    ap.add_argument("--input-type", required=True, choices=["csv", "parquet"], help="Input type")
    ap.add_argument("--table", default="base_table", help="Name of base table in DuckDB")
    ap.add_argument("--schema", default=None, help="Optional schema SQL path (mostly for CSV)")
    # CSV ingestion knobs
    ap.add_argument("--csv-sample-size", type=int, default=None, help="DuckDB read_csv_auto sample_size (e.g., -1 for full scan)")
    ap.add_argument("--csv-all-varchar", action="store_true", help="Force all CSV columns to VARCHAR")
    ap.add_argument("--csv-ignore-errors", action="store_true", help="Skip bad CSV rows during parsing")

    ap.add_argument("--min-col", default=None)
    ap.add_argument("--filter-col", default=None)
    ap.add_argument("--filter-val", default=None, help="String or number; passed as SQL literal safely for strings")
    ap.add_argument("--select-col", default=None)
    ap.add_argument("--select-cols", default=None, help="Optional comma-separated list of additional selectivity columns")
    ap.add_argument("--auto-cols", action="store_true", help="Auto-pick min/filter/select columns and filter value")
    ap.add_argument("--selectivities", default="0.01,0.1,0.25,0.5,0.9")

    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--repeats", type=int, default=7)
    ap.add_argument("--warmup", type=int, default=1)

    # Parquet knobs
    ap.add_argument("--parquet-codec", default="zstd")
    ap.add_argument("--parquet-row-group-size", type=int, default=128_000)

    # DuckDB knobs
    ap.add_argument("--threads", type=int, default=None)

    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    if args.threads is not None:
        con.execute(f"PRAGMA threads={int(args.threads)};")

    # Ingest
    if args.input_type == "csv":
        read_csv_options: Dict[str, Any] = {}
        if args.csv_sample_size is not None:
            read_csv_options["sample_size"] = args.csv_sample_size
        if args.csv_all_varchar:
            read_csv_options["all_varchar"] = True
        if args.csv_ignore_errors:
            read_csv_options["ignore_errors"] = True
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

    # Basic sanity
    rowcount = con.execute(f"SELECT COUNT(*) FROM {args.table};").fetchone()[0]

    # Shared selectivities
    ps = [float(x.strip()) for x in args.selectivities.split(",") if x.strip()]
    select_cols = _select_cols(args.select_col, args.select_cols)

    # Backends: Parquet now; Vortex later
    rows_csv: List[Dict[str, Any]] = []
    report: Dict[str, Any] = {
        "system": {"platform": platform.platform(), "python": platform.python_version(), "machine": platform.node()},
        "dataset": {"input": args.input, "input_type": args.input_type, "rows": rowcount},
        "columns": {"min_col": args.min_col, "filter_col": args.filter_col, "select_col": args.select_col, "select_cols": select_cols},
        "formats": {},
    }

    # ---------- Parquet pipeline ----------
    parquet_out = str(out_dir / "parquet")
    parquet_meta = parquet_backend.write(
        con,
        args.table,
        parquet_out,  # backend will create file inside if directory
        options={"codec": args.parquet_codec, "row_group_size": args.parquet_row_group_size},
    )
    parquet_scan = parquet_backend.scan_expr(parquet_meta.get("parquet_path", parquet_out))

    # Queries
    filter_val_sql = args.filter_val
    # treat non-numeric as string literal
    if not _looks_numeric(args.filter_val):
        filter_val_sql = "'" + args.filter_val.replace("'", "''") + "'"

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

    # ---------- Vortex placeholder ----------
    report["formats"]["vortex_default"] = {
        "note": "Not run yet: vortex_backend.py not implemented.",
    }

    # Write outputs (include dataset name to avoid overwrites)
    dataset_label = _dataset_label(args.input)
    results_path = out_dir / f"results_{dataset_label}.csv"
    report_json_path = out_dir / f"report_{dataset_label}.json"
    report_md_path = out_dir / f"report_{dataset_label}.md"
    write_csv(rows_csv, str(results_path))
    write_json(report, str(report_json_path))
    write_markdown(_markdown_summary(report), str(report_md_path))

    print(f"Done. Wrote: {results_path}, {report_json_path}, {report_md_path}")


def _looks_numeric(s: str) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        return False


def _dataset_label(input_path: str) -> str:
    p = Path(input_path)
    name = p.name if p.name else "dataset"
    if p.is_file():
        name = p.stem
    # keep label simple for filenames
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in name)


def _row(args, fmt: str, variant: str, query_name: str, selectivity: Optional[float],
         write_meta: Dict[str, Any], qmeta: Dict[str, Any], select_col: Optional[str] = None) -> Dict[str, Any]:
    return {
        "format": fmt,
        "variant": variant,
        "query_name": query_name,
        "selectivity": selectivity,
        "min_col": args.min_col,
        "filter_col": args.filter_col,
        "select_col": select_col or args.select_col,
        "rows": None,  # keep if you want, or pass rowcount here
        "compression_time_s": write_meta.get("compression_time_s"),
        "compressed_size_bytes": write_meta.get("output_size_bytes"),
        "time_ms_median": qmeta.get("median_ms"),
        "time_ms_p95": qmeta.get("p95_ms"),
        "runs": qmeta.get("runs"),
        "result_value": qmeta.get("result_value"),
    }


def _markdown_summary(report: Dict[str, Any]) -> str:
    # small human-readable summary
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
                lines.append(f"- best_select_col: `{body.get('best_select_col')}` (avg median_ms **{body.get('best_select_col_avg_median_ms'):.2f}**)")
        else:
            lines.append(f"- {body.get('note','')}")
        lines.append("")
    return "\n".join(lines)


def _select_cols(select_col: str, select_cols: Optional[str]) -> List[str]:
    cols = [select_col]
    if select_cols:
        cols.extend([c.strip() for c in select_cols.split(",") if c.strip()])
    # de-dupe while preserving order
    out = []
    seen = set()
    for c in cols:
        if c not in seen:
            out.append(c)
            seen.add(c)
    return out


def _auto_pick_cols(con: duckdb.DuckDBPyConnection, table_name: str) -> Tuple[str, str, Any, str]:
    desc = con.execute(f"DESCRIBE {table_name};").fetchall()
    # DESCRIBE output: (column_name, column_type, ...)
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

    min_col = numeric_cols[0] if numeric_cols else (date_cols[0] if date_cols else all_cols[0])
    select_col = numeric_cols[0] if numeric_cols else (date_cols[0] if date_cols else all_cols[0])
    filter_col = text_cols[0] if text_cols else (numeric_cols[0] if numeric_cols else all_cols[0])
    filter_val = con.execute(
        f"SELECT {filter_col} FROM {table_name} WHERE {filter_col} IS NOT NULL LIMIT 1;"
    ).fetchone()
    filter_val = filter_val[0] if filter_val else None
    return min_col, filter_col, filter_val, select_col


if __name__ == "__main__":
    main()
