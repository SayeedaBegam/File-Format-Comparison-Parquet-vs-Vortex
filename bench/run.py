# bench/run.py
from __future__ import annotations

import argparse
import platform
import time
from pathlib import Path
from typing import Any, Dict, List

import duckdb

from ingest.generic_ingest import create_base_table_from_csv, create_base_table_from_parquet
from backends import parquet_backend
try:
    from backends import vortex_backend
    _VORTEX_AVAILABLE = True
except Exception:
    vortex_backend = None
    _VORTEX_AVAILABLE = False
from report.plots import generate_dataset_plots, generate_overall_plots
from report.summary import generate_overall_summary
from report.report import write_csv, write_json, write_markdown
from utils_run import (
    _auto_pick_cols,
    _auto_select_cols,
    _column_type_counts,
    _count_csv_rows_and_size,
    _dataset_label,
    _describe_types,
    _format_filter_value,
    _like_pattern_specs_by_col,
    _markdown_summary,
    _ndv_ratio_by_col,
    _ndv_ratio_by_type,
    _ndv_ratio_top_cols,
    _null_count,
    _parse_casts,
    _parse_list,
    _parquet_encodings,
    _quote_ident,
    _pick_random_access,
    _row,
    _select_cols,
    _vortex_encodings,
    _vortex_numeric_expr,
    format_value_sql,
    quantile_thresholds,
    timed_query,
)


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
    ap.add_argument("--parquet-codec", default=None)
    ap.add_argument("--parquet-codecs", default="zstd,snappy,uncompressed")
    ap.add_argument("--parquet-row-group-size", type=int, default=128_000)
    ap.add_argument("--vortex-compact", action="store_true")
    ap.add_argument("--vortex-cast", default=None)
    ap.add_argument("--vortex-drop-cols", default=None)
    ap.add_argument(
        "--like-tests",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable/disable LIKE predicate tests (default: true)",
    )
    ap.add_argument("--like-max-candidates", type=int, default=50)
    ap.add_argument("--like-pattern-len", type=int, default=3)
    ap.add_argument(
        "--validate-io",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable/disable validation (default: true)",
    )
    ap.add_argument("--threads", type=int, default=None)
    ap.add_argument(
        "--include-cold",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include a cold-run timing per query (default: true)",
    )
    ap.add_argument("--sorted-by", default=None, help="Optional column name to sort data before writing")
    ap.add_argument(
        "--baseline-duckdb",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include DuckDB table baseline timings (default: true)",
    )
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    if args.threads is not None:
        con.execute(f"PRAGMA threads={int(args.threads)};")

    input_size_bytes = None
    input_rows = None
    drop_notes: List[str] = []
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
        has_header = args.csv_header == "true" if args.csv_header is not None else False
        input_rows, input_size_bytes = _count_csv_rows_and_size(args.input, has_header)
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
        if not args.select_cols:
            auto_sel_cols = _auto_select_cols(con, args.table)
            if auto_sel_cols:
                args.select_cols = ",".join(auto_sel_cols)
    elif not (args.min_col and args.filter_col and args.filter_val is not None and args.select_col):
        raise SystemExit("Provide --min-col, --filter-col, --filter-val, --select-col or use --auto-cols")

    rowcount = con.execute(f"SELECT COUNT(*) FROM {args.table};").fetchone()[0]
    col_type_counts = _column_type_counts(con, args.table)
    ndv_stats = _ndv_ratio_by_col(con, args.table, rowcount)
    ndv_top_cols = _ndv_ratio_top_cols(ndv_stats, 10)
    ndv_by_type = _ndv_ratio_by_type(ndv_stats)
    dropped_rows = None
    if input_rows is not None:
        dropped_rows = max(input_rows - rowcount, 0)
        if dropped_rows > 0:
            if args.csv_ignore_errors:
                drop_notes.append("rows dropped because --csv-ignore-errors skips malformed rows")
            drop_notes.append("common causes: bad quotes, type conversion failures, inconsistent delimiters")
    ps = [float(x.strip()) for x in args.selectivities.split(",") if x.strip()]
    select_cols = _select_cols(args.select_col, args.select_cols)
    like_specs_by_col = {}
    if args.like_tests:
        like_specs_by_col = _like_pattern_specs_by_col(
            con,
            args.table,
            ps,
            rowcount,
            max_candidates=args.like_max_candidates,
            pattern_len=args.like_pattern_len,
        )

    rows_csv: List[Dict[str, Any]] = []
    random_access_col, random_access_val = _pick_random_access(con, args.table)
    report: Dict[str, Any] = {
        "system": {"platform": platform.platform(), "python": platform.python_version(), "machine": platform.node()},
        "dataset": {
            "input": args.input,
            "input_type": args.input_type,
            "rows": rowcount,
            "input_rows": input_rows,
            "dropped_rows": dropped_rows,
            "drop_notes": drop_notes if drop_notes else None,
            "input_size_bytes": input_size_bytes,
            "column_type_counts": col_type_counts,
            "ndv_ratio_by_type": ndv_by_type,
            "sorted_by": args.sorted_by,
        },
        "columns": {
            "min_col": args.min_col,
            "filter_col": args.filter_col,
            "select_col": args.select_col,
            "select_cols": select_cols,
            "random_access_col": random_access_col,
            "ndv_ratio_top_cols": ndv_top_cols,
        },
        "formats": {},
    }

    if args.parquet_codec:
        parquet_codecs = [args.parquet_codec]
    else:
        parquet_codecs = [c.strip() for c in args.parquet_codecs.split(",") if c.strip()]
    seen_codecs = set()
    parquet_codecs = [c for c in parquet_codecs if not (c in seen_codecs or seen_codecs.add(c))]

    filter_val_sql = _format_filter_value(con, args.table, args.filter_col, args.filter_val)

    def _time(sql: str) -> Dict[str, Any]:
        return timed_query(
            con,
            sql,
            repeats=args.repeats,
            warmup=args.warmup,
            include_cold=args.include_cold,
        )

    source_table = args.table
    if args.sorted_by:
        sorted_table = f"{args.table}_sorted"
        con.execute(
            f"CREATE OR REPLACE TABLE {sorted_table} AS SELECT * FROM {args.table} ORDER BY {args.sorted_by};"
        )
        source_table = sorted_table

    if args.baseline_duckdb:
        duckdb_meta = {
            "format": "duckdb_table",
            "compression_time_s": 0.0,
            "output_size_bytes": input_size_bytes,
            "note": "Baseline: queries run directly on DuckDB table (no external file scan).",
        }
        duckdb_ratio = None
        if input_size_bytes:
            duckdb_ratio = input_size_bytes / input_size_bytes

        q_full_base = f"SELECT min({args.min_col}) FROM {args.table};"
        q_sel_pred_base = f"SELECT min({args.min_col}) FROM {args.table} WHERE {args.filter_col} = {filter_val_sql};"
        m_full_base = _time(q_full_base)
        m_sel_pred_base = _time(q_sel_pred_base)

        m_random_base = None
        if random_access_col and random_access_val is not None:
            q_pl_col = _quote_ident(random_access_col)
            pl_val_sql = format_value_sql(random_access_val)
            q_point = f"SELECT * FROM {args.table} WHERE {q_pl_col} = {pl_val_sql} LIMIT 1;"
            m_random_base = _time(q_point)

        sel_results_by_col_table: Dict[str, List[Dict[str, Any]]] = {}
        avg_selectivity_ms_table: Dict[str, float] = {}
        for sel_col in select_cols:
            thresholds = quantile_thresholds(con, args.table, sel_col, ps)
            sel_results = []
            for p, thr in thresholds:
                thr_sql = format_value_sql(thr)
                q_sel = f"SELECT min({args.min_col}) FROM {args.table} WHERE {sel_col} <= {thr_sql};"
                m_sel = _time(q_sel)
                sel_results.append({"p": p, "threshold": thr, **m_sel})
                rows_csv.append(
                    _row(
                        args,
                        "duckdb",
                        "duckdb_table",
                        "selectivity",
                        p,
                        duckdb_meta,
                        m_sel,
                        select_col=sel_col,
                    )
                )
            sel_results_by_col_table[sel_col] = sel_results
            ms_values = [r["median_ms"] for r in sel_results if r.get("median_ms") is not None]
            if ms_values:
                avg_selectivity_ms_table[sel_col] = sum(ms_values) / len(ms_values)

        rows_csv.append(_row(args, "duckdb", "duckdb_table", "full_scan_min", None, duckdb_meta, m_full_base))
        rows_csv.append(
            _row(args, "duckdb", "duckdb_table", "selective_predicate", None, duckdb_meta, m_sel_pred_base)
        )
        if m_random_base:
            rows_csv.append(_row(args, "duckdb", "duckdb_table", "random_access", None, duckdb_meta, m_random_base))

        like_results_by_col_table: Dict[str, List[Dict[str, Any]]] = {}
        for col, specs in like_specs_by_col.items():
            qcol = _quote_ident(col)
            for spec in specs:
                pattern_sql = format_value_sql(spec["pattern"])
                q_like = f"SELECT COUNT(*) FROM {args.table} WHERE {qcol} LIKE {pattern_sql} ESCAPE '!';"
                m_like = _time(q_like)
                match_count = m_like.get("result_value")
                sel = (match_count / rowcount) if rowcount else None
                item = {**spec, "match_count": match_count, "selectivity": sel, **m_like}
                like_results_by_col_table.setdefault(col, []).append(item)
                targets = spec.get("target_selectivities")
                targets_str = ",".join([str(t) for t in targets]) if isinstance(targets, list) else None
                rows_csv.append(
                    _row(
                        args,
                        "duckdb",
                        "duckdb_table",
                        "like_predicate",
                        sel,
                        duckdb_meta,
                        m_like,
                        select_col=col,
                        extras={
                            "pattern_type": spec["pattern_type"],
                            "pattern": spec["pattern"],
                            "target_selectivities": targets_str,
                            "match_count": match_count,
                        },
                    )
                )

        best_select_col_table = None
        if avg_selectivity_ms_table:
            best_select_col_table = min(avg_selectivity_ms_table.items(), key=lambda kv: kv[1])

        report["formats"]["duckdb_table"] = {
            "write": duckdb_meta,
            "compression_ratio": duckdb_ratio,
            "queries": {
                "full_scan_min": m_full_base,
                "selective_predicate": m_sel_pred_base,
                "random_access": m_random_base,
                "selectivity_by_col": sel_results_by_col_table,
                "like_by_col": like_results_by_col_table,
            },
            "best_select_col": best_select_col_table[0] if best_select_col_table else None,
            "best_select_col_avg_median_ms": best_select_col_table[1] if best_select_col_table else None,
        }

    if args.validate_io:
        base_count = con.execute(f"SELECT COUNT(*) FROM {args.table};").fetchone()[0]
        base_min = con.execute(f"SELECT min({args.min_col}) FROM {args.table};").fetchone()[0]
        base_nulls_min = _null_count(con, args.table, args.min_col)
        base_nulls_filter = _null_count(con, args.table, args.filter_col)
        base_filtered = con.execute(
            f"SELECT COUNT(*) FROM {args.table} WHERE {args.filter_col} = {filter_val_sql};"
        ).fetchone()[0]

    dataset_label = _dataset_label(args.input)
    run_tag = f"{dataset_label}_{int(time.time())}"

    for codec in parquet_codecs:
        parquet_out = str(out_dir / f"parquet_{codec}_{run_tag}.parquet")
        parquet_meta = parquet_backend.write(
            con,
            source_table,
            parquet_out,
            options={"codec": codec, "row_group_size": args.parquet_row_group_size},
        )
        parquet_scan = parquet_backend.scan_expr(parquet_meta.get("parquet_path", parquet_out))

        q_full = f"SELECT min({args.min_col}) FROM {parquet_scan};"
        q_sel_pred = f"SELECT min({args.min_col}) FROM {parquet_scan} WHERE {args.filter_col} = {filter_val_sql};"
        m_full = _time(q_full)
        m_sel_pred = _time(q_sel_pred)

        m_random = None
        if random_access_col and random_access_val is not None:
            q_pl_col = _quote_ident(random_access_col)
            pl_val_sql = format_value_sql(random_access_val)
            q_point = f"SELECT * FROM {parquet_scan} WHERE {q_pl_col} = {pl_val_sql} LIMIT 1;"
            m_random = _time(q_point)

        sel_results_by_col: Dict[str, List[Dict[str, Any]]] = {}
        avg_selectivity_ms: Dict[str, float] = {}
        for sel_col in select_cols:
            thresholds = quantile_thresholds(con, args.table, sel_col, ps)
            sel_results = []
            for p, thr in thresholds:
                thr_sql = format_value_sql(thr)
                q_sel = f"SELECT min({args.min_col}) FROM {parquet_scan} WHERE {sel_col} <= {thr_sql};"
                m_sel = _time(q_sel)
                sel_results.append({"p": p, "threshold": thr, **m_sel})
                rows_csv.append(_row(args, "parquet", f"parquet_{codec}", "selectivity", p, parquet_meta, m_sel, select_col=sel_col))
            sel_results_by_col[sel_col] = sel_results
            ms_values = [r["median_ms"] for r in sel_results if r.get("median_ms") is not None]
            if ms_values:
                avg_selectivity_ms[sel_col] = sum(ms_values) / len(ms_values)

        rows_csv.append(_row(args, "parquet", f"parquet_{codec}", "full_scan_min", None, parquet_meta, m_full))
        rows_csv.append(_row(args, "parquet", f"parquet_{codec}", "selective_predicate", None, parquet_meta, m_sel_pred))
        if m_random:
            rows_csv.append(_row(args, "parquet", f"parquet_{codec}", "random_access", None, parquet_meta, m_random))

        like_results_by_col: Dict[str, List[Dict[str, Any]]] = {}
        for col, specs in like_specs_by_col.items():
            qcol = _quote_ident(col)
            for spec in specs:
                pattern_sql = format_value_sql(spec["pattern"])
                q_like = f"SELECT COUNT(*) FROM {parquet_scan} WHERE {qcol} LIKE {pattern_sql} ESCAPE '!';"
                m_like = _time(q_like)
                match_count = m_like.get("result_value")
                sel = (match_count / rowcount) if rowcount else None
                item = {**spec, "match_count": match_count, "selectivity": sel, **m_like}
                like_results_by_col.setdefault(col, []).append(item)
                targets = spec.get("target_selectivities")
                targets_str = ",".join([str(t) for t in targets]) if isinstance(targets, list) else None
                rows_csv.append(
                    _row(
                        args,
                        "parquet",
                        f"parquet_{codec}",
                        "like_predicate",
                        sel,
                        parquet_meta,
                        m_like,
                        select_col=col,
                        extras={
                            "pattern_type": spec["pattern_type"],
                            "pattern": spec["pattern"],
                            "target_selectivities": targets_str,
                            "match_count": match_count,
                        },
                    )
                )

        best_select_col = None
        if avg_selectivity_ms:
            best_select_col = min(avg_selectivity_ms.items(), key=lambda kv: kv[1])

        parquet_ratio = None
        if input_size_bytes and parquet_meta.get("output_size_bytes"):
            parquet_ratio = input_size_bytes / parquet_meta.get("output_size_bytes")
        parquet_encodings = _parquet_encodings(parquet_meta.get("parquet_path", parquet_out))
        report["formats"][f"parquet_{codec}"] = {
            "write": parquet_meta,
            "compression_ratio": parquet_ratio,
            "encodings": parquet_encodings,
            "queries": {
                "full_scan_min": m_full,
                "selective_predicate": m_sel_pred,
                "random_access": m_random,
                "selectivity_by_col": sel_results_by_col,
                "like_by_col": like_results_by_col,
            },
            "best_select_col": best_select_col[0] if best_select_col else None,
            "best_select_col_avg_median_ms": best_select_col[1] if best_select_col else None,
        }
        if args.validate_io:
            pq_count = con.execute(f"SELECT COUNT(*) FROM {parquet_scan};").fetchone()[0]
            pq_min = con.execute(f"SELECT min({args.min_col}) FROM {parquet_scan};").fetchone()[0]
            pq_nulls_min = _null_count(con, parquet_scan, args.min_col)
            pq_nulls_filter = _null_count(con, parquet_scan, args.filter_col)
            pq_filtered = con.execute(
                f"SELECT COUNT(*) FROM {parquet_scan} WHERE {args.filter_col} = {filter_val_sql};"
            ).fetchone()[0]
            report["formats"][f"parquet_{codec}"]["validation"] = {
                "base_count": base_count,
                "format_count": pq_count,
                "base_min": base_min,
                "format_min": pq_min,
                "count_match": base_count == pq_count,
                "min_match": base_min == pq_min,
                "base_filtered_count": base_filtered,
                "format_filtered_count": pq_filtered,
                "filtered_count_match": base_filtered == pq_filtered,
                "base_nulls_min_col": base_nulls_min,
                "format_nulls_min_col": pq_nulls_min,
                "min_nulls_match": base_nulls_min == pq_nulls_min,
                "base_nulls_filter_col": base_nulls_filter,
                "format_nulls_filter_col": pq_nulls_filter,
                "filter_nulls_match": base_nulls_filter == pq_nulls_filter,
            }

    if _VORTEX_AVAILABLE:
        try:
            vortex_out = str(out_dir / "vortex")
            vortex_table = source_table
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
            vx_text_cols = {c for c, t in _describe_types(con, "vortex_dataset").items() if t in {"VARCHAR", "TEXT"}}

            q_full_vx = f"SELECT min({min_col_expr_vx}) FROM vortex_dataset;"
            q_sel_pred_vx = f"SELECT min({min_col_expr_vx}) FROM vortex_dataset WHERE {args.filter_col} = {filter_val_sql_vx};"
            m_full_vx = _time(q_full_vx)
            m_sel_pred_vx = _time(q_sel_pred_vx)

            m_random_vx = None
            if random_access_col and random_access_val is not None:
                q_pl_col = _quote_ident(random_access_col)
                pl_val_sql = format_value_sql(random_access_val)
                q_point_vx = f"SELECT * FROM vortex_dataset WHERE {q_pl_col} = {pl_val_sql} LIMIT 1;"
                m_random_vx = _time(q_point_vx)

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
                    m_sel = _time(q_sel)
                    sel_results.append({"p": p, "threshold": thr, **m_sel})
                    rows_csv.append(_row(args, "vortex", vortex_meta.get("variant", "vortex_default"), "selectivity", p, vortex_meta, m_sel, select_col=sel_col))
                sel_results_by_col_vx[sel_col] = sel_results
                ms_values = [r["median_ms"] for r in sel_results if r.get("median_ms") is not None]
                if ms_values:
                    avg_selectivity_ms_vx[sel_col] = sum(ms_values) / len(ms_values)

            rows_csv.append(_row(args, "vortex", vortex_meta.get("variant", "vortex_default"), "full_scan_min", None, vortex_meta, m_full_vx))
            rows_csv.append(_row(args, "vortex", vortex_meta.get("variant", "vortex_default"), "selective_predicate", None, vortex_meta, m_sel_pred_vx))
            if m_random_vx:
                rows_csv.append(_row(args, "vortex", vortex_meta.get("variant", "vortex_default"), "random_access", None, vortex_meta, m_random_vx))

            like_results_by_col_vx: Dict[str, List[Dict[str, Any]]] = {}
            for col, specs in like_specs_by_col.items():
                if col not in vx_text_cols:
                    continue
                qcol = _quote_ident(col)
                for spec in specs:
                    pattern_sql = format_value_sql(spec["pattern"])
                    q_like = f"SELECT COUNT(*) FROM vortex_dataset WHERE {qcol} LIKE {pattern_sql} ESCAPE '!';"
                    m_like = _time(q_like)
                    match_count = m_like.get("result_value")
                    sel = (match_count / rowcount) if rowcount else None
                    item = {**spec, "match_count": match_count, "selectivity": sel, **m_like}
                    like_results_by_col_vx.setdefault(col, []).append(item)
                    targets = spec.get("target_selectivities")
                    targets_str = ",".join([str(t) for t in targets]) if isinstance(targets, list) else None
                    rows_csv.append(
                        _row(
                            args,
                            "vortex",
                            vortex_meta.get("variant", "vortex_default"),
                            "like_predicate",
                            sel,
                            vortex_meta,
                            m_like,
                            select_col=col,
                            extras={
                                "pattern_type": spec["pattern_type"],
                                "pattern": spec["pattern"],
                                "target_selectivities": targets_str,
                                "match_count": match_count,
                            },
                        )
                    )

            best_select_col_vx = None
            if avg_selectivity_ms_vx:
                best_select_col_vx = min(avg_selectivity_ms_vx.items(), key=lambda kv: kv[1])

            vortex_ratio = None
            if input_size_bytes and vortex_meta.get("output_size_bytes"):
                vortex_ratio = input_size_bytes / vortex_meta.get("output_size_bytes")
            vortex_encodings = _vortex_encodings(vortex_meta.get("vortex_path", vortex_out))
            report["formats"][vortex_meta.get("variant", "vortex_default")] = {
                "write": vortex_meta,
                "compression_ratio": vortex_ratio,
                "encodings": vortex_encodings,
                "queries": {
                    "full_scan_min": m_full_vx,
                    "selective_predicate": m_sel_pred_vx,
                    "random_access": m_random_vx,
                    "selectivity_by_col": sel_results_by_col_vx,
                    "like_by_col": like_results_by_col_vx,
                },
                "best_select_col": best_select_col_vx[0] if best_select_col_vx else None,
                "best_select_col_avg_median_ms": best_select_col_vx[1] if best_select_col_vx else None,
            }
            if args.validate_io:
                base_count = con.execute(f"SELECT COUNT(*) FROM {args.table};").fetchone()[0]
                base_min = con.execute(f"SELECT min({args.min_col}) FROM {args.table};").fetchone()[0]
                base_nulls_min = _null_count(con, args.table, args.min_col)
                base_nulls_filter = _null_count(con, args.table, args.filter_col)
                base_filtered = con.execute(
                    f"SELECT COUNT(*) FROM {args.table} WHERE {args.filter_col} = {filter_val_sql};"
                ).fetchone()[0]

                vx_count = con.execute("SELECT COUNT(*) FROM vortex_dataset;").fetchone()[0]
                vx_min = con.execute(f"SELECT min({min_col_expr_vx}) FROM vortex_dataset;").fetchone()[0]
                vx_nulls_min = _null_count(con, "vortex_dataset", args.min_col)
                vx_nulls_filter = _null_count(con, "vortex_dataset", args.filter_col)
                vx_filtered = con.execute(
                    f"SELECT COUNT(*) FROM vortex_dataset WHERE {args.filter_col} = {filter_val_sql_vx};"
                ).fetchone()[0]
                report["formats"][vortex_meta.get("variant", "vortex_default")]["validation"] = {
                    "base_count": base_count,
                    "format_count": vx_count,
                    "base_min": base_min,
                    "format_min": vx_min,
                    "count_match": base_count == vx_count,
                    "min_match": base_min == vx_min,
                    "base_filtered_count": base_filtered,
                    "format_filtered_count": vx_filtered,
                    "filtered_count_match": base_filtered == vx_filtered,
                    "base_nulls_min_col": base_nulls_min,
                    "format_nulls_min_col": vx_nulls_min,
                    "min_nulls_match": base_nulls_min == vx_nulls_min,
                    "base_nulls_filter_col": base_nulls_filter,
                    "format_nulls_filter_col": vx_nulls_filter,
                    "filter_nulls_match": base_nulls_filter == vx_nulls_filter,
                }
        except Exception as e:
            report["formats"]["vortex_error"] = {
                "note": f"Vortex run failed: {e}",
            }
    else:
        report["formats"]["vortex_default"] = {
            "note": "Vortex backend unavailable (missing dependencies or import error).",
        }

    results_path = out_dir / f"results_{dataset_label}.csv"
    report_json_path = out_dir / f"report_{dataset_label}.json"
    report_md_path = out_dir / f"report_{dataset_label}.md"
    write_csv(rows_csv, str(results_path))
    write_json(report, str(report_json_path))
    write_markdown(_markdown_summary(report), str(report_md_path))
    generate_dataset_plots(report, out_dir / "plots" / dataset_label, max_cols=10)
    generate_overall_plots(out_dir / "plots" / "overall", out_dir)
    generate_overall_summary(out_dir, out_dir)

    print(f"Done. Wrote: {results_path}, {report_json_path}, {report_md_path}")


if __name__ == "__main__":
    main()
