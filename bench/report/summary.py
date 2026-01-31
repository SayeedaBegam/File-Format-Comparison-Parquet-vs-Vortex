from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def _geomean(values: Iterable[float]) -> Optional[float]:
    vals = [v for v in values if v is not None and v > 0]
    if not vals:
        return None
    return math.exp(sum(math.log(v) for v in vals) / len(vals))


def _load_reports(reports_dir: Path) -> List[Dict[str, Any]]:
    reports = []
    for p in sorted(reports_dir.glob("report_*.json")):
        try:
            reports.append(json.loads(p.read_text(encoding="utf-8")))
        except Exception:
            continue
    return reports


def _dataset_label(report: Dict[str, Any]) -> str:
    inp = report.get("dataset", {}).get("input", "dataset")
    p = Path(str(inp))
    return p.stem if p.name else "dataset"


def _format_int(val: Any) -> str:
    if val is None:
        return "n/a"
    try:
        return f"{int(val):,}"
    except Exception:
        return str(val)


def _format_mb(val: Any) -> str:
    try:
        return f"{(float(val) / (1024 * 1024)):.2f}"
    except Exception:
        return "n/a"


def _format_float(val: Any) -> str:
    try:
        if val is None:
            return "n/a"
        return f"{float(val):.3f}".rstrip("0").rstrip(".")
    except Exception:
        return "n/a"


def _like_summary_by_type(like_by_col: Dict[str, Any]) -> Dict[str, Optional[float]]:
    buckets: Dict[str, List[float]] = {"prefix": [], "suffix": [], "contains": []}
    for items in like_by_col.values():
        for it in items:
            ptype = it.get("pattern_type")
            med = it.get("median_ms")
            if ptype in buckets and med is not None:
                buckets[ptype].append(med)
    out: Dict[str, Optional[float]] = {}
    for key, vals in buckets.items():
        out[key] = (sum(vals) / len(vals)) if vals else None
    return out


def _build_summary(reports: List[Dict[str, Any]]) -> Dict[str, Any]:
    datasets = []
    for r in reports:
        ds = r.get("dataset", {})
        datasets.append(
            {
                "name": _dataset_label(r),
                "input": ds.get("input"),
                "rows": ds.get("rows"),
                "input_rows": ds.get("input_rows"),
                "dropped_rows": ds.get("dropped_rows"),
                "input_size_bytes": ds.get("input_size_bytes"),
                "column_type_counts": ds.get("column_type_counts"),
                "ndv_ratio_by_type": ds.get("ndv_ratio_by_type"),
            }
        )

    formats = sorted({name for r in reports for name, body in r.get("formats", {}).items() if "write" in body})
    format_summary: Dict[str, Any] = {}
    for name in formats:
        ratio_vals = []
        comp_vals = []
        comp_speed_vals = []
        decomp_vals = []
        decomp_speed_vals = []
        size_vals = []
        q_full_vals = []
        q_sel_vals = []
        q_rand_vals = []
        like_prefix = []
        like_suffix = []
        like_contains = []
        for r in reports:
            body = r.get("formats", {}).get(name, {})
            if not body:
                continue
            ratio = body.get("compression_ratio")
            if ratio is not None:
                ratio_vals.append(ratio)
            comp = body.get("write", {}).get("compression_time_s")
            if comp is not None:
                comp_vals.append(comp)
            comp_speed = body.get("write", {}).get("compression_speed_mb_s")
            if comp_speed is not None:
                comp_speed_vals.append(comp_speed)
            decomp = body.get("write", {}).get("decompression_time_s")
            if decomp is not None:
                decomp_vals.append(decomp)
            decomp_speed = body.get("write", {}).get("decompression_speed_mb_s")
            if decomp_speed is not None:
                decomp_speed_vals.append(decomp_speed)
            size = body.get("write", {}).get("output_size_bytes")
            if size is not None:
                size_vals.append(size)
            queries = body.get("queries", {})
            v = queries.get("full_scan_min", {}).get("median_ms")
            if v is not None:
                q_full_vals.append(v)
            v = queries.get("selective_predicate", {}).get("median_ms")
            if v is not None:
                q_sel_vals.append(v)
            v = queries.get("random_access", {}).get("median_ms")
            if v is not None:
                q_rand_vals.append(v)
            like_by_col = queries.get("like_by_col", {})
            if like_by_col:
                summary = _like_summary_by_type(like_by_col)
                if summary.get("prefix") is not None:
                    like_prefix.append(summary["prefix"])
                if summary.get("suffix") is not None:
                    like_suffix.append(summary["suffix"])
                if summary.get("contains") is not None:
                    like_contains.append(summary["contains"])

        format_summary[name] = {
            "datasets": len(ratio_vals) or len(comp_vals) or len(size_vals),
            "compression_ratio_geomean": _geomean(ratio_vals),
            "compression_time_s_geomean": _geomean(comp_vals),
            "compression_speed_mb_s_geomean": _geomean(comp_speed_vals),
            "decompression_time_s_geomean": _geomean(decomp_vals),
            "decompression_speed_mb_s_geomean": _geomean(decomp_speed_vals),
            "output_size_bytes_geomean": _geomean(size_vals),
            "query_median_ms_geomean": {
                "full_scan_min": _geomean(q_full_vals),
                "selective_predicate": _geomean(q_sel_vals),
                "random_access": _geomean(q_rand_vals),
            },
            "like_median_ms_geomean": {
                "prefix": _geomean(like_prefix),
                "suffix": _geomean(like_suffix),
                "contains": _geomean(like_contains),
            },
        }

    return {
        "dataset_count": len(datasets),
        "datasets": datasets,
        "formats": format_summary,
    }


def generate_overall_summary(reports_dir: Path, out_dir: Path) -> None:
    reports = _load_reports(reports_dir)
    if not reports:
        return

    summary = _build_summary(reports)
    out_dir.mkdir(parents=True, exist_ok=True)

    out_json = out_dir / "overall_summary.json"
    out_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    lines = []
    lines.append("# Overall Summary")
    lines.append("")
    lines.append(f"- datasets: **{_format_int(summary['dataset_count'])}**")
    lines.append("")
    lines.append("## Datasets")
    lines.append("name | rows | input_rows | dropped_rows | input_size_mb")
    lines.append("--- | --- | --- | --- | ---")
    for ds in summary["datasets"]:
        lines.append(
            f"{ds['name']} | "
            f"{_format_int(ds.get('rows'))} | "
            f"{_format_int(ds.get('input_rows'))} | "
            f"{_format_int(ds.get('dropped_rows'))} | "
            f"{_format_mb(ds.get('input_size_bytes'))}"
        )
    lines.append("")
    lines.append("## Formats (Geomean)")
    lines.append("format | comp_ratio | comp_time_s | comp_speed_mb_s | decomp_time_s | decomp_speed_mb_s | size_mb | full_scan_ms | selective_pred_ms | random_access_ms")
    lines.append("--- | --- | --- | --- | --- | --- | --- | --- | --- | ---")
    for name, body in summary["formats"].items():
        q = body.get("query_median_ms_geomean", {})
        size_mb = None
        if body.get("output_size_bytes_geomean") is not None:
            size_mb = float(body["output_size_bytes_geomean"]) / (1024 * 1024)
        lines.append(
            f"{name} | "
            f"{_format_float(body.get('compression_ratio_geomean'))} | "
            f"{_format_float(body.get('compression_time_s_geomean'))} | "
            f"{_format_float(body.get('compression_speed_mb_s_geomean'))} | "
            f"{_format_float(body.get('decompression_time_s_geomean'))} | "
            f"{_format_float(body.get('decompression_speed_mb_s_geomean'))} | "
            f"{_format_float(size_mb)} | "
            f"{_format_float(q.get('full_scan_min'))} | "
            f"{_format_float(q.get('selective_predicate'))} | "
            f"{_format_float(q.get('random_access'))}"
        )

    out_md = out_dir / "overall_summary.md"
    out_md.write_text("\n".join(lines), encoding="utf-8")
