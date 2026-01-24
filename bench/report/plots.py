from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import matplotlib.pyplot as plt


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _formats_with_write(report: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    out = []
    for name, body in report.get("formats", {}).items():
        if "write" in body and "queries" in body:
            out.append((name, body))
    return out


def _metric_or_none(body: Dict[str, Any], metric: str) -> Optional[float]:
    if metric == "random_access":
        q = body.get("queries", {})
        return q.get("random_access", {}).get("median_ms") or q.get("point_lookup", {}).get("median_ms")
    return body.get("queries", {}).get(metric, {}).get("median_ms")


def _format_number(val: Optional[float]) -> str:
    if val is None:
        return "n/a"
    try:
        if abs(val - round(val)) < 1e-6:
            return f"{int(round(val)):,}"
    except Exception:
        return str(val)
    return f"{val:,.2f}".rstrip("0").rstrip(".")


def _plot_bar(ax, labels: List[str], values: List[Optional[float]], title: str, ylabel: str) -> None:
    xs = list(range(len(labels)))
    vals = [v if v is not None else 0.0 for v in values]
    ax.bar(xs, vals, color="#4c78a8")
    ax.set_xticks(xs)
    ax.set_xticklabels(labels, rotation=15, ha="right")
    ax.set_title(title)
    ax.set_ylabel(ylabel)

    for i, v in enumerate(values):
        label = _format_number(v)
        ax.text(i, vals[i], label, ha="center", va="bottom", fontsize=8)


def _plot_grouped_bars(
    ax,
    group_labels: List[str],
    series_labels: List[str],
    series_values: List[List[Optional[float]]],
    title: str,
    ylabel: str,
    show_legend: bool = True,
    legend_outside: bool = False,
) -> None:
    n_groups = len(group_labels)
    n_series = len(series_labels)
    if n_groups == 0 or n_series == 0:
        return

    width = 0.8 / max(n_series, 1)
    xs = list(range(n_groups))
    for i, (label, values) in enumerate(zip(series_labels, series_values)):
        vals = [v if v is not None else 0.0 for v in values]
        x_offset = [x + i * width for x in xs]
        bars = ax.bar(x_offset, vals, width=width, label=label)
        for rect, v in zip(bars, values):
            label_txt = _format_number(v)
            ax.text(
                rect.get_x() + rect.get_width() / 2,
                rect.get_height(),
                label_txt,
                ha="center",
                va="bottom",
                fontsize=7,
            )

    ax.set_xticks([x + width * (n_series - 1) / 2 for x in xs])
    ax.set_xticklabels(group_labels, rotation=15, ha="right")
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    if show_legend:
        if legend_outside:
            ax.legend(fontsize=8, bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0)
        else:
            ax.legend(fontsize=8)


def _plot_stacked_bars(
    ax,
    group_labels: List[str],
    series_labels: List[str],
    series_values: List[List[Optional[float]]],
    title: str,
    ylabel: str,
    show_legend: bool = True,
    legend_outside: bool = False,
) -> None:
    n_groups = len(group_labels)
    n_series = len(series_labels)
    if n_groups == 0 or n_series == 0:
        return
    xs = list(range(n_groups))
    bottoms = [0.0] * n_groups
    for label, values in zip(series_labels, series_values):
        vals = [v if v is not None else 0.0 for v in values]
        ax.bar(xs, vals, bottom=bottoms, label=label)
        bottoms = [b + v for b, v in zip(bottoms, vals)]
    ax.set_xticks(xs)
    ax.set_xticklabels(group_labels, rotation=15, ha="right")
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    for i, total in enumerate(bottoms):
        if total > 0:
            ax.text(i, total, _format_number(total), ha="center", va="bottom", fontsize=7)
    if show_legend:
        if legend_outside:
            ax.legend(fontsize=8, bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0)
        else:
            ax.legend(fontsize=8)


def _select_cols(report: Dict[str, Any], max_cols: int) -> List[str]:
    cols = report.get("columns", {}).get("select_cols", []) or []
    return cols[:max_cols]


def _like_cols(report: Dict[str, Any], max_cols: int) -> List[str]:
    for _, body in _formats_with_write(report):
        like_by_col = body.get("queries", {}).get("like_by_col", {})
        if like_by_col:
            return list(like_by_col.keys())[:max_cols]
    return []


def _plot_selectivity_curves(report: Dict[str, Any], out_dir: Path, max_cols: int) -> None:
    cols = _select_cols(report, max_cols)
    if not cols:
        return

    formats = _formats_with_write(report)
    n = len(cols)
    ncols = 2
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(8, 3.5 * nrows), squeeze=False)
    axes_flat = [ax for row in axes for ax in row]
    for idx, col in enumerate(cols):
        ax = axes_flat[idx]
        for name, body in formats:
            sel = body.get("queries", {}).get("selectivity_by_col", {}).get(col, [])
            if not sel:
                continue
            xs = [item["p"] * 100 for item in sel if item.get("p") is not None]
            ys = [item["median_ms"] for item in sel if item.get("median_ms") is not None]
            if xs and ys:
                ax.plot(xs, ys, marker="o", label=name)
        ax.set_title(col)
        ax.set_xlabel("Selectivity (%)")
        ax.set_ylabel("Median ms")
        ax.legend(fontsize=8)

    for ax in axes_flat[n:]:
        ax.axis("off")

    fig.suptitle("Selectivity Curves (Top-10 columns)")
    fig.tight_layout()
    fig.savefig(out_dir / "selectivity_curves.png", dpi=150)
    plt.close(fig)


def _plot_like_summary(report: Dict[str, Any], out_dir: Path) -> None:
    formats = _formats_with_write(report)
    if not formats:
        return

    pattern_types = ["prefix", "suffix", "contains"]
    series_labels = [name for name, _ in formats]
    series_values: List[List[Optional[float]]] = []

    for _, body in formats:
        like_by_col = body.get("queries", {}).get("like_by_col", {})
        vals_by_type: Dict[str, List[float]] = {t: [] for t in pattern_types}
        for items in like_by_col.values():
            for it in items:
                ptype = it.get("pattern_type")
                med = it.get("median_ms")
                if ptype in vals_by_type and med is not None:
                    vals_by_type[ptype].append(med)
        series_values.append([
            (sum(vals_by_type[t]) / len(vals_by_type[t])) if vals_by_type[t] else None
            for t in pattern_types
        ])

    fig, ax = plt.subplots(figsize=(6, 4))
    _plot_grouped_bars(
        ax,
        pattern_types,
        series_labels,
        series_values,
        "LIKE Summary (avg median)",
        "Median ms",
        legend_outside=True,
    )
    fig.tight_layout()
    fig.savefig(out_dir / "like_summary.png", dpi=150)
    plt.close(fig)


def _plot_like_per_column(report: Dict[str, Any], out_dir: Path, max_cols: int) -> None:
    cols = _like_cols(report, max_cols)
    if not cols:
        return
    formats = _formats_with_write(report)
    if not formats:
        return

    pattern_types = ["prefix", "suffix", "contains"]
    n = len(cols)
    ncols = 2
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(9, 3.8 * nrows), squeeze=False)
    axes_flat = [ax for row in axes for ax in row]
    legend_handles = None
    legend_labels = None
    for idx, col in enumerate(cols):
        ax = axes_flat[idx]
        series_labels = [name for name, _ in formats]
        series_values: List[List[Optional[float]]] = []
        for _, body in formats:
            like_by_col = body.get("queries", {}).get("like_by_col", {}).get(col, [])
            vals_by_type: Dict[str, List[float]] = {t: [] for t in pattern_types}
            for it in like_by_col:
                ptype = it.get("pattern_type")
                med = it.get("median_ms")
                if ptype in vals_by_type and med is not None:
                    vals_by_type[ptype].append(med)
            series_values.append([
                (sum(vals_by_type[t]) / len(vals_by_type[t])) if vals_by_type[t] else None
                for t in pattern_types
            ])
        _plot_grouped_bars(ax, pattern_types, series_labels, series_values, col, "Median ms", show_legend=False)
        if legend_handles is None:
            legend_handles, legend_labels = ax.get_legend_handles_labels()
    for ax in axes_flat[n:]:
        ax.axis("off")
    if legend_handles and legend_labels:
        fig.legend(legend_handles, legend_labels, loc="upper center", ncol=len(legend_labels), fontsize=8)
        fig.subplots_adjust(top=0.86)
    fig.suptitle("LIKE Predicates by Column (Top-10 columns)")
    fig.tight_layout()
    fig.savefig(out_dir / "like_by_column.png", dpi=150)
    plt.close(fig)


def _plot_ndv_top_cols(report: Dict[str, Any], out_dir: Path, max_cols: int) -> None:
    items = report.get("columns", {}).get("ndv_ratio_top_cols", []) or []
    if not items:
        return
    cols = [item.get("col") for item in items][:max_cols]
    ratios = [item.get("ndv_ratio") for item in items][:max_cols]
    if not cols:
        return
    fig, ax = plt.subplots(figsize=(6, 4))
    _plot_bar(ax, cols, ratios, "NDV Ratio (Top-10 columns)", "NDV / Rows")
    fig.tight_layout()
    fig.savefig(out_dir / "ndv_ratio_top_cols.png", dpi=150)
    plt.close(fig)


def _plot_ndv_by_type(report: Dict[str, Any], out_dir: Path) -> None:
    ndv_by_type = report.get("dataset", {}).get("ndv_ratio_by_type", {})
    if not ndv_by_type:
        return
    order = ["numeric", "text", "date", "bool", "other"]
    labels = []
    values = []
    for key in order:
        if key in ndv_by_type:
            labels.append(key)
            values.append(ndv_by_type.get(key))
    fig, ax = plt.subplots(figsize=(6, 4))
    _plot_bar(ax, labels, values, "NDV Ratio by Type", "NDV / Rows")
    fig.tight_layout()
    fig.savefig(out_dir / "ndv_ratio_by_type.png", dpi=150)
    plt.close(fig)


def generate_dataset_plots(report: Dict[str, Any], out_dir: Path, max_cols: int = 5) -> None:
    _ensure_dir(out_dir)
    formats = _formats_with_write(report)
    if not formats:
        return

    names = [name for name, _ in formats]
    sizes = [body.get("write", {}).get("output_size_bytes") for _, body in formats]
    ratios = [body.get("compression_ratio") for _, body in formats]
    comp_times = [body.get("write", {}).get("compression_time_s") for _, body in formats]

    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(8, 4))
    input_size = report.get("dataset", {}).get("input_size_bytes")
    size_labels = []
    size_values: List[Optional[float]] = []
    if input_size is not None:
        size_labels.append("input")
        size_values.append(input_size)
    size_labels.extend(names)
    size_values.extend(sizes)
    size_values_mb = [(v / (1024 * 1024)) if v is not None else None for v in size_values]
    _plot_bar(axes[0], size_labels, size_values_mb, "Input vs Compressed Size", "MB")
    _plot_bar(axes[1], names, ratios, "Compression Ratio", "Ratio")
    fig.tight_layout()
    fig.savefig(out_dir / "storage.png", dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(5, 4))
    _plot_bar(ax, names, comp_times, "Compression Time", "Seconds")
    fig.tight_layout()
    fig.savefig(out_dir / "compression_time.png", dpi=150)
    plt.close(fig)

    query_metrics = [
        ("full_scan_min", "Full Scan"),
        ("selective_predicate", "Selective Predicate"),
        ("random_access", "Random Access"),
    ]
    group_labels = []
    series_values = [[] for _ in formats]
    for metric, label in query_metrics:
        values = [_metric_or_none(body, metric) for _, body in formats]
        if any(v is not None for v in values):
            group_labels.append(label)
            for idx, v in enumerate(values):
                series_values[idx].append(v)
    if group_labels:
        fig, ax = plt.subplots(figsize=(6, 4))
        _plot_grouped_bars(
            ax,
            group_labels,
            names,
            series_values,
            "Scan and Predicate Latency",
            "Median ms",
        )
        fig.tight_layout()
        fig.savefig(out_dir / "scan_predicates.png", dpi=150)
        plt.close(fig)

    _plot_selectivity_curves(report, out_dir, max_cols=max_cols)
    _plot_like_summary(report, out_dir)
    _plot_like_per_column(report, out_dir, max_cols=max_cols)
    _plot_ndv_top_cols(report, out_dir, max_cols=max_cols)
    _plot_ndv_by_type(report, out_dir)

    parquet_formats = [(name, body) for name, body in formats if name.startswith("parquet_")]
    if parquet_formats:
        pf_names = [name for name, _ in parquet_formats]
        pf_ratios = [body.get("compression_ratio") for _, body in parquet_formats]
        fig, ax = plt.subplots(figsize=(6, 4))
        _plot_bar(ax, pf_names, pf_ratios, "Parquet Codecs (Compression Ratio)", "Ratio")
        fig.tight_layout()
        fig.savefig(out_dir / "parquet_codecs_compression_ratio.png", dpi=150)
        plt.close(fig)

        group_labels = []
        series_values = [[] for _ in parquet_formats]
        for metric, label in query_metrics:
            vals = [_metric_or_none(body, metric) for _, body in parquet_formats]
            if any(v is not None for v in vals):
                group_labels.append(label)
                for idx, v in enumerate(vals):
                    series_values[idx].append(v)
        if group_labels:
            fig, ax = plt.subplots(figsize=(6, 4))
            _plot_grouped_bars(
                ax,
                group_labels,
                pf_names,
                series_values,
                "Parquet Codecs (Scan/Predicate)",
                "Median ms",
            )
            fig.tight_layout()
            fig.savefig(out_dir / "parquet_codecs_scan_predicates.png", dpi=150)
            plt.close(fig)


def _geomean(values: Iterable[float]) -> Optional[float]:
    vals = [v for v in values if v is not None and v > 0]
    if not vals:
        return None
    return math.exp(sum(math.log(v) for v in vals) / len(vals))


def _load_reports(out_dir: Path) -> List[Dict[str, Any]]:
    reports = []
    for p in sorted(out_dir.glob("report_*.json")):
        try:
            reports.append(json.loads(p.read_text(encoding="utf-8")))
        except Exception:
            continue
    return reports


def _dataset_label_from_report(report: Dict[str, Any]) -> str:
    inp = report.get("dataset", {}).get("input", "dataset")
    p = Path(str(inp))
    return p.stem if p.name else "dataset"


def _plot_dataset_overview(reports: List[Dict[str, Any]], out_dir: Path) -> None:
    labels = [_dataset_label_from_report(r) for r in reports]
    input_sizes = [r.get("dataset", {}).get("input_size_bytes") for r in reports]
    rows = [r.get("dataset", {}).get("rows") for r in reports]

    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(9, 4))
    input_sizes_mb = [(v / (1024 * 1024)) if v is not None else None for v in input_sizes]
    _plot_bar(axes[0], labels, input_sizes_mb, "Input Size by Dataset", "MB")
    _plot_bar(axes[1], labels, rows, "Rows by Dataset", "Rows")
    fig.tight_layout()
    fig.savefig(out_dir / "dataset_size_rows.png", dpi=150)
    plt.close(fig)

    type_keys = ["numeric", "text", "date", "bool", "other"]
    type_counts = []
    for key in type_keys:
        type_counts.append([r.get("dataset", {}).get("column_type_counts", {}).get(key) for r in reports])
    fig, ax = plt.subplots(figsize=(8, 4))
    _plot_stacked_bars(
        ax,
        labels,
        type_keys,
        type_counts,
        "Column Types by Dataset",
        "Columns",
        legend_outside=True,
    )
    fig.tight_layout()
    fig.savefig(out_dir / "dataset_column_types.png", dpi=150)
    plt.close(fig)

    ndv_series = []
    for key in type_keys:
        ndv_series.append([r.get("dataset", {}).get("ndv_ratio_by_type", {}).get(key) for r in reports])
    fig, ax = plt.subplots(figsize=(8, 4))
    _plot_grouped_bars(ax, labels, type_keys, ndv_series, "NDV Ratio by Type and Dataset", "NDV / Rows")
    fig.tight_layout()
    fig.savefig(out_dir / "dataset_ndv_by_type.png", dpi=150)
    plt.close(fig)


def generate_overall_plots(out_dir: Path, reports_dir: Path) -> None:
    _ensure_dir(out_dir)
    reports = _load_reports(reports_dir)
    if not reports:
        return

    _plot_dataset_overview(reports, out_dir)

    formats = sorted({name for r in reports for name, body in r.get("formats", {}).items() if "write" in body})
    if not formats:
        return

    # Compression ratio geomean.
    ratios = []
    for name in formats:
        vals = []
        for r in reports:
            body = r.get("formats", {}).get(name, {})
            ratio = body.get("compression_ratio")
            if ratio is not None:
                vals.append(ratio)
        ratios.append(_geomean(vals))

    fig, ax = plt.subplots(figsize=(6, 4))
    _plot_bar(ax, formats, ratios, "Compression Ratio (Geomean)", "Ratio")
    fig.tight_layout()
    fig.savefig(out_dir / "compression_ratio_geomean.png", dpi=150)
    plt.close(fig)

    # Query latency geomean.
    query_metrics = [
        ("full_scan_min", "Full Scan"),
        ("selective_predicate", "Selective Predicate"),
        ("random_access", "Random Access"),
    ]
    group_labels = []
    series_values = [[] for _ in formats]
    for metric, label in query_metrics:
        vals_by_format = []
        for name in formats:
            vals = []
            for r in reports:
                body = r.get("formats", {}).get(name, {})
                v = body.get("queries", {}).get(metric, {}).get("median_ms")
                if v is not None:
                    vals.append(v)
            vals_by_format.append(_geomean(vals))
        if any(v is not None for v in vals_by_format):
            group_labels.append(label)
            for idx, v in enumerate(vals_by_format):
                series_values[idx].append(v)
    if group_labels:
        fig, ax = plt.subplots(figsize=(6, 4))
        _plot_grouped_bars(ax, group_labels, formats, series_values, "Scan and Predicate (Geomean)", "Median ms")
        fig.tight_layout()
        fig.savefig(out_dir / "scan_predicates_geomean.png", dpi=150)
        plt.close(fig)

    # LIKE summary geomean by pattern type.
    pattern_types = ["prefix", "suffix", "contains"]
    group_labels = pattern_types
    series_values = []
    for name in formats:
        vals_by_type: Dict[str, List[float]] = {t: [] for t in pattern_types}
        for r in reports:
            body = r.get("formats", {}).get(name, {})
            like_by_col = body.get("queries", {}).get("like_by_col", {})
            if not like_by_col:
                continue
            tmp: Dict[str, List[float]] = {t: [] for t in pattern_types}
            for items in like_by_col.values():
                for it in items:
                    ptype = it.get("pattern_type")
                    med = it.get("median_ms")
                    if ptype in tmp and med is not None:
                        tmp[ptype].append(med)
            for ptype in pattern_types:
                if tmp[ptype]:
                    avg = sum(tmp[ptype]) / len(tmp[ptype])
                    vals_by_type[ptype].append(avg)
        series_values.append([_geomean(vals_by_type[t]) for t in pattern_types])
    if any(any(v is not None for v in series) for series in series_values):
        fig, ax = plt.subplots(figsize=(6, 4))
        _plot_grouped_bars(ax, group_labels, formats, series_values, "LIKE Summary (Geomean)", "Median ms")
        fig.tight_layout()
        fig.savefig(out_dir / "like_summary_geomean.png", dpi=150)
        plt.close(fig)

    parquet_formats = [f for f in formats if f.startswith("parquet_")]
    if parquet_formats:
        parquet_ratios = []
        for name in parquet_formats:
            vals = []
            for r in reports:
                body = r.get("formats", {}).get(name, {})
                ratio = body.get("compression_ratio")
                if ratio is not None:
                    vals.append(ratio)
            parquet_ratios.append(_geomean(vals))
        fig, ax = plt.subplots(figsize=(6, 4))
        _plot_bar(ax, parquet_formats, parquet_ratios, "Parquet Codecs (Compression Ratio)", "Ratio")
        fig.tight_layout()
        fig.savefig(out_dir / "parquet_codecs_compression_ratio_geomean.png", dpi=150)
        plt.close(fig)

        group_labels = []
        series_values = [[] for _ in parquet_formats]
        for metric, label in query_metrics:
            vals_by_format = []
            for name in parquet_formats:
                vals = []
                for r in reports:
                    body = r.get("formats", {}).get(name, {})
                    v = body.get("queries", {}).get(metric, {}).get("median_ms")
                    if v is not None:
                        vals.append(v)
                vals_by_format.append(_geomean(vals))
            if any(v is not None for v in vals_by_format):
                group_labels.append(label)
                for idx, v in enumerate(vals_by_format):
                    series_values[idx].append(v)
        if group_labels:
            fig, ax = plt.subplots(figsize=(6, 4))
            _plot_grouped_bars(
                ax,
                group_labels,
                parquet_formats,
                series_values,
                "Parquet Codecs (Scan/Predicate Geomean)",
                "Median ms",
            )
            fig.tight_layout()
            fig.savefig(out_dir / "parquet_codecs_scan_predicates_geomean.png", dpi=150)
            plt.close(fig)
