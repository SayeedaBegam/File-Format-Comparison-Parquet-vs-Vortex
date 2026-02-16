from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


def load_report_from_dir(run_dir: Path) -> Optional[dict]:
    reports = sorted(run_dir.glob("report_*.json"))
    if not reports:
        return None
    return json.loads(reports[0].read_text(encoding="utf-8"))


def parse_counts(raw: str) -> List[int]:
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    counts = []
    for part in parts:
        value = int(part.replace("_", ""))
        if value <= 0:
            raise ValueError(f"Row count must be positive: {part}")
        counts.append(value)
    return sorted(set(counts))


def extract_metric(report: dict, fmt: str, metric: str) -> Optional[float]:
    data = report.get("formats", {}).get(fmt)
    if not data:
        return None
    if metric == "compression_ratio":
        return data.get("compression_ratio")
    if metric == "output_size":
        return data.get("write", {}).get("output_size_bytes")
    if metric == "write_time":
        return data.get("write", {}).get("compression_time_s")
    if metric == "compression_speed":
        return data.get("write", {}).get("compression_speed_mb_s")
    if metric == "decompression_time":
        return data.get("write", {}).get("decompression_time_s")
    if metric == "decompression_speed":
        return data.get("write", {}).get("decompression_speed_mb_s")
    if metric == "full_scan":
        return data.get("queries", {}).get("full_scan_min", {}).get("median_ms")
    if metric == "selective":
        return data.get("queries", {}).get("selective_predicate", {}).get("median_ms")
    if metric == "random_access":
        return data.get("queries", {}).get("random_access", {}).get("median_ms")
    if metric == "cold_full_scan":
        return data.get("queries", {}).get("full_scan_min", {}).get("cold_ms")
    if metric == "cold_selective":
        return data.get("queries", {}).get("selective_predicate", {}).get("cold_ms")
    if metric == "cold_random_access":
        return data.get("queries", {}).get("random_access", {}).get("cold_ms")
    return None


def run_benchmark(
    run_py: Path,
    input_csv: Path,
    out_dir: Path,
    row_limit: Optional[int],
    csv_delimiter: Optional[str],
    csv_header: Optional[str],
    csv_nullstr: Optional[str],
    csv_sample_size: Optional[int],
    csv_ignore_errors: bool,
    csv_all_varchar: bool,
    validate_io: Optional[bool],
    auto_cols: bool,
    schema: Optional[Path],
    threads: Optional[int],
    repeats: Optional[int],
    warmup: Optional[int],
    parquet_codecs: Optional[str],
) -> Path:
    cmd = [
        sys.executable,
        str(run_py),
        "--input",
        str(input_csv),
        "--input-type",
        "csv",
        "--out",
        str(out_dir),
        "--table",
        "base_table",
    ]
    if row_limit is not None:
        cmd += ["--row-limit", str(row_limit)]
    if csv_delimiter:
        cmd += ["--csv-delimiter", csv_delimiter]
    if csv_header in {"true", "false"}:
        cmd += ["--csv-header", csv_header]
    if csv_nullstr:
        cmd += ["--csv-nullstr", csv_nullstr]
    if csv_sample_size is not None:
        cmd += ["--csv-sample-size", str(csv_sample_size)]
    if csv_ignore_errors:
        cmd += ["--csv-ignore-errors"]
    if csv_all_varchar:
        cmd += ["--csv-all-varchar"]
    if validate_io is not None:
        cmd.append("--validate-io" if validate_io else "--no-validate-io")
    if auto_cols:
        cmd += ["--auto-cols"]
    if schema:
        cmd += ["--schema", str(schema)]
    if threads is not None:
        cmd += ["--threads", str(threads)]
    if repeats is not None:
        cmd += ["--repeats", str(repeats)]
    if warmup is not None:
        cmd += ["--warmup", str(warmup)]
    if parquet_codecs:
        cmd += ["--parquet-codecs", parquet_codecs]

    subprocess.run(cmd, check=True)

    reports = sorted(out_dir.glob("report_*.json"))
    if not reports:
        raise FileNotFoundError(f"No report_*.json found in {out_dir}")
    return reports[0]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to NYC_1.csv or NYC_1.csv.bz2")
    ap.add_argument("--out-root", required=True, help="Output directory for row scaling runs")
    ap.add_argument(
        "--row-counts",
        default="1000000,2000000,4000000,6000000,8000000,10000000",
        help="Comma-separated row counts",
    )
    ap.add_argument("--csv-delimiter", default="|")
    ap.add_argument("--csv-header", default="true", choices=["true", "false"])
    ap.add_argument("--csv-nullstr", default=None)
    ap.add_argument("--csv-sample-size", type=int, default=None)
    ap.add_argument("--csv-ignore-errors", action="store_true")
    ap.add_argument("--csv-all-varchar", action="store_true")
    ap.add_argument(
        "--validate-io",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable validation (default: None - uses run.py default)",
    )
    ap.add_argument("--auto-cols", action="store_true")
    ap.add_argument("--schema", default=None)
    ap.add_argument("--threads", type=int, default=None)
    ap.add_argument("--repeats", type=int, default=None)
    ap.add_argument("--warmup", type=int, default=None)
    ap.add_argument("--parquet-codecs", default=None)
    ap.add_argument(
        "--include-duckdb",
        action="store_true",
        help="Include duckdb_table in the summary output",
    )
    ap.add_argument(
        "--rebuild-summary-only",
        action="store_true",
        help="Rebuild row_scaling_summary.json from existing rows_* reports (no benchmarking)",
    )
    args = ap.parse_args()

    input_csv = Path(args.input).expanduser().resolve()
    if not input_csv.exists():
        raise FileNotFoundError(input_csv)

    out_root = Path(args.out_root).expanduser().resolve()
    out_root.mkdir(parents=True, exist_ok=True)
    row_counts = parse_counts(args.row_counts)

    reports_by_count: Dict[int, dict] = {}
    if args.rebuild_summary_only:
        for run_dir in sorted(out_root.glob("rows_*")):
            try:
                count = int(run_dir.name.replace("rows_", ""))
            except ValueError:
                continue
            report = load_report_from_dir(run_dir)
            if report:
                reports_by_count[count] = report
        if not reports_by_count:
            raise FileNotFoundError("No rows_* reports found to rebuild summary.")
    else:
        run_py = Path(__file__).parent / "run.py"
        for count in row_counts:
            run_out = out_root / f"rows_{count}"
            run_out.mkdir(parents=True, exist_ok=True)
            report_path = run_benchmark(
                run_py=run_py,
                input_csv=input_csv,
                out_dir=run_out,
                row_limit=count,
                csv_delimiter=args.csv_delimiter,
                csv_header=args.csv_header,
                csv_nullstr=args.csv_nullstr,
                csv_sample_size=args.csv_sample_size,
                csv_ignore_errors=args.csv_ignore_errors,
                csv_all_varchar=args.csv_all_varchar,
                validate_io=args.validate_io,
                auto_cols=args.auto_cols,
                schema=Path(args.schema).resolve() if args.schema else None,
                threads=args.threads,
                repeats=args.repeats,
                warmup=args.warmup,
                parquet_codecs=args.parquet_codecs,
            )
            reports_by_count[count] = json.loads(report_path.read_text(encoding="utf-8"))

    metrics = [
        {"key": "compression_ratio", "label": "Compression ratio", "unit": "ratio"},
        {"key": "output_size", "label": "Compressed size", "unit": "bytes"},
        {"key": "write_time", "label": "Compression time", "unit": "s"},
        {"key": "compression_speed", "label": "Compression speed", "unit": "MB/s"},
        {"key": "decompression_time", "label": "Decompression time", "unit": "s"},
        {"key": "decompression_speed", "label": "Decompression speed", "unit": "MB/s"},
        {"key": "full_scan", "label": "Full scan median", "unit": "ms"},
        {"key": "selective", "label": "Selective predicate median", "unit": "ms"},
        {"key": "random_access", "label": "Random access median", "unit": "ms"},
        {"key": "cold_full_scan", "label": "Cold full scan", "unit": "ms"},
        {"key": "cold_selective", "label": "Cold selective predicate", "unit": "ms"},
        {"key": "cold_random_access", "label": "Cold random access", "unit": "ms"},
    ]

    formats: List[str] = []
    for report in reports_by_count.values():
        for fmt in report.get("formats", {}).keys():
            if fmt == "vortex_error":
                continue
            if fmt == "duckdb_table" and not args.include_duckdb:
                continue
            if fmt not in formats:
                formats.append(fmt)

    series = {metric["key"]: {fmt: [] for fmt in formats} for metric in metrics}
    for count in sorted(reports_by_count.keys()):
        report = reports_by_count[count]
        for metric in metrics:
            for fmt in formats:
                value = extract_metric(report, fmt, metric["key"])
                series[metric["key"]][fmt].append(value)

    summary = {
        "dataset": "NYC_1",
        "row_counts": sorted(reports_by_count.keys()),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "metrics": metrics,
        "formats": formats,
        "series": series,
        "reports": {str(count): f"rows_{count}/report_NYC_1.json" for count in reports_by_count.keys()},
    }
    (out_root / "row_scaling_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
