# File-Format-Comparison-Parquet-vs-Vortex

Benchmark suite and web dashboard to compare Parquet vs Vortex (and a DuckDB in-table baseline) under the same query engine.

## Objective
Provide a repeatable, comparison of:
- Storage efficiency (size, compression ratio, write time)
- Scan/query latency (full scans, selective predicates, point lookups)
- Selectivity behavior across thresholds
- Text LIKE predicate behavior
- Validation of logical correctness vs the base table

The goal is not to declare a universal "winner", but to make trade-offs visible per dataset, codec, and query shape.

---

## High-level pipeline
1) Ingest CSV or Parquet into a DuckDB table.
2) Optionally sort the data by one or more columns.
3) Write Parquet (multiple codecs) and Vortex from that table.
4) Run the same query set against each format scan plus a DuckDB table baseline.
5) Save JSON + Markdown + CSV reports and render plots.
6) Aggregate per-dataset reports into an overall summary.

---

## What is measured

### Storage metrics
- **output_size_bytes**
- **compression_ratio** = input_size_bytes / output_size_bytes (higher is better)
- **compression_time_s**
- **compression_speed_mb_s**
- **decompression_time_s** (materialize scan into a temp table)
- **decompression_speed_mb_s**

### Query latency metrics (per format)
All timings report:
- **median_ms**
- **p95_ms**
- **runs**
- **cold_ms** (optional; one cold run before warmup)

Queries:
- **full_scan_min**: `min(min_col)` over the full table
- **selective_predicate**: `min(min_col)` with `filter_col = filter_val`
- **random_access**: `SELECT * ... WHERE random_access_col = value LIMIT 1`
- **selectivity**: `min(min_col)` with `select_col <= threshold` for multiple thresholds
- **LIKE predicates** (if enabled): prefix/suffix/contains patterns on text columns

### Data profiling
- **NDV ratio** per column and by type
- **column_type_counts**
- **input_rows**, **dropped_rows**, **drop_notes** (CSV parsing)
- **input_size_bytes**

### Validation (optional, default on)
Compares row count, min(), filtered counts, and null counts between base table and each format.

---

## Output locations
All outputs go under `out/`.

Per-dataset outputs:
- `out/report_<dataset>.json`
- `out/report_<dataset>.md`
- `out/results_<dataset>.csv`
- `out/plots/<dataset>/*.png`
- `out/parquet_<codec>_<dataset>_<timestamp>.parquet`
- `out/vortex/<table>.vortex`

Overall outputs:
- `out/overall_summary.json`
- `out/overall_summary.md`
- `out/plots/overall/*.png`

Upload artifacts:
- `out/uploads/<uploaded file>`
- `out/uploads/<dataset>_schema.sql` (optional)

Note: overall_summary is built from all `out/report_*.json` files present, not just those listed in the UI manifest.

---

## Quickstart (CLI)

Install dependencies:
```bash
python -m pip install -r bench/requirements.txt
```

Basic CSV run:
```bash
python bench/run.py \
  --input data/my.csv \
  --input-type csv \
  --auto-cols \
  --out out
```

With schema:
```bash
python bench/run.py \
  --input data/my.csv \
  --input-type csv \
  --schema data/my.table.sql \
  --table my_table \
  --auto-cols \
  --out out
```

With sorting:
```bash
python bench/run.py \
  --input data/my.csv \
  --input-type csv \
  --auto-cols \
  --sorted-by my_filter_column \
  --out out
```

---

## Website (dashboard UI)
The dashboard reads JSON outputs and provides:
- Overall summary
- Per-dataset drilldowns
- Interactive charts + diagnostics
- Upload workflow
- Custom SQL timings across formats
- 10-row preview for uploaded data

Run the site:
```bash
python -m pip install -r website/requirements.txt
python -m pip install -r bench/requirements.txt
python website/server.py
```

Open:
- `http://localhost:5000/website/index.html`
- `http://localhost:5000/website/dataset.html`
- `http://localhost:5000/website/upload.html`

The UI dataset selector reads `website/data/datasets.json`.

---

## Upload workflow
The upload page:
- Saves the dataset to `out/uploads/`
- Optionally saves a schema SQL file
- Runs the benchmark via `bench/run.py`
- Updates `website/data/datasets.json`
- Returns plots and report JSON to the UI

---

## Key parameters

### Input + ingestion
- `--input`: path to CSV or Parquet (file or dir)
- `--input-type`: `csv` or `parquet`
- `--schema`: optional SQL schema file
- `--csv-delimiter`, `--csv-header`, `--csv-nullstr`
- `--csv-ignore-errors`
- `--csv-sample-size -1` (full scan for inference)

### Query selection
- `--auto-cols`: auto-pick `min_col`, `filter_col`, `filter_val`, `select_col`
- `--select-cols`: override selectivity columns
- `--selectivities`: percentiles (default `0.01,0.1,0.25,0.5,0.9`)

### Parquet
- `--parquet-codec` or `--parquet-codecs` (default: `zstd,snappy,uncompressed`)
- `--parquet-row-group-size`

### Vortex
- `--vortex-compact` (label only; DuckDB defaults used)
- `--vortex-cast`, `--vortex-drop-cols`

### Diagnostics
- `--include-cold` / `--no-include-cold`: record cold timing (default: on)
- `--baseline-duckdb` / `--no-baseline-duckdb`: include DuckDB table baseline (default: on)
- `--sorted-by`: sort by column(s) before writing

---

## Requirements and optional dependencies
- Core: DuckDB + Matplotlib (see `bench/requirements.txt`)
- Website: Flask + Werkzeug (see `website/requirements.txt`)
- Optional:
  - PyArrow for Parquet encoding inspection
  - Python `vortex` module for Vortex encoding inspection
  - DuckDB Vortex extension (Linux/WSL often required)

If Vortex is unavailable, reports will include a `vortex_error` note.

---

## File map
- `bench/run.py`: main benchmark runner
- `bench/utils_run.py`: timing, validation, profiling helpers
- `bench/ingest/generic_ingest.py`: CSV/Parquet ingestion
- `bench/backends/parquet_backend.py`: Parquet write + metadata
- `bench/backends/vortex_backend.py`: Vortex write + scan
- `bench/report/*`: CSV/JSON/Markdown writers + plots + summary
- `website/server.py`: upload API + query API + static serving
- `website/*.js`: dashboard rendering

Legacy/standalone:
- `bench/run_goutham.py`, `bench/utils_run_goutham.py`, `bench/simple_rw.py`

---

## Repro tips
- Use the same dataset + schema across runs.
- Compare cold vs warm to assess caching.
- Use `--sorted-by` to test pruning effects.
- Keep old `report_*.json` files if you want them included in overall summaries, otherwise delete them.
