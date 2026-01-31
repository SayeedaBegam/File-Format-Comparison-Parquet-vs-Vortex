# File-Format-Comparison-Parquet-vs-Vortex

Benchmark suite and web dashboard to compare Parquet vs Vortex (and a DuckDB in-table baseline) under the same query engine. The pipeline ingests a dataset into DuckDB, writes each format, and runs a consistent set of queries to measure storage size, latency, and selectivity behavior.

This README documents:
- What is measured and why
- Where results are stored
- How to run the benchmark and website
- What new diagnostics mean (cold vs warm, row groups, sorting)

---

## High-level approach
1) Ingest CSV or Parquet into a DuckDB table.
2) Optionally sort the data by a chosen column.
3) Write Parquet and (if available) Vortex from that table.
4) Run the same queries against each format scan.
5) Save results to CSV, JSON, Markdown and render plots.

---

## Metrics captured

### Storage metrics
- **output_size_bytes**: size of the generated file(s) for each format
- **compression_ratio**: input_size_bytes / output_size_bytes (higher is better)
- **compression_time_s**: time to write each format

### Query latency metrics (per format)
All query latencies include:
- **median_ms**
- **p95_ms**
- **runs**
- **cold_ms** (optional; one cold run before warmup)

Queries:
- **full_scan_min**: `min(min_col)` over the full table
- **selective_predicate**: `min(min_col)` with `filter_col = filter_val`
- **random_access**: `SELECT * ... WHERE random_access_col = value LIMIT 1`
- **selectivity**: `min(min_col)` with `select_col <= threshold` at 1%, 10%, 25%, 50%, 90%
- **LIKE predicates** (if enabled): prefix/suffix/contains patterns on text columns

### Data profiling
- **NDV ratio** per column and by type (NDV / rows)
- **column_type_counts** (numeric/text/date/bool/other)
- **dropped_rows** + drop notes when CSV parsing skips malformed rows

### Validation (optional, default on)
Compares row count, min(), filtered counts, and null counts between base table and each format.

### Diagnostics (new)
These are critical to explain surprising results:
- **cold_ms**: cold run timing (before warmup)
- **duckdb_table** baseline: same queries directly on the DuckDB table
- **row_group_count** (Parquet): a proxy for block-level reads
- **sorted_by** (dataset metadata): indicates if data was sorted before writing

---

## Why these diagnostics matter
They help answer questions like:
- *Why is Vortex faster than uncompressed Parquet?*  
  Compare Vortex vs Parquet vs DuckDB baseline + cold/warm runs.
- *Are results influenced by caching?*  
  Compare cold vs warm timings.
- *Does sorting improve pruning?*  
  Sort by a filter/select column and compare row_group_count + latency deltas.
- *Why does performance change with selectivity?*  
  Check selectivity curves by column and format.

---

## Output locations
All outputs go under `out/`:
- **Per-dataset results**:
  - `out/report_<dataset>.json`
  - `out/report_<dataset>.md`
  - `out/results_<dataset>.csv`
  - `out/plots/<dataset>/*.png`
- **Overall results**:
  - `out/overall_summary.json`
  - `out/overall_summary.md`
  - `out/plots/overall/*.png`

The web UI reads from these outputs.

---

## Website (dashboard UI)
The website is a front-end for `out/` reports.
It includes:
- Overall summary
- Per-dataset drilldowns
- Interactive charts
- Diagnostics (cold vs warm, baseline, row groups)
- Upload workflow (CSV + optional schema)
- Custom SQL timing on uploaded data
- First 10 rows preview

### Run the site (full features)
```bash
python -m pip install -r website/requirements.txt
python -m pip install -r bench/requirements.txt
python website/server.py
```
Open:
- `http://localhost:5000/website/index.html`
- `http://localhost:5000/website/dataset.html`
- `http://localhost:5000/website/upload.html`

### Upload workflow
On the Upload page you can:
- Upload CSV
- Provide optional schema (SQL)
- Provide optional sort column
- Run a custom SELECT query with timings

Uploaded datasets are written to `out/` and appended to:
- `website/data/datasets.json`

The UI reloads automatically using cache-busting and localStorage fallback.

---

## CLI usage (bench/run.py)

### Basic CSV run
```bash
python bench/run.py \
  --input data/my.csv \
  --input-type csv \
  --auto-cols \
  --out out
```

### With schema
```bash
python bench/run.py \
  --input data/my.csv \
  --input-type csv \
  --schema data/my.table.sql \
  --table my_table \
  --auto-cols \
  --out out
```

### With sorting + cold timings + baseline
```bash
python bench/run.py \
  --input data/my.csv \
  --input-type csv \
  --auto-cols \
  --sorted-by my_filter_column \
  --include-cold \
  --baseline-duckdb \
  --out out
```

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
- `--vortex-compact` (label only)
- `--vortex-cast`, `--vortex-drop-cols`

### Diagnostics
- `--include-cold`: record cold run time
- `--baseline-duckdb`: include DuckDB table baseline
- `--sorted-by`: sort by a column before writing formats

---

## Notes on Vortex
The Vortex extension may not be available on Windows builds of DuckDB.
If you see `vortex_error` in the report:
- Run on Linux/WSL where `INSTALL vortex; LOAD vortex;` works.

---

## Files and responsibilities
- `bench/run.py`: benchmark runner + CLI
- `bench/utils_run.py`: timing, validation, profiling helpers
- `bench/ingest/generic_ingest.py`: CSV/Parquet ingestion
- `bench/backends/parquet_backend.py`: Parquet write + metadata
- `bench/backends/vortex_backend.py`: Vortex write + scan
- `bench/report/*`: CSV/JSON/Markdown writers + plots
- `website/`: dashboard UI + upload API (`server.py`)

---

## Reproducibility tips
- Use the same dataset and schema across runs.
- Compare cold vs warm to assess caching.
- Use `--sorted-by` to test block pruning effects.
- Always include baseline to separate storage vs engine effects.
