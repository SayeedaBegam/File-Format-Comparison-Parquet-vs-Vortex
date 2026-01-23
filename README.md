# File-Format-Comparison-Parquet-vs-Vortex

Simple benchmark to compare Parquet vs Vortex using DuckDB. The code loads a dataset into DuckDB, writes Parquet and Vortex, then runs the same queries on both formats.

## Approach
We compare formats inside DuckDB so the query engine is the same for both.

Pipeline:
1) Ingest CSV or Parquet into an in-memory DuckDB table.
2) Write Parquet (DuckDB COPY, compressed).
3) Write Vortex (DuckDB Vortex extension).
4) Run the same queries on Parquet and Vortex scans.
5) Save results to CSV/JSON/Markdown.

## Metrics
For each format we record:
- write time (compression_time_s) — lower is better
- output size (size_bytes) — lower is better
- compression ratio (input_size_bytes / output_size_bytes) — higher is better
- query latencies (median and p95) — lower is better
- selectivity timings for several percentiles — lower is better
- validation checks (row count, min(), filtered count, null counts) — should match base table

Queries used:
- full_scan_min: `min(min_col)` over the full table
- random_access: `min(min_col)` with `filter_col = filter_val`
- selectivity: `min(min_col)` with `select_col <= threshold` at 1%, 10%, 25%, 50%, 90%

Column roles:
- min_col: the column used for `min(...)` queries (numeric/date with real variation)
- filter_col / filter_val: used to build the random_access filter
- select_cols: the list of columns used for selectivity curves

When `--auto-cols` is used, selectivity is run across all numeric/date columns that are not constant.

Recommendations in the report:
- storage-first: picks the format with the highest compression_ratio
- read-latency-first: picks the format with the lowest random_access median
- scan-first: picks the format with the lowest full_scan_min median

CSV row counts in the report:
- Input rows: number of rows in the CSV file (line count minus header if present).
- Dropped rows: `input_rows - loaded_rows`.
- Drop notes: short reasons (e.g., malformed rows, type conversion failures).

## Current results (from `out/`)
Reports already generated are stored under:
- `out/report_*.md` and `out/report_*.json`
- `out/results_*.csv`

Observed patterns in those reports:
- Parquet output is usually smaller.
- Vortex often gives faster random-access timings.
- Exact numbers depend on the dataset and parsing options.

Check the latest report files for the full numbers:
- `out/report_CityMaxCapita_1.md`
- `out/report_Euro2016_1.md`
- `out/report_IGlocations1_1.md`
- `out/report_MedPayment2_1.md`

## Files and responsibilities
- `bench/run.py`: main runner and CLI
- `bench/utils_run.py`: helper functions (auto-pick logic, SQL formatting, timing, validation, markdown)
- `bench/ingest/generic_ingest.py`: CSV/Parquet ingestion
- `bench/backends/parquet_backend.py`: Parquet write + scan expression
- `bench/backends/vortex_backend.py`: Vortex write + scan expression (DuckDB extension)
- `bench/report/report.py`: CSV/JSON/Markdown writers

## Install
From this folder:
```bash
pip install -r bench/requirements.txt
```

## How to run

### 1) CSV with auto-pick (safe parsing)
Use when CSV is pipe-delimited and has no header (example: CityMaxCapita_1_vor.csv).
```bash
python3 bench/run.py \
  --input ../CityMaxCapita_1_vor.csv \
  --input-type csv \
  --auto-cols \
  --csv-delimiter '|' \
  --csv-header false \
  --csv-nullstr null \
  --csv-sample-size -1 \
  --out out
```

### 2) CSV with schema (strict types)
Use when you have a schema file and want fixed types.
```bash
python3 bench/run.py \
  --input ../Euro2016_1.csv \
  --input-type csv \
  --schema ../Euro2016_1.table.sql \
  --table Euro2016_1 \
  --csv-delimiter '|' \
  --csv-header false \
  --csv-nullstr null \
  --csv-ignore-errors \
  --auto-cols \
  --out out
```

### 3) Vortex options and validation (enabled by default)
```bash
python3 bench/run.py \
  --input ../Euro2016_1.csv \
  --input-type csv \
  --schema ../Euro2016_1.table.sql \
  --table Euro2016_1 \
  --csv-delimiter '|' \
  --csv-header false \
  --csv-nullstr null \
  --csv-ignore-errors \
  --auto-cols \
  --vortex-compact \
  --vortex-cast column01:BIGINT,column10:VARCHAR \
  --vortex-drop-cols tweet \
  --validate-io \
  --out out
```

## Key parameters
Input and ingestion:
- `--input`: CSV or Parquet path (file or dir)
- `--input-type`: `csv` or `parquet`
- `--schema`: SQL file to create the table before loading CSV
- `--csv-delimiter`, `--csv-header`, `--csv-nullstr`
- `--csv-sample-size -1`: full scan for better type inference
- `--csv-ignore-errors`: skip malformed rows

Query selection:
- `--auto-cols`: auto-pick `min_col`, `filter_col`, `filter_val`, `select_col`
- `--select-cols`: override which columns are used for selectivity (default: all numeric/date, non-constant)
- `--selectivities`: percentiles, e.g. `0.01,0.1,0.25,0.5,0.9`

Parquet:
- `--parquet-codec` (default `zstd`)
- `--parquet-row-group-size`

Vortex:
- `--vortex-compact`: label only (DuckDB extension does not expose extra knobs)
- `--vortex-cast`: cast columns before Vortex write, format `col:TYPE`
- `--vortex-drop-cols`: drop columns before Vortex write

Validation:
- Validation is on by default: compares row counts, min(), filtered counts, and null counts between base table and formats.
- Explicitly enable with `--validate-io`, or disable with `--no-validate-io`.

## Notes
- If your CSV has no header, do not use `--csv-header true`. It will treat the first data row as column names.
- If schema loading fails due to bad quotes, use `--csv-ignore-errors`.
- Vortex benchmarks run through DuckDB’s Vortex extension (`read_vortex`), so timings include file I/O and decoding inside DuckDB.
