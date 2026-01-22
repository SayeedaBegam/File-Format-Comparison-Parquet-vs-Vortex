# Benchmark Report

- Input: `clouddb_project/Euro2016_1.csv` (csv)
- Rows: **2052497**
- min_col: `column00`
- filter_col: `column02`
- select_cols: `column00`

## parquet_zstd
- size_bytes: **102528930**
- compression_time_s: **1.221**
- full_scan_min median_ms: **18.49**
- random_access median_ms: **19.55**
- best_select_col: `column00` (avg median_ms **18.73**)
- selectivity:
  - column00: 1%: 18.76ms, 10%: 18.89ms, 25%: 18.75ms, 50%: 18.73ms, 90%: 18.50ms

## vortex_default
- size_bytes: **173506500**
- compression_time_s: **3.467**
- full_scan_min median_ms: **3.22**
- random_access median_ms: **5.04**
- best_select_col: `column00` (avg median_ms **3.30**)
- selectivity:
  - column00: 1%: 3.21ms, 10%: 3.70ms, 25%: 3.21ms, 50%: 3.14ms, 90%: 3.21ms
