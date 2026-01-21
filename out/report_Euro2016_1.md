# Benchmark Report

- Input: `clouddb_project/Euro2016_1.csv` (csv)
- Rows: **2052497**
- min_col: `column00`
- filter_col: `column02`
- select_cols: `column00`

## parquet_zstd
- size_bytes: **102528930**
- compression_time_s: **1.240**
- full_scan_min median_ms: **20.42**
- random_access median_ms: **20.37**
- best_select_col: `column00` (avg median_ms **19.99**)
- selectivity:
  - column00: 1%: 18.56ms, 10%: 21.98ms, 25%: 19.76ms, 50%: 19.61ms, 90%: 20.04ms

## vortex_compact
- size_bytes: **732851198**
- compression_time_s: **2.236**
- full_scan_min median_ms: **19.23**
- random_access median_ms: **0.36**
- best_select_col: `column00` (avg median_ms **23.86**)
- selectivity:
  - column00: 1%: 32.12ms, 10%: 27.58ms, 25%: 20.01ms, 50%: 19.95ms, 90%: 19.65ms
