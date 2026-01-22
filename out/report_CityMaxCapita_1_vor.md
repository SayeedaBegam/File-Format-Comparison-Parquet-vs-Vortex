# Benchmark Report

- Input: `clouddb_project/CityMaxCapita_1_vor.csv` (csv)
- Rows: **912657**
- min_col: `column03`
- filter_col: `column00`
- select_cols: `column03`

## parquet_zstd
- size_bytes: **89412492**
- compression_time_s: **1.242**
- full_scan_min median_ms: **12.09**
- random_access median_ms: **8.89**
- best_select_col: `column03` (avg median_ms **12.54**)
- selectivity:
  - column03: 1%: 12.85ms, 10%: 11.92ms, 25%: 12.22ms, 50%: 12.72ms, 90%: 12.96ms

## vortex_default
- size_bytes: **150132564**
- compression_time_s: **3.863**
- full_scan_min median_ms: **4.38**
- random_access median_ms: **5.25**
- best_select_col: `column03` (avg median_ms **4.62**)
- selectivity:
  - column03: 1%: 3.73ms, 10%: 4.45ms, 25%: 4.67ms, 50%: 5.09ms, 90%: 5.14ms
