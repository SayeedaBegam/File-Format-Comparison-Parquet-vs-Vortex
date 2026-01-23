# Benchmark Report

- Input: `clouddb_project/Euro2016_1.csv` (csv)
- Rows: **2052497**
- min_col: `column01`
- filter_col: `column02`
- select_cols: `column01, column03, column04, column06, column08, column10`

## parquet_zstd
- size_bytes: **102528930**
- compression_time_s: **1.216**
- full_scan_min median_ms: **20.43**
- random_access median_ms: **21.31**
- best_select_col: `column10` (avg median_ms **15.21**)
- validation_pass: **True**
- selectivity:
  - column01: 1%: 26.66ms, 10%: 26.54ms, 25%: 26.09ms, 50%: 24.14ms, 90%: 22.79ms
  - column03: 1%: 19.62ms, 10%: 19.33ms, 25%: 19.07ms, 50%: 20.80ms, 90%: 20.30ms
  - column04: 1%: 22.21ms, 10%: 20.52ms, 25%: 20.54ms, 50%: 20.70ms, 90%: 20.42ms
  - column06: 1%: 24.17ms, 10%: 22.28ms, 25%: 25.62ms, 50%: 23.74ms, 90%: 25.14ms
  - column08: 1%: 24.33ms, 10%: 29.53ms, 25%: 26.11ms, 50%: 28.14ms, 90%: 31.40ms
  - column10: 1%: 9.87ms, 10%: 11.98ms, 25%: 14.45ms, 50%: 16.34ms, 90%: 23.43ms

## vortex_default
- size_bytes: **173506500**
- compression_time_s: **3.449**
- full_scan_min median_ms: **4.41**
- random_access median_ms: **4.87**
- best_select_col: `column03` (avg median_ms **4.91**)
- validation_pass: **True**
- selectivity:
  - column01: 1%: 8.70ms, 10%: 6.41ms, 25%: 6.17ms, 50%: 6.48ms, 90%: 6.21ms
  - column03: 1%: 4.94ms, 10%: 4.94ms, 25%: 4.73ms, 50%: 4.91ms, 90%: 5.04ms
  - column04: 1%: 5.12ms, 10%: 4.97ms, 25%: 5.08ms, 50%: 5.02ms, 90%: 5.01ms
  - column06: 1%: 6.06ms, 10%: 6.58ms, 25%: 6.91ms, 50%: 6.84ms, 90%: 8.02ms
  - column08: 1%: 6.66ms, 10%: 6.90ms, 25%: 7.22ms, 50%: 6.71ms, 90%: 7.09ms
  - column10: 1%: 5.27ms, 10%: 5.36ms, 25%: 5.32ms, 50%: 6.04ms, 90%: 7.37ms
