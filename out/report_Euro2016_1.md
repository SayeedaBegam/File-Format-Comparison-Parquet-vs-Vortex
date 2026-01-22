# Benchmark Report

- Input: `clouddb_project/Euro2016_1.csv` (csv)
- Rows: **2052497**
- min_col: `column01`
- filter_col: `column02`
- select_cols: `column01, column03, column04, column06, column08, column10`

## parquet_zstd
- size_bytes: **102531934**
- compression_time_s: **1.185**
- full_scan_min median_ms: **22.63**
- random_access median_ms: **22.59**
- best_select_col: `column10` (avg median_ms **15.49**)
- selectivity:
  - column01: 1%: 26.56ms, 10%: 24.05ms, 25%: 23.01ms, 50%: 22.05ms, 90%: 22.13ms
  - column03: 1%: 19.88ms, 10%: 20.69ms, 25%: 21.75ms, 50%: 20.11ms, 90%: 22.36ms
  - column04: 1%: 21.29ms, 10%: 21.76ms, 25%: 22.47ms, 50%: 21.50ms, 90%: 21.35ms
  - column06: 1%: 24.55ms, 10%: 24.23ms, 25%: 24.02ms, 50%: 23.72ms, 90%: 24.44ms
  - column08: 1%: 24.25ms, 10%: 23.76ms, 25%: 24.48ms, 50%: 24.53ms, 90%: 23.04ms
  - column10: 1%: 10.42ms, 10%: 12.85ms, 25%: 16.61ms, 50%: 15.85ms, 90%: 21.73ms

## vortex_default
- size_bytes: **173503388**
- compression_time_s: **3.450**
- full_scan_min median_ms: **4.45**
- random_access median_ms: **4.92**
- best_select_col: `column03` (avg median_ms **4.95**)
- selectivity:
  - column01: 1%: 8.52ms, 10%: 8.40ms, 25%: 6.26ms, 50%: 6.15ms, 90%: 6.33ms
  - column03: 1%: 4.90ms, 10%: 4.95ms, 25%: 4.93ms, 50%: 4.85ms, 90%: 5.11ms
  - column04: 1%: 4.91ms, 10%: 5.02ms, 25%: 5.12ms, 50%: 5.07ms, 90%: 5.03ms
  - column06: 1%: 5.91ms, 10%: 6.87ms, 25%: 7.26ms, 50%: 6.94ms, 90%: 7.18ms
  - column08: 1%: 6.48ms, 10%: 6.77ms, 25%: 6.89ms, 50%: 6.79ms, 90%: 7.37ms
  - column10: 1%: 4.90ms, 10%: 5.07ms, 25%: 5.25ms, 50%: 5.63ms, 90%: 6.75ms
