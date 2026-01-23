# Benchmark Report

- Input: `clouddb_project/MedPayment2_1.csv` (csv)
- Rows: **9153273**
- min_col: `column10`
- filter_col: `column24`
- select_cols: `column10, column01, column02, column03, column04, column05, column08, column22, column25, column26, column27, column28`

## parquet_zstd
- size_bytes: **431308194**
- compression_time_s: **4.693**
- full_scan_min median_ms: **37.49**
- random_access median_ms: **36.97**
- best_select_col: `column05` (avg median_ms **35.06**)
- selectivity:
  - column10: 1%: 37.32ms, 10%: 37.41ms, 25%: 35.47ms, 50%: 35.52ms, 90%: 34.16ms
  - column01: 1%: 40.99ms, 10%: 39.95ms, 25%: 38.71ms, 50%: 38.12ms, 90%: 39.23ms
  - column02: 1%: 41.20ms, 10%: 39.98ms, 25%: 41.51ms, 50%: 39.38ms, 90%: 40.31ms
  - column03: 1%: 43.37ms, 10%: 45.39ms, 25%: 43.25ms, 50%: 41.62ms, 90%: 43.98ms
  - column04: 1%: 37.44ms, 10%: 37.65ms, 25%: 38.02ms, 50%: 39.67ms, 90%: 38.63ms
  - column05: 1%: 34.42ms, 10%: 34.17ms, 25%: 34.92ms, 50%: 35.87ms, 90%: 35.92ms
  - column08: 1%: 35.05ms, 10%: 35.02ms, 25%: 37.10ms, 50%: 37.05ms, 90%: 37.37ms
  - column22: 1%: 36.84ms, 10%: 35.23ms, 25%: 39.60ms, 50%: 35.82ms, 90%: 35.91ms
  - column25: 1%: 39.01ms, 10%: 38.91ms, 25%: 38.74ms, 50%: 38.43ms, 90%: 37.83ms
  - column26: 1%: 42.06ms, 10%: 41.70ms, 25%: 40.65ms, 50%: 41.10ms, 90%: 41.35ms
  - column27: 1%: 43.49ms, 10%: 42.62ms, 25%: 39.84ms, 50%: 37.73ms, 90%: 38.30ms
  - column28: 1%: 41.07ms, 10%: 38.27ms, 25%: 39.39ms, 50%: 39.55ms, 90%: 38.03ms

## vortex_default
- size_bytes: **636291500**
- compression_time_s: **17.397**
- full_scan_min median_ms: **8.56**
- random_access median_ms: **6.42**
- best_select_col: `column08` (avg median_ms **12.82**)
- selectivity:
  - column10: 1%: 16.97ms, 10%: 16.72ms, 25%: 16.65ms, 50%: 16.96ms, 90%: 17.81ms
  - column01: 1%: 14.63ms, 10%: 16.84ms, 25%: 17.65ms, 50%: 19.47ms, 90%: 22.59ms
  - column02: 1%: 15.47ms, 10%: 18.27ms, 25%: 20.13ms, 50%: 21.92ms, 90%: 25.08ms
  - column03: 1%: 13.34ms, 10%: 15.65ms, 25%: 16.79ms, 50%: 18.91ms, 90%: 21.17ms
  - column04: 1%: 19.79ms, 10%: 19.60ms, 25%: 20.08ms, 50%: 21.22ms, 90%: 21.40ms
  - column05: 1%: 20.31ms, 10%: 19.99ms, 25%: 19.81ms, 50%: 20.12ms, 90%: 20.10ms
  - column08: 1%: 10.27ms, 10%: 11.43ms, 25%: 11.44ms, 50%: 13.88ms, 90%: 17.10ms
  - column22: 1%: 22.08ms, 10%: 21.03ms, 25%: 21.90ms, 50%: 22.04ms, 90%: 21.94ms
  - column25: 1%: 17.79ms, 10%: 18.70ms, 25%: 18.50ms, 50%: 18.01ms, 90%: 20.18ms
  - column26: 1%: 19.43ms, 10%: 19.45ms, 25%: 19.35ms, 50%: 20.89ms, 90%: 24.04ms
  - column27: 1%: 18.79ms, 10%: 18.25ms, 25%: 18.45ms, 50%: 18.72ms, 90%: 20.21ms
  - column28: 1%: 15.06ms, 10%: 18.95ms, 25%: 20.74ms, 50%: 23.06ms, 90%: 26.11ms
