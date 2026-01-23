# Benchmark Report

- Input: `clouddb_project/CityMaxCapita_1.csv` (csv)
- Rows: **912657**
- Input rows: **912657**
- Dropped rows: **0**
- min_col: `column03`
- filter_col: `column26`
- select_cols: `column03, column04, column05, column06, column08, column09, column11, column13, column15, column16, column18, column25`
- recommendations:
  - storage-first: `parquet_zstd`
  - read-latency-first: `vortex_default`
  - scan-first: `vortex_default`

## parquet_zstd
- size_bytes: **89412492**
- compression_time_s: **1.263**
- compression_ratio: **3.905**
- full_scan_min median_ms: **13.23**
- random_access median_ms: **11.82**
- best_select_col: `column09` (avg median_ms **11.86**)
- validation_pass: **True**
- selectivity:
  - column03: 1%: 12.51ms, 10%: 12.76ms, 25%: 12.94ms, 50%: 12.82ms, 90%: 13.33ms
  - column04: 1%: 13.78ms, 10%: 12.94ms, 25%: 12.88ms, 50%: 13.39ms, 90%: 13.51ms
  - column05: 1%: 13.20ms, 10%: 13.74ms, 25%: 13.21ms, 50%: 14.20ms, 90%: 14.44ms
  - column06: 1%: 18.07ms, 10%: 17.72ms, 25%: 16.84ms, 50%: 17.16ms, 90%: 18.35ms
  - column08: 1%: 13.55ms, 10%: 13.44ms, 25%: 14.03ms, 50%: 14.79ms, 90%: 12.47ms
  - column09: 1%: 11.48ms, 10%: 11.86ms, 25%: 12.33ms, 50%: 11.77ms, 90%: 11.88ms
  - column11: 1%: 12.27ms, 10%: 11.47ms, 25%: 12.78ms, 50%: 12.70ms, 90%: 12.84ms
  - column13: 1%: 13.52ms, 10%: 13.78ms, 25%: 12.79ms, 50%: 13.73ms, 90%: 13.69ms
  - column15: 1%: 13.50ms, 10%: 14.95ms, 25%: 15.47ms, 50%: 14.65ms, 90%: 14.59ms
  - column16: 1%: 13.31ms, 10%: 12.60ms, 25%: 12.30ms, 50%: 11.81ms, 90%: 11.68ms
  - column18: 1%: 10.88ms, 10%: 13.82ms, 25%: 14.64ms, 50%: 13.86ms, 90%: 13.46ms
  - column25: 1%: 18.70ms, 10%: 17.51ms, 25%: 19.31ms, 50%: 17.63ms, 90%: 21.24ms

## vortex_default
- size_bytes: **150132564**
- compression_time_s: **3.890**
- compression_ratio: **2.326**
- full_scan_min median_ms: **4.06**
- random_access median_ms: **4.74**
- best_select_col: `column03` (avg median_ms **4.78**)
- validation_pass: **True**
- selectivity:
  - column03: 1%: 4.73ms, 10%: 4.56ms, 25%: 4.58ms, 50%: 4.89ms, 90%: 5.12ms
  - column04: 1%: 5.14ms, 10%: 4.57ms, 25%: 5.59ms, 50%: 5.54ms, 90%: 5.93ms
  - column05: 1%: 5.04ms, 10%: 5.04ms, 25%: 5.47ms, 50%: 5.41ms, 90%: 5.42ms
  - column06: 1%: 6.83ms, 10%: 6.47ms, 25%: 6.76ms, 50%: 6.95ms, 90%: 7.13ms
  - column08: 1%: 6.68ms, 10%: 8.53ms, 25%: 6.56ms, 50%: 6.75ms, 90%: 7.22ms
  - column09: 1%: 6.55ms, 10%: 6.45ms, 25%: 6.53ms, 50%: 6.67ms, 90%: 6.94ms
  - column11: 1%: 6.67ms, 10%: 6.82ms, 25%: 6.73ms, 50%: 6.80ms, 90%: 6.85ms
  - column13: 1%: 5.71ms, 10%: 5.19ms, 25%: 5.45ms, 50%: 5.63ms, 90%: 5.77ms
  - column15: 1%: 6.68ms, 10%: 6.88ms, 25%: 6.92ms, 50%: 7.07ms, 90%: 7.36ms
  - column16: 1%: 9.91ms, 10%: 9.72ms, 25%: 9.07ms, 50%: 9.66ms, 90%: 7.76ms
  - column18: 1%: 5.74ms, 10%: 7.13ms, 25%: 7.00ms, 50%: 7.30ms, 90%: 6.72ms
  - column25: 1%: 8.95ms, 10%: 8.75ms, 25%: 9.04ms, 50%: 9.30ms, 90%: 8.66ms
