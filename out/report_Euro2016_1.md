# Benchmark Report

- Input: `clouddb_project/Euro2016_1.csv` (csv)
- Rows: **14601**
- min_col: `id`
- filter_col: `latitude`
- select_cols: `id`

## parquet_zstd
- size_bytes: **807203**
- compression_time_s: **0.043**
- full_scan_min median_ms: **9.34**
- random_access median_ms: **4.38**
- best_select_col: `id` (avg median_ms **5.19**)
- selectivity:
  - id: 1%: 4.23ms, 10%: 4.62ms, 25%: 5.51ms, 50%: 6.98ms, 90%: 4.63ms

## vortex_default
- size_bytes: **1244360**
- compression_time_s: **0.064**
- full_scan_min median_ms: **5.46**
- random_access median_ms: **6.90**
- best_select_col: `id` (avg median_ms **4.59**)
- selectivity:
  - id: 1%: 4.32ms, 10%: 3.98ms, 25%: 5.86ms, 50%: 4.13ms, 90%: 4.65ms
