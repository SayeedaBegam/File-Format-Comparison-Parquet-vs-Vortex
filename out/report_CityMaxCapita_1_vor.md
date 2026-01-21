# Benchmark Report

- Input: `clouddb_project/CityMaxCapita_1_vor.csv` (csv)
- Rows: **912657**
- min_col: `column03`
- filter_col: `column00`
- select_cols: `column03`

## parquet_zstd
- size_bytes: **89412492**
- compression_time_s: **1.276**
- full_scan_min median_ms: **11.92**
- random_access median_ms: **8.49**
- best_select_col: `column03` (avg median_ms **13.43**)
- selectivity:
  - column03: 1%: 13.19ms, 10%: 13.51ms, 25%: 12.94ms, 50%: 13.46ms, 90%: 14.07ms

## vortex_compact
- size_bytes: **480618135**
- compression_time_s: **4.271**
- full_scan_min median_ms: **0.95**
- random_access median_ms: **0.94**
- best_select_col: `column03` (avg median_ms **1.85**)
- selectivity:
  - column03: 1%: 1.24ms, 10%: 1.29ms, 25%: 1.91ms, 50%: 2.16ms, 90%: 2.65ms
