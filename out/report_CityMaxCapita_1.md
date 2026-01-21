# Benchmark Report

- Input: `Dataset/CityMaxCapita_1.csv` (csv)
- Rows: **912657**
- min_col: `column03`
- filter_col: `column00`
- select_cols: `column03`

## parquet_zstd
- size_bytes: **89467618**
- compression_time_s: **1.340**
- full_scan_min median_ms: **7.80**
- random_access median_ms: **4.23**
- best_select_col: `column03` (avg median_ms **6.53**)

## vortex_default
- Not run yet: vortex_backend.py not implemented.
