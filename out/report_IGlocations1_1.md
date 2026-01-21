# Benchmark Report

- Input: `Dataset/IGlocations1_1.csv` (csv)
- Rows: **81611**
- min_col: `column01`
- filter_col: `column17`
- select_col: `column08`

## parquet_zstd
- size_bytes: **2289958**
- compression_time_s: **0.052**
- full_scan_min median_ms: **0.68**
- random_access median_ms: **0.78**

## vortex_default
- Not run yet: vortex_backend.py not implemented.
