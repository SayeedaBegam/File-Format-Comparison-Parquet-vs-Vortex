# Overall Summary

- datasets: **1**

## Datasets
name | rows | input_rows | dropped_rows | input_size_mb
--- | --- | --- | --- | ---
NYC_1 | 8,000,000 | 9,624,351 | 1,624,351 | 6471.16

## Formats (Geomean)
format | comp_ratio | comp_time_s | comp_speed_mb_s | decomp_time_s | decomp_speed_mb_s | size_mb | full_scan_ms | selective_pred_ms | random_access_ms
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 1 | n/a | n/a | n/a | n/a | 6471.158 | 1.26 | 0.636 | 11.845
parquet_snappy | 17.976 | 6.819 | 948.937 | 0.829 | 434.361 | 359.992 | 33.002 | 25.084 | 74.426
parquet_uncompressed | 10.477 | 10.378 | 623.546 | 0.806 | 766.157 | 617.682 | 31.562 | 22.385 | 45.476
parquet_zstd | 24.729 | 5.603 | 1154.988 | 0.828 | 316.069 | 261.681 | 34.595 | 24.749 | 90.532
vortex_default | 19.63 | 7.69 | 841.536 | 6.022 | 54.739 | 329.651 | 6.402 | 4.868 | 46.678

## Formats (Cold run across datasets)
format | cold_full_geomean | cold_full_median | cold_full_p95 | cold_sel_geomean | cold_sel_median | cold_sel_p95 | cold_rand_geomean | cold_rand_median | cold_rand_p95
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 1.836 | 1.836 | 1.836 | 0.727 | 0.727 | 0.727 | 11.895 | 11.895 | 11.895
parquet_snappy | 495.601 | 495.601 | 495.601 | 31.303 | 31.303 | 31.303 | 129.089 | 129.089 | 129.089
parquet_uncompressed | 698.649 | 698.649 | 698.649 | 29.899 | 29.899 | 29.899 | 121.48 | 121.48 | 121.48
parquet_zstd | 404.296 | 404.296 | 404.296 | 27.754 | 27.754 | 27.754 | 131.684 | 131.684 | 131.684
vortex_default | 228.603 | 228.603 | 228.603 | 8.591 | 8.591 | 8.591 | 177.492 | 177.492 | 177.492