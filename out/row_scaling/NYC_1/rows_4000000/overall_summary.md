# Overall Summary

- datasets: **1**

## Datasets
name | rows | input_rows | dropped_rows | input_size_mb
--- | --- | --- | --- | ---
ozil43 | 4,000,000 | 9,624,351 | 5,624,351 | 6471.16

## Formats (Geomean)
format | comp_ratio | comp_time_s | comp_speed_mb_s | decomp_time_s | decomp_speed_mb_s | size_mb | full_scan_ms | selective_pred_ms | random_access_ms
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 1 | n/a | n/a | n/a | n/a | 6471.158 | 0.539 | 1.721 | 7.742
parquet_snappy | 39.24 | 3.413 | 1896.106 | 0.448 | 368.41 | 164.914 | 31.542 | 31.459 | 46.69
parquet_uncompressed | 21.732 | 4.674 | 1384.552 | 0.442 | 674.319 | 297.773 | 31.137 | 30.554 | 27.894
parquet_zstd | 54.371 | 3.009 | 2150.292 | 0.48 | 248.048 | 119.018 | 33.042 | 34.336 | 56.609
vortex_default | 40.874 | 3.887 | 1664.936 | 2.261 | 70.024 | 158.32 | 4.123 | 4.886 | 28.218

## Formats (Cold run across datasets)
format | cold_full_geomean | cold_full_median | cold_full_p95 | cold_sel_geomean | cold_sel_median | cold_sel_p95 | cold_rand_geomean | cold_rand_median | cold_rand_p95
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 1.15 | 1.15 | 1.15 | 2.004 | 2.004 | 2.004 | 7.463 | 7.463 | 7.463
parquet_snappy | 255.092 | 255.092 | 255.092 | 44.488 | 44.488 | 44.488 | 96.966 | 96.966 | 96.966
parquet_uncompressed | 717.454 | 717.454 | 717.454 | 57.71 | 57.71 | 57.71 | 95.159 | 95.159 | 95.159
parquet_zstd | 275.208 | 275.208 | 275.208 | 41.682 | 41.682 | 41.682 | 94.506 | 94.506 | 94.506
vortex_default | 117.176 | 117.176 | 117.176 | 14.732 | 14.732 | 14.732 | 152.492 | 152.492 | 152.492