# Overall Summary

- datasets: **1**

## Datasets
name | rows | input_rows | dropped_rows | input_size_mb
--- | --- | --- | --- | ---
NYC_1 | 1,000,000 | 9,624,351 | 8,624,351 | 6471.16

## Formats (Geomean)
format | comp_ratio | comp_time_s | comp_speed_mb_s | decomp_time_s | decomp_speed_mb_s | size_mb | full_scan_ms | selective_pred_ms | random_access_ms
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 1 | n/a | n/a | n/a | n/a | 6471.158 | 0.391 | 1.318 | 2.35
parquet_snappy | 199.694 | 0.819 | 7904.737 | 0.164 | 197.472 | 32.405 | 11.433 | 11.397 | 17.785
parquet_uncompressed | 126.973 | 0.976 | 6631.147 | 0.169 | 302.206 | 50.965 | 10.824 | 10.865 | 9.615
parquet_zstd | 261.482 | 0.814 | 7953.523 | 0.173 | 142.976 | 24.748 | 11.493 | 12.167 | 33.237
vortex_default | 187.723 | 1.13 | 5726.543 | 0.974 | 35.39 | 34.472 | 3.172 | 2.927 | 10.261

## Formats (Cold run across datasets)
format | cold_full_geomean | cold_full_median | cold_full_p95 | cold_sel_geomean | cold_sel_median | cold_sel_p95 | cold_rand_geomean | cold_rand_median | cold_rand_p95
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 0.442 | 0.442 | 0.442 | 1.315 | 1.315 | 1.315 | 3.785 | 3.785 | 3.785
parquet_snappy | 66.131 | 66.131 | 66.131 | 15.255 | 15.255 | 15.255 | 64.953 | 64.953 | 64.953
parquet_uncompressed | 92.116 | 92.116 | 92.116 | 17.996 | 17.996 | 17.996 | 95.585 | 95.585 | 95.585
parquet_zstd | 59.7 | 59.7 | 59.7 | 15.685 | 15.685 | 15.685 | 81.493 | 81.493 | 81.493
vortex_default | 33.139 | 33.139 | 33.139 | 10.568 | 10.568 | 10.568 | 126.39 | 126.39 | 126.39