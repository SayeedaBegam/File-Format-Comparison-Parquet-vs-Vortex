# Overall Summary

- datasets: **1**

## Datasets
name | rows | input_rows | dropped_rows | input_size_mb
--- | --- | --- | --- | ---
ozil43 | 2,000,000 | 9,624,351 | 7,624,351 | 6471.16

## Formats (Geomean)
format | comp_ratio | comp_time_s | comp_speed_mb_s | decomp_time_s | decomp_speed_mb_s | size_mb | full_scan_ms | selective_pred_ms | random_access_ms
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 1 | n/a | n/a | n/a | n/a | 6471.158 | 0.401 | 0.921 | 3.313
parquet_snappy | 80.034 | 1.731 | 3738.642 | 0.256 | 316.051 | 80.855 | 20.88 | 18.731 | 28.856
parquet_uncompressed | 45.415 | 2.225 | 2908.773 | 0.268 | 532.189 | 142.489 | 21.127 | 17.188 | 15.279
parquet_zstd | 109.517 | 1.519 | 4260.001 | 0.257 | 229.573 | 59.088 | 21.743 | 18.45 | 34.619
vortex_default | 81.499 | 2.163 | 2991.123 | 1.783 | 44.535 | 79.402 | 3.349 | 4.606 | 22.402

## Formats (Cold run across datasets)
format | cold_full_geomean | cold_full_median | cold_full_p95 | cold_sel_geomean | cold_sel_median | cold_sel_p95 | cold_rand_geomean | cold_rand_median | cold_rand_p95
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 0.663 | 0.663 | 0.663 | 1.321 | 1.321 | 1.321 | 4.544 | 4.544 | 4.544
parquet_snappy | 127.202 | 127.202 | 127.202 | 79.437 | 79.437 | 79.437 | 77.11 | 77.11 | 77.11
parquet_uncompressed | 180.249 | 180.249 | 180.249 | 31.44 | 31.44 | 31.44 | 118.001 | 118.001 | 118.001
parquet_zstd | 103.814 | 103.814 | 103.814 | 23.216 | 23.216 | 23.216 | 75.367 | 75.367 | 75.367
vortex_default | 289.596 | 289.596 | 289.596 | 11.593 | 11.593 | 11.593 | 138.374 | 138.374 | 138.374