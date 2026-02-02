# Overall Summary

- datasets: **1**

## Datasets
name | rows | input_rows | dropped_rows | input_size_mb
--- | --- | --- | --- | ---
ozil43 | 6,000,000 | 9,624,351 | 3,624,351 | 6471.16

## Formats (Geomean)
format | comp_ratio | comp_time_s | comp_speed_mb_s | decomp_time_s | decomp_speed_mb_s | size_mb | full_scan_ms | selective_pred_ms | random_access_ms
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 1 | n/a | n/a | n/a | n/a | 6471.158 | 0.734 | 0.8 | 8.106
parquet_snappy | 25.678 | 5.149 | 1256.767 | 0.605 | 416.504 | 252.016 | 40.156 | 29.837 | 62.177
parquet_uncompressed | 14.519 | 7.079 | 914.086 | 0.596 | 747.357 | 445.697 | 29.064 | 26.89 | 31.972
parquet_zstd | 35.542 | 4.835 | 1338.344 | 0.608 | 299.652 | 182.073 | 33.258 | 32.628 | 70.896
vortex_default | 27.176 | 5.766 | 1122.316 | 4.109 | 57.945 | 238.121 | 5.989 | 5.095 | 31.13

## Formats (Cold run across datasets)
format | cold_full_geomean | cold_full_median | cold_full_p95 | cold_sel_geomean | cold_sel_median | cold_sel_p95 | cold_rand_geomean | cold_rand_median | cold_rand_p95
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 1.722 | 1.722 | 1.722 | 1.228 | 1.228 | 1.228 | 9.329 | 9.329 | 9.329
parquet_snappy | 371.461 | 371.461 | 371.461 | 42.035 | 42.035 | 42.035 | 117.323 | 117.323 | 117.323
parquet_uncompressed | 524.92 | 524.92 | 524.92 | 51.854 | 51.854 | 51.854 | 100.602 | 100.602 | 100.602
parquet_zstd | 298.704 | 298.704 | 298.704 | 41.69 | 41.69 | 41.69 | 110.894 | 110.894 | 110.894
vortex_default | 171.548 | 171.548 | 171.548 | 17.709 | 17.709 | 17.709 | 154.735 | 154.735 | 154.735