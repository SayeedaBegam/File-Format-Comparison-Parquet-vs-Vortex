# Overall Summary

- datasets: **1**

## Datasets
name | rows | input_rows | dropped_rows | input_size_mb
--- | --- | --- | --- | ---
NYC_1 | 9,624,351 | 9,624,351 | 0 | 6471.16

## Formats (Geomean)
format | comp_ratio | comp_time_s | comp_speed_mb_s | decomp_time_s | decomp_speed_mb_s | size_mb | full_scan_ms | selective_pred_ms | random_access_ms
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 1 | n/a | n/a | n/a | n/a | 6471.158 | 1.436 | 0.717 | 14.062
parquet_snappy | 13.743 | 7.173 | 902.196 | 0.958 | 491.421 | 470.882 | 36.203 | 30.616 | 94.366
parquet_uncompressed | 8.09 | 9.339 | 692.915 | 0.935 | 855.22 | 799.902 | 35.225 | 28.43 | 48.265
parquet_zstd | 18.909 | 7.144 | 905.813 | 0.956 | 357.801 | 342.228 | 38.732 | 32.647 | 115.64
vortex_default | 15.693 | 9.344 | 692.568 | 3.858 | 106.894 | 412.372 | 7.498 | 5.064 | 52.351

## Formats (Cold run across datasets)
format | cold_full_geomean | cold_full_median | cold_full_p95 | cold_sel_geomean | cold_sel_median | cold_sel_p95 | cold_rand_geomean | cold_rand_median | cold_rand_p95
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 2.251 | 2.251 | 2.251 | 0.939 | 0.939 | 0.939 | 14.656 | 14.656 | 14.656
parquet_snappy | 617.209 | 617.209 | 617.209 | 32.254 | 32.254 | 32.254 | 148.719 | 148.719 | 148.719
parquet_uncompressed | 838.952 | 838.952 | 838.952 | 34.85 | 34.85 | 34.85 | 123.363 | 123.363 | 123.363
parquet_zstd | 478.67 | 478.67 | 478.67 | 33.037 | 33.037 | 33.037 | 173.632 | 173.632 | 173.632
vortex_default | 276.075 | 276.075 | 276.075 | 8.92 | 8.92 | 8.92 | 162.402 | 162.402 | 162.402