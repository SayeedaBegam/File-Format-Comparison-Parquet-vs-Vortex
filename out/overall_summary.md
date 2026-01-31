# Overall Summary

- datasets: **15**

## Datasets
name | rows | input_rows | dropped_rows | input_size_mb
--- | --- | --- | --- | ---
1769877051_MLB_18 | 173,622 | 173,622 | 0 | 42.73
1769877075_MLB_18 | 173,622 | 173,622 | 0 | 42.73
1769877242_MLB_18 | 173,622 | 173,622 | 0 | 42.73
1769877427_dummy | 3 | 4 | 1 | 0.00
1769881248_MLB_18 | 173,622 | 173,622 | 0 | 42.73
CityMaxCapita_1 | 912,657 | 912,657 | 0 | 332.98
Euro2016_1 | 2,052,497 | 2,052,497 | 0 | 390.62
IGlocations1_1 | 81,611 | 81,611 | 0 | 6.58
MLB_18 | 173,622 | 173,622 | 0 | 42.73
MedPayment1_1 | 9,153,273 | 9,153,273 | 0 | 1747.66
MedPayment2_1 | 9,153,273 | 9,153,273 | 0 | 1840.65
NYC_1 | 9,624,351 | 9,624,351 | 0 | 6471.16
Physicians_1 | 9,153,273 | 9,153,273 | 0 | 1747.66
Uberlandia_1 | 7,559,227 | 7,559,227 | 0 | 6601.02
dummy | 3 | 4 | 1 | 0.00

## Formats (Geomean)
format | comp_ratio | comp_time_s | size_mb | full_scan_ms | selective_pred_ms | random_access_ms
--- | --- | --- | --- | --- | --- | ---
duckdb_table | 1 | n/a | 0.941 | 0.247 | 0.584 | 0.741
parquet_snappy | 2.793 | 0.577 | 12.468 | 10.192 | 10.267 | 16.793
parquet_uncompressed | 2.114 | 0.676 | 16.476 | 10.054 | 10.14 | 12.357
parquet_zstd | 3.453 | 0.544 | 10.085 | 10.446 | 10.661 | 20.713
vortex_default | 1.969 | 0.544 | 17.687 | 3.84 | 4.175 | 8.736