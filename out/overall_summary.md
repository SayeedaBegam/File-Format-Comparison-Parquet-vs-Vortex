# Overall Summary

- datasets: **8**

## Datasets
name | rows | input_rows | dropped_rows | input_size_mb
--- | --- | --- | --- | ---
CityMaxCapita_1 | 912,657 | 912,657 | 0 | 332.98
Euro2016_1 | 2,052,497 | 2,052,497 | 0 | 390.62
IGlocations1_1 | 81,611 | 81,611 | 0 | 6.58
MedPayment1_1 | 9,153,273 | 9,153,273 | 0 | 1747.66
MedPayment2_1 | 9,153,273 | 9,153,273 | 0 | 1840.65
NYC_1 | 9,624,351 | 9,624,351 | 0 | 6471.16
Physicians_1 | 9,153,273 | 9,153,273 | 0 | 1747.66
Uberlandia_1 | 7,559,227 | 7,559,227 | 0 | 6601.02

## Formats (Geomean)
format | comp_ratio | comp_time_s | size_mb | full_scan_ms | selective_pred_ms | random_access_ms
--- | --- | --- | --- | --- | --- | ---
parquet_snappy | 4.395 | 2.66 | 186.716 | 22.609 | 22.553 | 38.097
parquet_uncompressed | 2.757 | 3.534 | 297.695 | 22.602 | 22.061 | 24.982
parquet_zstd | 5.953 | 2.35 | 137.859 | 23.366 | 22.157 | 45.177
vortex_default | 3.978 | 2.546 | 206.313 | 5.144 | 5.08 | 16.625