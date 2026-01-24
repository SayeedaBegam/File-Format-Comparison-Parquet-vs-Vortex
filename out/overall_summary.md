# Overall Summary

- datasets: **7**

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

## Formats (Geomean)
format | comp_ratio | comp_time_s | size_mb | full_scan_ms | selective_pred_ms | random_access_ms
--- | --- | --- | --- | --- | --- | ---
parquet_snappy | 3.831 | 2.165 | 159.022 | 21.377 | 20.585 | 38.047
parquet_uncompressed | 2.435 | 2.906 | 250.231 | 21.399 | 20.626 | 25.265
parquet_zstd | 5.129 | 1.916 | 118.791 | 22.144 | 20.203 | 45.852
vortex_default | 3.559 | 2.077 | 171.162 | 5.12 | 4.839 | 13.428