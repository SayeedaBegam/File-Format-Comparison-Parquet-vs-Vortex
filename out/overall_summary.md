# Overall Summary

- datasets: **5**

## Datasets
name | rows | input_rows | dropped_rows | input_size_mb
--- | --- | --- | --- | ---
CityMaxCapita_1 | 912,657 | 912,657 | 0 | 332.98
Euro2016_1 | 2,052,497 | 2,052,497 | 0 | 390.62
IGlocations1_1 | 81,611 | 81,611 | 0 | 6.58
MedPayment1_1 | 9,153,273 | 9,153,273 | 0 | 1747.66
MedPayment2_1 | 9,153,273 | 9,153,273 | 0 | 1840.65

## Formats (Geomean)
format | comp_ratio | comp_time_s | size_mb | full_scan_ms | selective_pred_ms | random_access_ms
--- | --- | --- | --- | --- | --- | ---
parquet_snappy | 2.947 | 1.417 | 104.379 | 16.156 | 15.969 | 27.683
parquet_uncompressed | 1.877 | 1.938 | 163.934 | 15.189 | 15.547 | 18.6
parquet_zstd | 3.997 | 1.227 | 76.955 | 16.841 | 16.006 | 32.622
vortex_default | 2.698 | 1.281 | 114.037 | 4.39 | 4.451 | 9.17