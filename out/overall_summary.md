# Overall Summary

- datasets: **6**

## Datasets
name | rows | input_rows | dropped_rows | input_size_mb
--- | --- | --- | --- | ---
CityMaxCapita_1 | 912,657 | 912,657 | 0 | 332.98
Euro2016_1 | 2,052,497 | 2,052,497 | 0 | 390.62
IGlocations1_1 | 81,611 | 81,611 | 0 | 6.58
MedPayment1_1 | 9,153,273 | 9,153,273 | 0 | 1747.66
MedPayment2_1 | 9,153,273 | 9,153,273 | 0 | 1840.65
NYC_1 | 9,624,351 | 9,624,351 | 0 | 6471.16

## Formats (Geomean)
format | comp_ratio | comp_time_s | size_mb | full_scan_ms | selective_pred_ms | random_access_ms
--- | --- | --- | --- | --- | --- | ---
parquet_snappy | 3.809 | 1.886 | 134.175 | 18.773 | 18.161 | 34.012
parquet_uncompressed | 2.394 | 2.532 | 213.497 | 17.363 | 17.215 | 21.835
parquet_zstd | 5.179 | 1.669 | 98.685 | 19.471 | 17.956 | 40.007
vortex_default | 3.618 | 1.778 | 141.281 | 4.781 | 4.676 | 12.336