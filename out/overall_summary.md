# Overall Summary

- datasets: **17**

## Datasets
name | rows | input_rows | dropped_rows | input_size_mb
--- | --- | --- | --- | ---
Arade_1 | 9,888,775 | 9,888,775 | 0 | 811.35
Bimbo_1 | 74,180,464 | 74,180,464 | 0 | 3037.92
CityMaxCapita_1 | 912,657 | 912,657 | 0 | 332.98
Euro2016_1 | 2,052,497 | 2,052,497 | 0 | 390.62
HashTags_1 | 511,511 | 511,511 | 0 | 640.17
IGlocations1_1 | 81,427 | 81,611 | 184 | 6.58
MLB_18 | 173,622 | 173,622 | 0 | 42.73
MedPayment1_1 | 9,153,273 | 9,153,273 | 0 | 1747.66
MedPayment2_1 | 9,153,273 | 9,153,273 | 0 | 1840.65
Medicare1_1 | 8,645,072 | 8,645,072 | 0 | 1665.36
Medicare2_1 | 9,153,273 | 9,153,273 | 0 | 1746.33
Medicare3_1 | 9,287,877 | 9,287,877 | 0 | 2149.35
Physicians_1 | 9,153,273 | 9,153,273 | 0 | 1747.66
Rentabilidad_1 | 399,545 | 399,545 | 0 | 377.91
Romance_1 | 1,586,588 | 1,586,588 | 0 | 532.85
YaleLanguages_1 | 806,586 | 904,873 | 98,287 | 238.13
dummy | 3 | 3 | 0 | 0.00

## Formats (Geomean)
format | comp_ratio | comp_time_s | comp_speed_mb_s | decomp_time_s | decomp_speed_mb_s | size_mb | full_scan_ms | selective_pred_ms | random_access_ms
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 1 | n/a | n/a | n/a | n/a | 219.93 | 0.909 | 1.628 | 3.708
parquet_snappy | 3.174 | 1.351 | 162.768 | 0.161 | 429.445 | 69.285 | 10.017 | 11.826 | 22.501
parquet_uncompressed | 2.182 | 1.782 | 123.415 | 0.152 | 661.892 | 100.772 | 10.091 | 11.52 | 14.693
parquet_zstd | 4.197 | 1.186 | 185.49 | 0.167 | 314.058 | 52.402 | 10.523 | 12.63 | 28.962
vortex_default | 2.662 | 1.366 | 161.053 | 0.415 | 198.82 | 82.605 | 4.579 | 5.112 | 12.237