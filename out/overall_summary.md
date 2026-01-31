# Overall Summary

- datasets: **13**

## Datasets
name | rows | input_rows | dropped_rows | input_size_mb
--- | --- | --- | --- | ---
Arade_1 | 9,888,775 | 9,888,775 | 0 | 811.35
Bimbo_1 | 74,180,464 | 74,180,464 | 0 | 3037.92
CityMaxCapita_1 | 912,657 | 912,657 | 0 | 332.98
Euro2016_1 | 2,052,497 | 2,052,497 | 0 | 390.62
MLB_18 | 173,622 | 173,622 | 0 | 42.73
MedPayment1_1 | 9,153,273 | 9,153,273 | 0 | 1747.66
MedPayment2_1 | 9,153,273 | 9,153,273 | 0 | 1840.65
Medicare1_1 | 8,645,072 | 8,645,072 | 0 | 1665.36
Medicare2_1 | 9,153,273 | 9,153,273 | 0 | 1746.33
Medicare3_1 | 9,287,877 | 9,287,877 | 0 | 2149.35
Physicians_1 | 9,153,273 | 9,153,273 | 0 | 1747.66
Rentabilidad_1 | 399,545 | 399,545 | 0 | 377.91
Romance_1 | 1,586,588 | 1,586,588 | 0 | 532.85

## Formats (Geomean)
format | comp_ratio | comp_time_s | comp_speed_mb_s | decomp_time_s | decomp_speed_mb_s | size_mb | full_scan_ms | selective_pred_ms | random_access_ms
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 1 | n/a | n/a | n/a | n/a | 835.683 | 1.231 | 2.344 | 6.271
parquet_snappy | 3.644 | 2.823 | 296.049 | 0.283 | 811.492 | 229.336 | 22.102 | 25.896 | 40.202
parquet_uncompressed | 2.52 | 3.797 | 220.107 | 0.264 | 1256.999 | 331.651 | 22.705 | 26.133 | 27.512
parquet_zstd | 4.871 | 2.424 | 344.77 | 0.287 | 598.048 | 171.576 | 22.395 | 26.774 | 53.33
vortex_default | 3.43 | 2.588 | 322.879 | 0.713 | 341.736 | 243.66 | 6.145 | 6.353 | 18.223