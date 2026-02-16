# Overall Summary

- datasets: **19**

## Datasets
name | rows | input_rows | dropped_rows | input_size_mb
--- | --- | --- | --- | ---
Arade_1 | 9,888,775 | 9,888,775 | 0 | 811.35
Bimbo_1 | 74,180,464 | 74,180,464 | 0 | 3037.92
CityMaxCapita_1 | 912,657 | 912,657 | 0 | 332.98
Euro2016_1 | 2,052,497 | 2,052,497 | 0 | 390.62
Food_1 | 5,216,593 | 5,216,593 | 0 | 205.91
HashTags_1 | 511,511 | 511,511 | 0 | 640.17
IGlocations1_1 | 81,611 | 81,611 | 0 | 6.58
MLB_18 | 173,622 | 173,622 | 0 | 42.73
MedPayment1_1 | 9,153,273 | 9,153,273 | 0 | 1747.66
MedPayment2_1 | 9,153,273 | 9,153,273 | 0 | 1840.65
Medicare1_1 | 8,645,072 | 8,645,072 | 0 | 1665.36
Medicare2_1 | 9,153,273 | 9,153,273 | 0 | 1746.33
Medicare3_1 | 9,287,877 | 9,287,877 | 0 | 2149.35
NYC_1 | 9,624,351 | 9,624,351 | 0 | 6471.16
Physicians_1 | 9,153,273 | 9,153,273 | 0 | 1747.66
Rentabilidad_1 | 399,545 | 399,545 | 0 | 377.91
Romance_1 | 1,586,588 | 1,586,588 | 0 | 532.85
YaleLanguages_1 | 806,586 | 904,873 | 98,287 | 238.13
dummy | 3 | 3 | 0 | 0.00

## Formats (Geomean)
format | comp_ratio | comp_time_s | comp_speed_mb_s | decomp_time_s | decomp_speed_mb_s | size_mb | full_scan_ms | selective_pred_ms | random_access_ms
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 1 | n/a | n/a | n/a | n/a | 261.866 | 0.911 | 1.506 | 4.184
parquet_snappy | 3.496 | 1.477 | 177.265 | 0.182 | 410.773 | 74.912 | 13.079 | 15.338 | 28.979
parquet_uncompressed | 2.376 | 1.945 | 134.645 | 0.172 | 641.773 | 110.235 | 12.688 | 14.126 | 18.564
parquet_zstd | 4.644 | 1.295 | 202.146 | 0.189 | 297.725 | 56.393 | 13.657 | 15.394 | 37.355
vortex_default | 2.985 | 1.43 | 183.059 | 0.417 | 210.307 | 87.723 | 4.823 | 5.102 | 13.576

## Formats (Cold run across datasets)
format | cold_full_geomean | cold_full_median | cold_full_p95 | cold_sel_geomean | cold_sel_median | cold_sel_p95 | cold_rand_geomean | cold_rand_median | cold_rand_p95
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 1.267 | 1.117 | 3.023 | 1.691 | 1.532 | 5.226 | 4.954 | 5.928 | 32.762
parquet_snappy | 71.593 | 117.02 | 619.587 | 24.861 | 30.826 | 253.125 | 172.48 | 344.297 | 989.662
parquet_uncompressed | 87.517 | 222.322 | 810.341 | 32.664 | 36.044 | 805.559 | 172.389 | 332.076 | 1066.917
parquet_zstd | 72.47 | 86.904 | 553.21 | 24.746 | 32.73 | 196.659 | 157.849 | 267.312 | 959.231
vortex_default | 59.245 | 70.173 | 317.943 | 17.48 | 10.956 | 215.571 | 192.73 | 243.26 | 780.474