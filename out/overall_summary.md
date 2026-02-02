# Overall Summary

- datasets: **18**

## Datasets
name | rows | input_rows | dropped_rows | input_size_mb
--- | --- | --- | --- | ---
Arade_1 | 9,888,775 | 9,888,775 | 0 | 811.35
Bimbo_1 | 74,180,464 | 74,180,464 | 0 | 3037.92
CityMaxCapita_1 | 912,657 | 912,657 | 0 | 332.98
Euro2016_1 | 2,052,497 | 2,052,497 | 0 | 390.62
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
duckdb_table | 1 | n/a | n/a | n/a | n/a | 265.387 | 0.929 | 1.549 | 3.857
parquet_snappy | 3.448 | 1.552 | 170.996 | 0.196 | 392.632 | 76.976 | 14.446 | 16.673 | 28.388
parquet_uncompressed | 2.341 | 2.045 | 129.78 | 0.185 | 614.31 | 113.374 | 14.056 | 15.408 | 18.433
parquet_zstd | 4.571 | 1.363 | 194.76 | 0.203 | 285.571 | 58.054 | 15.051 | 16.701 | 36.426
vortex_default | 2.908 | 1.513 | 175.381 | 0.469 | 194.526 | 91.25 | 4.939 | 5.15 | 13.981

## Formats (Cold run across datasets)
format | cold_full_geomean | cold_full_median | cold_full_p95 | cold_sel_geomean | cold_sel_median | cold_sel_p95 | cold_rand_geomean | cold_rand_median | cold_rand_p95
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 1.281 | 1.48 | 3.023 | 1.742 | 1.555 | 5.226 | 4.604 | 5.292 | 32.762
parquet_snappy | 85.863 | 139.687 | 619.587 | 27.723 | 34.533 | 253.125 | 186.336 | 365.64 | 989.662
parquet_uncompressed | 105.566 | 248.957 | 810.341 | 37.071 | 38.301 | 805.559 | 193.3 | 396.319 | 1066.917
parquet_zstd | 85.793 | 113.598 | 553.21 | 27.434 | 37.149 | 196.659 | 166.608 | 288.816 | 959.231
vortex_default | 56.891 | 62.085 | 317.943 | 16.449 | 10.807 | 215.571 | 192.912 | 260.895 | 780.474