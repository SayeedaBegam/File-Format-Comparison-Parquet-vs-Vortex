# Overall Summary

- datasets: **18**

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

## Formats (Geomean)
format | comp_ratio | comp_time_s | comp_speed_mb_s | decomp_time_s | decomp_speed_mb_s | size_mb | full_scan_ms | selective_pred_ms | random_access_ms
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 1 | n/a | n/a | n/a | n/a | 608.201 | 0.993 | 1.683 | 4.984
parquet_snappy | 4.204 | 1.985 | 306.424 | 0.225 | 643.893 | 144.68 | 14.118 | 16.677 | 32.593
parquet_uncompressed | 2.796 | 2.658 | 228.802 | 0.213 | 1021.124 | 217.496 | 13.666 | 15.301 | 20.417
parquet_zstd | 5.689 | 1.734 | 350.792 | 0.237 | 451.492 | 106.9 | 14.794 | 16.783 | 42.876
vortex_default | 4.034 | 1.891 | 321.67 | 0.543 | 277.823 | 150.756 | 4.996 | 5.259 | 15.317

## Formats (Cold run across datasets)
format | cold_full_geomean | cold_full_median | cold_full_p95 | cold_sel_geomean | cold_sel_median | cold_sel_p95 | cold_rand_geomean | cold_rand_median | cold_rand_p95
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
duckdb_table | 1.363 | 1.48 | 3.023 | 1.886 | 1.555 | 5.226 | 5.907 | 8.749 | 32.762
parquet_snappy | 85.967 | 139.687 | 619.587 | 27.772 | 34.533 | 253.125 | 214.187 | 365.64 | 989.662
parquet_uncompressed | 104.995 | 248.957 | 810.341 | 36.72 | 38.301 | 805.559 | 211.455 | 396.319 | 1066.917
parquet_zstd | 85.762 | 113.598 | 553.21 | 27.174 | 37.149 | 196.659 | 195.352 | 288.816 | 959.231
vortex_default | 70.266 | 71.484 | 317.943 | 19.191 | 11.073 | 215.571 | 249.11 | 260.895 | 780.474