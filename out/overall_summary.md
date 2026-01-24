# Overall Summary

- datasets: **4**

## Datasets
name | rows | input_rows | dropped_rows | input_size_mb
--- | --- | --- | --- | ---
CityMaxCapita_1 | 912,657 | 912,657 | 0 | 332.98
Euro2016_1 | 2,052,497 | n/a | n/a | n/a
IGlocations1_1 | 81,611 | 81,611 | 0 | 6.58
MedPayment2_1 | 9,153,273 | n/a | n/a | 1840.65

## Formats (Geomean)
format | comp_ratio | comp_time_s | size_mb | full_scan_ms | selective_pred_ms | random_access_ms
--- | --- | --- | --- | --- | --- | ---
parquet_snappy | 2.457 | 0.063 | 2.679 | 3.383 | 3.797 | 5.951
parquet_uncompressed | 1.569 | 0.087 | 4.195 | 3.942 | 3.457 | 4.347
parquet_zstd | 3.753 | 0.825 | 52.271 | 13.919 | 3.325 | 15.489
vortex_default | 2.635 | 1.903 | 77.732 | 4.153 | 3.173 | 4.521