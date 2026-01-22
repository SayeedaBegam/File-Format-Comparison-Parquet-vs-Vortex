# Benchmark Report

- Input: `clouddb_project/MedPayment2_1.csv` (csv)
- Rows: **9153273**
- min_col: `npi`
- filter_col: `provider_type`
- select_cols: `npi, average_Medicare_allowed_amt, average_Medicare_payment_amt, average_submitted_chrg_amt, bene_day_srvc_cnt, bene_unique_cnt, line_srvc_cnt, nppes_provider_zip, stdev_Medicare_allowed_amt, stdev_Medicare_payment_amt, stdev_submitted_chrg_amt, Calculation_0820513143749095`

## parquet_zstd
- size_bytes: **431535370**
- compression_time_s: **4.797**
- full_scan_min median_ms: **30.87**
- random_access median_ms: **34.28**
- best_select_col: `nppes_provider_zip` (avg median_ms **32.70**)
- selectivity:
  - npi: 1%: 34.75ms, 10%: 32.57ms, 25%: 33.91ms, 50%: 34.30ms, 90%: 33.29ms
  - average_Medicare_allowed_amt: 1%: 40.84ms, 10%: 40.26ms, 25%: 40.26ms, 50%: 37.88ms, 90%: 37.47ms
  - average_Medicare_payment_amt: 1%: 40.17ms, 10%: 38.77ms, 25%: 36.90ms, 50%: 36.86ms, 90%: 36.02ms
  - average_submitted_chrg_amt: 1%: 39.29ms, 10%: 37.18ms, 25%: 36.21ms, 50%: 36.04ms, 90%: 35.89ms
  - bene_day_srvc_cnt: 1%: 34.34ms, 10%: 33.72ms, 25%: 34.83ms, 50%: 35.39ms, 90%: 33.91ms
  - bene_unique_cnt: 1%: 34.77ms, 10%: 35.30ms, 25%: 36.42ms, 50%: 35.76ms, 90%: 35.83ms
  - line_srvc_cnt: 1%: 33.47ms, 10%: 33.19ms, 25%: 34.15ms, 50%: 42.28ms, 90%: 34.60ms
  - nppes_provider_zip: 1%: 32.12ms, 10%: 32.81ms, 25%: 32.41ms, 50%: 32.86ms, 90%: 33.27ms
  - stdev_Medicare_allowed_amt: 1%: 40.91ms, 10%: 40.47ms, 25%: 37.79ms, 50%: 37.23ms, 90%: 38.05ms
  - stdev_Medicare_payment_amt: 1%: 56.88ms, 10%: 41.70ms, 25%: 39.63ms, 50%: 40.44ms, 90%: 39.48ms
  - stdev_submitted_chrg_amt: 1%: 38.85ms, 10%: 38.05ms, 25%: 39.99ms, 50%: 36.85ms, 90%: 34.86ms
  - Calculation_0820513143749095: 1%: 40.83ms, 10%: 37.95ms, 25%: 39.70ms, 50%: 37.59ms, 90%: 36.31ms

## vortex_default
- size_bytes: **640465108**
- compression_time_s: **17.234**
- full_scan_min median_ms: **8.77**
- random_access median_ms: **8.99**
- best_select_col: `npi` (avg median_ms **9.11**)
- selectivity:
  - npi: 1%: 9.49ms, 10%: 8.22ms, 25%: 9.91ms, 50%: 8.09ms, 90%: 9.82ms
  - average_Medicare_allowed_amt: 1%: 16.71ms, 10%: 18.39ms, 25%: 19.52ms, 50%: 17.48ms, 90%: 19.85ms
  - average_Medicare_payment_amt: 1%: 17.15ms, 10%: 17.92ms, 25%: 19.10ms, 50%: 21.62ms, 90%: 21.66ms
  - average_submitted_chrg_amt: 1%: 14.25ms, 10%: 16.28ms, 25%: 15.59ms, 50%: 17.09ms, 90%: 17.59ms
  - bene_day_srvc_cnt: 1%: 11.65ms, 10%: 9.67ms, 25%: 9.72ms, 50%: 9.40ms, 90%: 9.87ms
  - bene_unique_cnt: 1%: 10.13ms, 10%: 9.84ms, 25%: 9.78ms, 50%: 9.59ms, 90%: 10.10ms
  - line_srvc_cnt: 1%: 10.72ms, 10%: 11.86ms, 25%: 11.90ms, 50%: 11.85ms, 90%: 13.32ms
  - nppes_provider_zip: 1%: 10.41ms, 10%: 9.89ms, 25%: 9.87ms, 50%: 9.72ms, 90%: 9.75ms
  - stdev_Medicare_allowed_amt: 1%: 15.89ms, 10%: 16.20ms, 25%: 15.70ms, 50%: 16.21ms, 90%: 16.58ms
  - stdev_Medicare_payment_amt: 1%: 18.67ms, 10%: 18.45ms, 25%: 18.19ms, 50%: 18.19ms, 90%: 19.63ms
  - stdev_submitted_chrg_amt: 1%: 15.19ms, 10%: 15.85ms, 25%: 15.48ms, 50%: 15.52ms, 90%: 15.51ms
  - Calculation_0820513143749095: 1%: 15.87ms, 10%: 17.91ms, 25%: 18.71ms, 50%: 18.37ms, 90%: 19.29ms
