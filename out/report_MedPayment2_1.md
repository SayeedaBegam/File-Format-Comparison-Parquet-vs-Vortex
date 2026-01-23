# Benchmark Report

- Input: `clouddb_project/MedPayment2_1.csv` (csv)
- Rows: **9153273**
- min_col: `npi`
- filter_col: `provider_type`
- select_cols: `npi, average_Medicare_allowed_amt, average_Medicare_payment_amt, average_submitted_chrg_amt, bene_day_srvc_cnt, bene_unique_cnt, line_srvc_cnt, nppes_provider_zip, stdev_Medicare_allowed_amt, stdev_Medicare_payment_amt, stdev_submitted_chrg_amt, Calculation_0820513143749095`
- recommendations:
  - storage-first: `parquet_zstd`
  - read-latency-first: `vortex_default`
  - scan-first: `vortex_default`

## parquet_zstd
- size_bytes: **431523420**
- compression_time_s: **4.725**
- compression_ratio: **4.473**
- full_scan_min median_ms: **41.14**
- random_access median_ms: **39.19**
- best_select_col: `nppes_provider_zip` (avg median_ms **34.39**)
- validation_pass: **True**
- selectivity:
  - npi: 1%: 37.96ms, 10%: 36.66ms, 25%: 33.86ms, 50%: 33.84ms, 90%: 34.08ms
  - average_Medicare_allowed_amt: 1%: 39.38ms, 10%: 40.71ms, 25%: 38.84ms, 50%: 39.39ms, 90%: 40.08ms
  - average_Medicare_payment_amt: 1%: 40.17ms, 10%: 40.26ms, 25%: 41.30ms, 50%: 41.17ms, 90%: 39.59ms
  - average_submitted_chrg_amt: 1%: 42.34ms, 10%: 41.46ms, 25%: 42.04ms, 50%: 41.11ms, 90%: 38.24ms
  - bene_day_srvc_cnt: 1%: 37.11ms, 10%: 36.50ms, 25%: 36.34ms, 50%: 36.48ms, 90%: 35.75ms
  - bene_unique_cnt: 1%: 33.93ms, 10%: 34.71ms, 25%: 35.06ms, 50%: 34.48ms, 90%: 36.51ms
  - line_srvc_cnt: 1%: 34.16ms, 10%: 35.42ms, 25%: 34.19ms, 50%: 36.51ms, 90%: 38.22ms
  - nppes_provider_zip: 1%: 35.42ms, 10%: 34.48ms, 25%: 33.23ms, 50%: 34.27ms, 90%: 34.53ms
  - stdev_Medicare_allowed_amt: 1%: 41.54ms, 10%: 38.69ms, 25%: 37.64ms, 50%: 37.81ms, 90%: 37.64ms
  - stdev_Medicare_payment_amt: 1%: 39.53ms, 10%: 40.76ms, 25%: 39.51ms, 50%: 40.05ms, 90%: 40.53ms
  - stdev_submitted_chrg_amt: 1%: 41.14ms, 10%: 40.70ms, 25%: 40.74ms, 50%: 40.67ms, 90%: 42.56ms
  - Calculation_0820513143749095: 1%: 46.17ms, 10%: 44.35ms, 25%: 44.32ms, 50%: 44.50ms, 90%: 40.01ms

## vortex_default
- size_bytes: **640465108**
- compression_time_s: **17.363**
- compression_ratio: **3.014**
- full_scan_min median_ms: **6.51**
- random_access median_ms: **7.05**
- best_select_col: `npi` (avg median_ms **7.25**)
- validation_pass: **True**
- selectivity:
  - npi: 1%: 7.25ms, 10%: 7.24ms, 25%: 7.05ms, 50%: 7.34ms, 90%: 7.36ms
  - average_Medicare_allowed_amt: 1%: 14.52ms, 10%: 16.81ms, 25%: 17.65ms, 50%: 16.82ms, 90%: 16.97ms
  - average_Medicare_payment_amt: 1%: 15.39ms, 10%: 17.19ms, 25%: 18.36ms, 50%: 19.40ms, 90%: 20.07ms
  - average_submitted_chrg_amt: 1%: 13.35ms, 10%: 15.32ms, 25%: 15.04ms, 50%: 16.58ms, 90%: 16.67ms
  - bene_day_srvc_cnt: 1%: 10.75ms, 10%: 9.87ms, 25%: 10.03ms, 50%: 10.15ms, 90%: 10.33ms
  - bene_unique_cnt: 1%: 10.07ms, 10%: 9.33ms, 25%: 9.45ms, 50%: 9.59ms, 90%: 9.60ms
  - line_srvc_cnt: 1%: 10.68ms, 10%: 12.13ms, 25%: 12.00ms, 50%: 11.99ms, 90%: 12.78ms
  - nppes_provider_zip: 1%: 10.52ms, 10%: 9.72ms, 25%: 10.11ms, 50%: 10.13ms, 90%: 10.07ms
  - stdev_Medicare_allowed_amt: 1%: 16.84ms, 10%: 15.65ms, 25%: 15.78ms, 50%: 15.81ms, 90%: 16.11ms
  - stdev_Medicare_payment_amt: 1%: 18.27ms, 10%: 18.54ms, 25%: 17.98ms, 50%: 18.92ms, 90%: 19.93ms
  - stdev_submitted_chrg_amt: 1%: 15.15ms, 10%: 15.11ms, 25%: 15.19ms, 50%: 15.66ms, 90%: 16.17ms
  - Calculation_0820513143749095: 1%: 15.32ms, 10%: 16.99ms, 25%: 18.28ms, 50%: 19.04ms, 90%: 19.65ms
