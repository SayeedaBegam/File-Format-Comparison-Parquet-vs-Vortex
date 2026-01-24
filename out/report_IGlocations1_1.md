# Benchmark Report

- Input: `clouddb_project/IGlocations1_1.csv` (csv)
- Rows: **81611**
- Input rows: **81611**
- Dropped rows: **0**
- min_col: `column01`
- filter_col: `column04`
- select_cols: `column01, column00, column02, column04, column05, column08, column09, column10, column11, column12, column13, column14, column15, column16`
- recommendations:
  - storage-first: `parquet_zstd`
  - read-latency-first: `vortex_default`
  - scan-first: `parquet_zstd`

## parquet_zstd
- size_bytes: **2281299**
- compression_time_s: **0.073**
- compression_ratio: **3.026**
- encodings:
  - column00: RLE_DICTIONARY
  - column01: RLE_DICTIONARY
  - column02: RLE_DICTIONARY
  - column03: PLAIN
  - column04: RLE_DICTIONARY
  - column05: RLE_DICTIONARY
  - column06: RLE_DICTIONARY
  - column07: RLE_DICTIONARY
  - column08: RLE_DICTIONARY
  - column09: RLE_DICTIONARY
  - column10: RLE_DICTIONARY
  - column11: RLE_DICTIONARY
  - column12: RLE_DICTIONARY
  - column13: RLE_DICTIONARY
  - column14: RLE_DICTIONARY
  - column15: RLE_DICTIONARY
  - column16: RLE_DICTIONARY
  - column17: RLE_DICTIONARY
- full_scan_min median_ms: **3.25**
- selective_predicate median_ms: **3.56**
- point_lookup median_ms: **6.49**
- best_select_col: `column16` (avg median_ms **3.45**)
- validation_pass: **True**
- selectivity:
  - column01: 1%: 3.41ms, 10%: 3.36ms, 25%: 3.67ms, 50%: 3.65ms, 90%: 3.62ms
  - column00: 1%: 4.07ms, 10%: 3.88ms, 25%: 4.30ms, 50%: 4.32ms, 90%: 3.87ms
  - column02: 1%: 3.97ms, 10%: 3.91ms, 25%: 3.83ms, 50%: 4.17ms, 90%: 3.96ms
  - column04: 1%: 3.91ms, 10%: 3.41ms, 25%: 3.53ms, 50%: 3.56ms, 90%: 3.68ms
  - column05: 1%: 3.65ms, 10%: 3.84ms, 25%: 3.83ms, 50%: 3.96ms, 90%: 3.84ms
  - column08: 1%: 3.97ms, 10%: 3.83ms, 25%: 3.98ms, 50%: 3.94ms, 90%: 5.49ms
  - column09: 1%: 5.81ms, 10%: 5.71ms, 25%: 5.29ms, 50%: 6.07ms, 90%: 4.27ms
  - column10: 1%: 3.78ms, 10%: 3.75ms, 25%: 3.71ms, 50%: 3.89ms, 90%: 3.89ms
  - column11: 1%: 3.86ms, 10%: 4.01ms, 25%: 4.27ms, 50%: 3.81ms, 90%: 3.73ms
  - column12: 1%: 3.64ms, 10%: 3.88ms, 25%: 3.82ms, 50%: 3.84ms, 90%: 4.09ms
  - column13: 1%: 4.15ms, 10%: 3.74ms, 25%: 3.79ms, 50%: 3.87ms, 90%: 3.94ms
  - column14: 1%: 3.80ms, 10%: 3.50ms, 25%: 3.48ms, 50%: 3.49ms, 90%: 3.25ms
  - column15: 1%: 4.09ms, 10%: 3.59ms, 25%: 3.75ms, 50%: 3.55ms, 90%: 3.40ms
  - column16: 1%: 3.28ms, 10%: 3.22ms, 25%: 3.83ms, 50%: 3.41ms, 90%: 3.49ms

## vortex_default
- size_bytes: **2645528**
- compression_time_s: **0.106**
- compression_ratio: **2.610**
- encodings: unable to read vortex encodings: Type `u32` at position 1194 is unaligned.
	while verifying table field `max` at position 1194
	while verifying vector element 0 at position 24
	while verifying table field `field_stats` at position 16


- full_scan_min median_ms: **3.93**
- selective_predicate median_ms: **4.25**
- point_lookup median_ms: **3.59**
- best_select_col: `column01` (avg median_ms **3.92**)
- validation_pass: **True**
- selectivity:
  - column01: 1%: 4.14ms, 10%: 3.89ms, 25%: 3.76ms, 50%: 3.83ms, 90%: 3.98ms
  - column00: 1%: 3.84ms, 10%: 4.12ms, 25%: 4.19ms, 50%: 4.12ms, 90%: 4.41ms
  - column02: 1%: 4.15ms, 10%: 4.56ms, 25%: 4.29ms, 50%: 4.63ms, 90%: 3.87ms
  - column04: 1%: 4.37ms, 10%: 4.15ms, 25%: 4.12ms, 50%: 4.01ms, 90%: 3.98ms
  - column05: 1%: 4.33ms, 10%: 3.94ms, 25%: 4.01ms, 50%: 3.81ms, 90%: 4.48ms
  - column08: 1%: 4.48ms, 10%: 3.90ms, 25%: 4.55ms, 50%: 4.61ms, 90%: 4.58ms
  - column09: 1%: 4.58ms, 10%: 3.79ms, 25%: 4.62ms, 50%: 4.11ms, 90%: 4.23ms
  - column10: 1%: 4.02ms, 10%: 3.70ms, 25%: 4.48ms, 50%: 4.61ms, 90%: 4.98ms
  - column11: 1%: 4.22ms, 10%: 4.60ms, 25%: 3.78ms, 50%: 3.94ms, 90%: 3.88ms
  - column12: 1%: 4.16ms, 10%: 3.74ms, 25%: 3.93ms, 50%: 4.00ms, 90%: 4.06ms
  - column13: 1%: 4.61ms, 10%: 4.05ms, 25%: 4.58ms, 50%: 3.86ms, 90%: 3.77ms
  - column14: 1%: 4.12ms, 10%: 4.35ms, 25%: 3.97ms, 50%: 3.84ms, 90%: 4.36ms
  - column15: 1%: 3.89ms, 10%: 4.21ms, 25%: 3.91ms, 50%: 4.13ms, 90%: 3.96ms
  - column16: 1%: 4.17ms, 10%: 3.89ms, 25%: 4.08ms, 50%: 4.22ms, 90%: 3.97ms
