# Benchmark Report

- Input: `/home/utn/ozil43oh/sem_3/cloud_db/clouddb_project/File-Format-Comparison-Parquet-vs-Vortex/out/uploads/dummy.csv` (csv)
- Rows: **3**
- Input rows: **3**
- Dropped rows: **0**
- Input size: **0.00 MB**
- column_type_counts: numeric=2, text=1, date=0, bool=1, other=0
- min_col: `id`
- filter_col: `name`
- select_cols: `id, score`
- ndv_ratio_by_type: numeric=1.000, text=1.000, bool=0.667
- ndv_ratio_top_cols:
  - id: 1.000
  - name: 1.000
  - score: 1.000
  - active: 0.667
- recommendations:
  - storage-first: `parquet_snappy`
    - reason: highest compression_ratio 0.126
  - compression-speed-first: `parquet_uncompressed`
    - reason: highest compression_speed_mb_s 0.02
  - decompression-speed-first: `vortex_default`
    - reason: highest decompression_speed_mb_s 1.85
  - read-latency-first: `parquet_snappy`
    - reason: lowest random_access median_ms 0.29
  - scan-first: `parquet_snappy`
    - reason: lowest full_scan_min median_ms 0.28

## duckdb_table
- size_mb: **0.00**
- compression_time_s: **0.000**
- compression_ratio: **1.000**
- full_scan_min median_ms: **0.18** (p95 **0.19**, cold **0.21**)
- selective_predicate median_ms: **0.20** (p95 **0.21**, cold **0.23**)
- random_access median_ms: **0.17** (p95 **0.17**, cold **0.21**)
- best_select_col: `score` (avg median_ms **0.20**)
- selectivity:
  - id: 1%: 0.21ms, 10%: 0.21ms, 25%: 0.21ms, 50%: 0.20ms, 90%: 0.20ms
  - score: 1%: 0.20ms, 10%: 0.20ms, 25%: 0.20ms, 50%: 0.20ms, 90%: 0.21ms
- like_predicates:
  - name prefix target 1%,10%,25%,50%,90% (actual 33.33%) `ali%`: 0.21ms
  - name suffix target 1%,10%,25%,50%,90% (actual 33.33%) `%bob`: 0.18ms
  - name contains target 1%,10%,25%,50%,90% (actual 33.33%) `%bob%`: 0.19ms
- like_summary:
  - contains: avg median_ms **0.19** (n=1)
  - prefix: avg median_ms **0.21** (n=1)
  - suffix: avg median_ms **0.18** (n=1)

## parquet_zstd
- size_mb: **0.00**
- compression_time_s: **0.004**
- compression_speed_mb_s: **0.015**
- decompression_time_s: **0.000**
- decompression_speed_mb_s: **1.174**
- compression_ratio: **0.120**
- encodings:
  - id: PLAIN
  - name: PLAIN
  - active: PLAIN
  - score: PLAIN
- full_scan_min median_ms: **0.28** (p95 **0.30**, cold **0.43**)
- selective_predicate median_ms: **0.36** (p95 **0.38**, cold **0.33**)
- random_access median_ms: **0.35** (p95 **0.37**, cold **0.45**)
- best_select_col: `id` (avg median_ms **0.33**)
- validation_pass: **True**
- selectivity:
  - id: 1%: 0.33ms, 10%: 0.31ms, 25%: 0.33ms, 50%: 0.33ms, 90%: 0.34ms
  - score: 1%: 0.32ms, 10%: 0.32ms, 25%: 0.33ms, 50%: 0.31ms, 90%: 0.38ms
- like_predicates:
  - name prefix target 1%,10%,25%,50%,90% (actual 33.33%) `ali%`: 0.32ms
  - name suffix target 1%,10%,25%,50%,90% (actual 33.33%) `%bob`: 0.31ms
  - name contains target 1%,10%,25%,50%,90% (actual 33.33%) `%bob%`: 0.30ms
- like_summary:
  - contains: avg median_ms **0.30** (n=1)
  - prefix: avg median_ms **0.32** (n=1)
  - suffix: avg median_ms **0.31** (n=1)

## parquet_snappy
- size_mb: **0.00**
- compression_time_s: **0.004**
- compression_speed_mb_s: **0.016**
- decompression_time_s: **0.000**
- decompression_speed_mb_s: **1.114**
- compression_ratio: **0.126**
- encodings:
  - id: PLAIN
  - name: PLAIN
  - active: PLAIN
  - score: PLAIN
- full_scan_min median_ms: **0.28** (p95 **0.32**, cold **0.29**)
- selective_predicate median_ms: **0.31** (p95 **0.37**, cold **0.34**)
- random_access median_ms: **0.29** (p95 **0.29**, cold **0.32**)
- best_select_col: `id` (avg median_ms **0.33**)
- validation_pass: **True**
- selectivity:
  - id: 1%: 0.32ms, 10%: 0.32ms, 25%: 0.35ms, 50%: 0.34ms, 90%: 0.32ms
  - score: 1%: 0.34ms, 10%: 0.36ms, 25%: 0.32ms, 50%: 0.32ms, 90%: 0.33ms
- like_predicates:
  - name prefix target 1%,10%,25%,50%,90% (actual 33.33%) `ali%`: 0.31ms
  - name suffix target 1%,10%,25%,50%,90% (actual 33.33%) `%bob`: 0.30ms
  - name contains target 1%,10%,25%,50%,90% (actual 33.33%) `%bob%`: 0.32ms
- like_summary:
  - contains: avg median_ms **0.32** (n=1)
  - prefix: avg median_ms **0.31** (n=1)
  - suffix: avg median_ms **0.30** (n=1)

## parquet_uncompressed
- size_mb: **0.00**
- compression_time_s: **0.004**
- compression_speed_mb_s: **0.016**
- decompression_time_s: **0.000**
- decompression_speed_mb_s: **1.221**
- compression_ratio: **0.126**
- encodings:
  - id: PLAIN
  - name: PLAIN
  - active: PLAIN
  - score: PLAIN
- full_scan_min median_ms: **0.31** (p95 **0.31**, cold **0.32**)
- selective_predicate median_ms: **0.32** (p95 **0.34**, cold **0.37**)
- random_access median_ms: **0.29** (p95 **0.35**, cold **0.41**)
- best_select_col: `score` (avg median_ms **0.31**)
- validation_pass: **True**
- selectivity:
  - id: 1%: 0.33ms, 10%: 0.41ms, 25%: 0.36ms, 50%: 0.33ms, 90%: 0.33ms
  - score: 1%: 0.32ms, 10%: 0.30ms, 25%: 0.31ms, 50%: 0.31ms, 90%: 0.30ms
- like_predicates:
  - name prefix target 1%,10%,25%,50%,90% (actual 33.33%) `ali%`: 0.31ms
  - name suffix target 1%,10%,25%,50%,90% (actual 33.33%) `%bob`: 0.29ms
  - name contains target 1%,10%,25%,50%,90% (actual 33.33%) `%bob%`: 0.30ms
- like_summary:
  - contains: avg median_ms **0.30** (n=1)
  - prefix: avg median_ms **0.31** (n=1)
  - suffix: avg median_ms **0.29** (n=1)

## vortex_default
- size_mb: **0.01**
- compression_time_s: **0.020**
- compression_speed_mb_s: **0.003**
- decompression_time_s: **0.003**
- decompression_speed_mb_s: **1.849**
- compression_ratio: **0.013**
- encodings: unable to read vortex encodings: Type `u32` at position 330 is unaligned.
	while verifying table field `max` at position 330
	while verifying vector element 0 at position 24
	while verifying table field `field_stats` at position 16


- full_scan_min median_ms: **1.68** (p95 **1.70**, cold **1.72**)
- selective_predicate median_ms: **1.94** (p95 **1.96**, cold **3.09**)
- random_access median_ms: **0.62** (p95 **0.63**, cold **0.70**)
- best_select_col: `id` (avg median_ms **1.61**)
- validation_pass: **True**
- selectivity:
  - id: 1%: 1.55ms, 10%: 1.72ms, 25%: 1.54ms, 50%: 1.62ms, 90%: 1.65ms
  - score: 1%: 1.84ms, 10%: 1.84ms, 25%: 1.79ms, 50%: 1.81ms, 90%: 1.80ms
- like_predicates:
  - name prefix target 1%,10%,25%,50%,90% (actual 33.33%) `ali%`: 1.55ms
  - name suffix target 1%,10%,25%,50%,90% (actual 33.33%) `%bob`: 1.46ms
  - name contains target 1%,10%,25%,50%,90% (actual 33.33%) `%bob%`: 1.55ms
- like_summary:
  - contains: avg median_ms **1.55** (n=1)
  - prefix: avg median_ms **1.55** (n=1)
  - suffix: avg median_ms **1.46** (n=1)
