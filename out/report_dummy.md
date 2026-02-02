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
  - compression-speed-first: `parquet_zstd`
    - reason: highest compression_speed_mb_s 0.01
  - decompression-speed-first: `vortex_default`
    - reason: highest decompression_speed_mb_s 1.35
  - read-latency-first: `vortex_default`
    - reason: lowest random_access median_ms 1.62
  - scan-first: `vortex_default`
    - reason: lowest full_scan_min median_ms 2.58

## duckdb_table
- size_mb: **0.00**
- compression_time_s: **0.000**
- compression_ratio: **1.000**
- full_scan_min median_ms: **0.18** (p95 **0.19**, cold **0.20**)
- selective_predicate median_ms: **0.20** (p95 **0.20**, cold **0.21**)
- random_access median_ms: **0.17** (p95 **0.17**, cold **0.20**)
- best_select_col: `score` (avg median_ms **0.19**)
- selectivity:
  - id: 1%: 0.22ms, 10%: 0.22ms, 25%: 0.22ms, 50%: 0.20ms, 90%: 0.21ms
  - score: 1%: 0.19ms, 10%: 0.19ms, 25%: 0.20ms, 50%: 0.19ms, 90%: 0.19ms
- like_predicates:
  - name prefix target 1%,10%,25%,50%,90% (actual 33.33%) `ali%`: 0.17ms
  - name suffix target 1%,10%,25%,50%,90% (actual 33.33%) `%ice`: 0.18ms
  - name contains target 1%,10%,25%,50%,90% (actual 33.33%) `%bob%`: 0.18ms
- like_summary:
  - contains: avg median_ms **0.18** (n=1)
  - prefix: avg median_ms **0.17** (n=1)
  - suffix: avg median_ms **0.18** (n=1)

## parquet_zstd
- size_mb: **0.00**
- compression_time_s: **0.007**
- compression_speed_mb_s: **0.009**
- decompression_time_s: **0.004**
- decompression_speed_mb_s: **0.128**
- compression_ratio: **0.120**
- encodings:
  - id: PLAIN
  - name: PLAIN
  - active: PLAIN
  - score: PLAIN
- full_scan_min median_ms: **3.18** (p95 **4.31**, cold **2.60**)
- selective_predicate median_ms: **3.31** (p95 **4.09**, cold **4.06**)
- random_access median_ms: **3.25** (p95 **3.34**, cold **3.30**)
- best_select_col: `id` (avg median_ms **3.34**)
- validation_pass: **True**
- selectivity:
  - id: 1%: 3.22ms, 10%: 3.28ms, 25%: 3.35ms, 50%: 3.38ms, 90%: 3.45ms
  - score: 1%: 3.34ms, 10%: 3.37ms, 25%: 3.37ms, 50%: 3.34ms, 90%: 3.55ms
- like_predicates:
  - name prefix target 1%,10%,25%,50%,90% (actual 33.33%) `ali%`: 3.37ms
  - name suffix target 1%,10%,25%,50%,90% (actual 33.33%) `%ice`: 3.32ms
  - name contains target 1%,10%,25%,50%,90% (actual 33.33%) `%bob%`: 4.61ms
- like_summary:
  - contains: avg median_ms **4.61** (n=1)
  - prefix: avg median_ms **3.37** (n=1)
  - suffix: avg median_ms **3.32** (n=1)

## parquet_snappy
- size_mb: **0.00**
- compression_time_s: **0.007**
- compression_speed_mb_s: **0.009**
- decompression_time_s: **0.003**
- decompression_speed_mb_s: **0.165**
- compression_ratio: **0.126**
- encodings:
  - id: PLAIN
  - name: PLAIN
  - active: PLAIN
  - score: PLAIN
- full_scan_min median_ms: **3.44** (p95 **3.52**, cold **2.59**)
- selective_predicate median_ms: **3.45** (p95 **4.46**, cold **3.55**)
- random_access median_ms: **3.57** (p95 **3.73**, cold **4.66**)
- best_select_col: `score` (avg median_ms **3.45**)
- validation_pass: **True**
- selectivity:
  - id: 1%: 3.52ms, 10%: 3.41ms, 25%: 3.46ms, 50%: 3.46ms, 90%: 3.56ms
  - score: 1%: 3.55ms, 10%: 3.34ms, 25%: 3.48ms, 50%: 3.50ms, 90%: 3.40ms
- like_predicates:
  - name prefix target 1%,10%,25%,50%,90% (actual 33.33%) `ali%`: 3.34ms
  - name suffix target 1%,10%,25%,50%,90% (actual 33.33%) `%ice`: 3.19ms
  - name contains target 1%,10%,25%,50%,90% (actual 33.33%) `%bob%`: 3.25ms
- like_summary:
  - contains: avg median_ms **3.25** (n=1)
  - prefix: avg median_ms **3.34** (n=1)
  - suffix: avg median_ms **3.19** (n=1)

## parquet_uncompressed
- size_mb: **0.00**
- compression_time_s: **0.008**
- compression_speed_mb_s: **0.009**
- decompression_time_s: **0.004**
- decompression_speed_mb_s: **0.148**
- compression_ratio: **0.126**
- encodings:
  - id: PLAIN
  - name: PLAIN
  - active: PLAIN
  - score: PLAIN
- full_scan_min median_ms: **3.43** (p95 **3.71**, cold **3.94**)
- selective_predicate median_ms: **3.35** (p95 **3.54**, cold **3.43**)
- random_access median_ms: **3.56** (p95 **4.27**, cold **4.47**)
- best_select_col: `score` (avg median_ms **3.11**)
- validation_pass: **True**
- selectivity:
  - id: 1%: 3.39ms, 10%: 3.35ms, 25%: 3.31ms, 50%: 3.40ms, 90%: 3.46ms
  - score: 1%: 3.35ms, 10%: 3.24ms, 25%: 3.30ms, 50%: 3.19ms, 90%: 2.46ms
- like_predicates:
  - name prefix target 1%,10%,25%,50%,90% (actual 33.33%) `ali%`: 3.18ms
  - name suffix target 1%,10%,25%,50%,90% (actual 33.33%) `%ice`: 3.21ms
  - name contains target 1%,10%,25%,50%,90% (actual 33.33%) `%bob%`: 3.27ms
- like_summary:
  - contains: avg median_ms **3.27** (n=1)
  - prefix: avg median_ms **3.18** (n=1)
  - suffix: avg median_ms **3.21** (n=1)

## vortex_default
- size_mb: **0.01**
- compression_time_s: **0.009**
- compression_speed_mb_s: **0.007**
- decompression_time_s: **0.004**
- decompression_speed_mb_s: **1.352**
- compression_ratio: **0.013**
- encodings: unable to read vortex encodings: Type `u32` at position 330 is unaligned.
	while verifying table field `max` at position 330
	while verifying vector element 0 at position 24
	while verifying table field `field_stats` at position 16


- full_scan_min median_ms: **2.58** (p95 **2.64**, cold **2.66**)
- selective_predicate median_ms: **3.00** (p95 **3.08**, cold **3.09**)
- random_access median_ms: **1.62** (p95 **1.75**, cold **1.67**)
- best_select_col: `id` (avg median_ms **2.63**)
- validation_pass: **True**
- selectivity:
  - id: 1%: 2.60ms, 10%: 2.54ms, 25%: 2.68ms, 50%: 2.59ms, 90%: 2.75ms
  - score: 1%: 2.86ms, 10%: 2.91ms, 25%: 2.91ms, 50%: 2.92ms, 90%: 2.89ms
- like_predicates:
  - name prefix target 1%,10%,25%,50%,90% (actual 33.33%) `ali%`: 2.60ms
  - name suffix target 1%,10%,25%,50%,90% (actual 33.33%) `%ice`: 2.61ms
  - name contains target 1%,10%,25%,50%,90% (actual 33.33%) `%bob%`: 2.62ms
- like_summary:
  - contains: avg median_ms **2.62** (n=1)
  - prefix: avg median_ms **2.60** (n=1)
  - suffix: avg median_ms **2.61** (n=1)
