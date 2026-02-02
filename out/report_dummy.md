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
    - reason: highest decompression_speed_mb_s 1.40
  - read-latency-first: `vortex_default`
    - reason: lowest random_access median_ms 1.55
  - scan-first: `vortex_default`
    - reason: lowest full_scan_min median_ms 2.55

## duckdb_table
- size_mb: **0.00**
- compression_time_s: **0.000**
- compression_ratio: **1.000**
- full_scan_min median_ms: **0.19** (p95 **0.20**, cold **0.34**)
- selective_predicate median_ms: **0.20** (p95 **0.22**, cold **0.24**)
- random_access median_ms: **0.18** (p95 **0.19**, cold **0.21**)
- best_select_col: `score` (avg median_ms **0.20**)
- selectivity:
  - id: 1%: 0.22ms, 10%: 0.21ms, 25%: 0.21ms, 50%: 0.21ms, 90%: 0.21ms
  - score: 1%: 0.20ms, 10%: 0.20ms, 25%: 0.19ms, 50%: 0.20ms, 90%: 0.21ms
- like_predicates:
  - name prefix target 1%,10%,25%,50%,90% (actual 33.33%) `car%`: 0.19ms
  - name suffix target 1%,10%,25%,50%,90% (actual 33.33%) `%ice`: 0.19ms
  - name contains target 1%,10%,25%,50%,90% (actual 33.33%) `%lic%`: 0.18ms
- like_summary:
  - contains: avg median_ms **0.18** (n=1)
  - prefix: avg median_ms **0.19** (n=1)
  - suffix: avg median_ms **0.19** (n=1)

## parquet_zstd
- size_mb: **0.00**
- compression_time_s: **0.007**
- compression_speed_mb_s: **0.010**
- decompression_time_s: **0.003**
- decompression_speed_mb_s: **0.165**
- compression_ratio: **0.120**
- encodings:
  - id: PLAIN
  - name: PLAIN
  - active: PLAIN
  - score: PLAIN
- full_scan_min median_ms: **3.23** (p95 **4.09**, cold **3.50**)
- selective_predicate median_ms: **3.25** (p95 **4.14**, cold **4.59**)
- random_access median_ms: **3.12** (p95 **3.31**, cold **3.40**)
- best_select_col: `score` (avg median_ms **3.20**)
- validation_pass: **True**
- selectivity:
  - id: 1%: 3.10ms, 10%: 4.08ms, 25%: 3.90ms, 50%: 3.37ms, 90%: 3.29ms
  - score: 1%: 3.13ms, 10%: 3.11ms, 25%: 3.15ms, 50%: 3.29ms, 90%: 3.30ms
- like_predicates:
  - name prefix target 1%,10%,25%,50%,90% (actual 33.33%) `car%`: 3.26ms
  - name suffix target 1%,10%,25%,50%,90% (actual 33.33%) `%ice`: 3.31ms
  - name contains target 1%,10%,25%,50%,90% (actual 33.33%) `%lic%`: 3.30ms
- like_summary:
  - contains: avg median_ms **3.30** (n=1)
  - prefix: avg median_ms **3.26** (n=1)
  - suffix: avg median_ms **3.31** (n=1)

## parquet_snappy
- size_mb: **0.00**
- compression_time_s: **0.007**
- compression_speed_mb_s: **0.009**
- decompression_time_s: **0.004**
- decompression_speed_mb_s: **0.126**
- compression_ratio: **0.126**
- encodings:
  - id: PLAIN
  - name: PLAIN
  - active: PLAIN
  - score: PLAIN
- full_scan_min median_ms: **3.30** (p95 **4.10**, cold **2.66**)
- selective_predicate median_ms: **3.40** (p95 **3.43**, cold **3.39**)
- random_access median_ms: **3.49** (p95 **3.53**, cold **3.50**)
- best_select_col: `score` (avg median_ms **3.38**)
- validation_pass: **True**
- selectivity:
  - id: 1%: 3.44ms, 10%: 3.42ms, 25%: 3.43ms, 50%: 3.43ms, 90%: 3.48ms
  - score: 1%: 3.38ms, 10%: 3.59ms, 25%: 3.26ms, 50%: 3.40ms, 90%: 3.27ms
- like_predicates:
  - name prefix target 1%,10%,25%,50%,90% (actual 33.33%) `car%`: 4.13ms
  - name suffix target 1%,10%,25%,50%,90% (actual 33.33%) `%ice`: 5.33ms
  - name contains target 1%,10%,25%,50%,90% (actual 33.33%) `%lic%`: 3.25ms
- like_summary:
  - contains: avg median_ms **3.25** (n=1)
  - prefix: avg median_ms **4.13** (n=1)
  - suffix: avg median_ms **5.33** (n=1)

## parquet_uncompressed
- size_mb: **0.00**
- compression_time_s: **0.007**
- compression_speed_mb_s: **0.010**
- decompression_time_s: **0.004**
- decompression_speed_mb_s: **0.150**
- compression_ratio: **0.126**
- encodings:
  - id: PLAIN
  - name: PLAIN
  - active: PLAIN
  - score: PLAIN
- full_scan_min median_ms: **3.33** (p95 **3.41**, cold **3.30**)
- selective_predicate median_ms: **3.35** (p95 **4.09**, cold **3.97**)
- random_access median_ms: **3.35** (p95 **3.49**, cold **4.36**)
- best_select_col: `id` (avg median_ms **3.44**)
- validation_pass: **True**
- selectivity:
  - id: 1%: 3.40ms, 10%: 3.45ms, 25%: 3.43ms, 50%: 3.51ms, 90%: 3.41ms
  - score: 1%: 3.39ms, 10%: 3.44ms, 25%: 3.42ms, 50%: 3.50ms, 90%: 3.47ms
- like_predicates:
  - name prefix target 1%,10%,25%,50%,90% (actual 33.33%) `car%`: 3.45ms
  - name suffix target 1%,10%,25%,50%,90% (actual 33.33%) `%ice`: 3.51ms
  - name contains target 1%,10%,25%,50%,90% (actual 33.33%) `%lic%`: 3.49ms
- like_summary:
  - contains: avg median_ms **3.49** (n=1)
  - prefix: avg median_ms **3.45** (n=1)
  - suffix: avg median_ms **3.51** (n=1)

## vortex_default
- size_mb: **0.01**
- compression_time_s: **0.009**
- compression_speed_mb_s: **0.007**
- decompression_time_s: **0.004**
- decompression_speed_mb_s: **1.401**
- compression_ratio: **0.013**
- encodings: unable to read vortex encodings: Type `u32` at position 330 is unaligned.
	while verifying table field `max` at position 330
	while verifying vector element 0 at position 24
	while verifying table field `field_stats` at position 16


- full_scan_min median_ms: **2.55** (p95 **2.58**, cold **2.75**)
- selective_predicate median_ms: **2.95** (p95 **3.03**, cold **3.26**)
- random_access median_ms: **1.55** (p95 **1.61**, cold **1.90**)
- best_select_col: `id` (avg median_ms **2.56**)
- validation_pass: **True**
- selectivity:
  - id: 1%: 2.60ms, 10%: 2.59ms, 25%: 2.52ms, 50%: 2.57ms, 90%: 2.53ms
  - score: 1%: 2.82ms, 10%: 2.82ms, 25%: 2.82ms, 50%: 2.77ms, 90%: 2.81ms
- like_predicates:
  - name prefix target 1%,10%,25%,50%,90% (actual 33.33%) `car%`: 2.51ms
  - name suffix target 1%,10%,25%,50%,90% (actual 33.33%) `%ice`: 2.43ms
  - name contains target 1%,10%,25%,50%,90% (actual 33.33%) `%lic%`: 2.54ms
- like_summary:
  - contains: avg median_ms **2.54** (n=1)
  - prefix: avg median_ms **2.51** (n=1)
  - suffix: avg median_ms **2.43** (n=1)
