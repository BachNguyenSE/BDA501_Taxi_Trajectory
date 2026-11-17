# MapReduce cross-check (sample) — verdict: **PASS**

Input: `data/sample/porto_sample.csv`. Hadoop Streaming job on YARN/HDFS (`src/mapreduce/`) vs an independent
Spark SQL query with identical semantics (query text in `crosscheck_sample.json`).
Measured, not inferred.

## Key-by-key comparison (key = grid row, grid col, local hour)
| keys_in_both | n_segments_exact | total_km_within_tol | max_abs_km_diff | max_abs_speed_diff_kmh | keys_mapreduce | keys_spark_sql | keys_only_in_mapreduce | keys_only_in_spark_sql | max_rel_km_diff |
|---|---|---|---|---|---|---|---|---|---|
| 16,251 | 16,251 | 16,251 | 7.01e-05 | 5.99e-04 | 16,251 | 16,251 | 0 | 0 | 0.00 |

Segment counts must match exactly. total_km may differ by at most n_segments x 1e-06 km
per key: the mapper writes every segment's km as text with 6 decimals (error <= 5e-7 km) and each
combine re-writes partial sums the same way. Measured separately: Python and Spark haversine agree
to 1 ulp; the 6-decimal rounding has a mean bias of about +1.2e-8 km per segment, which is why
MapReduce totals are slightly higher on the busiest keys.

## Hadoop counters vs Spark SQL
| check | hadoop | spark_sql |
|---|---|---|
| segments emitted by mapper vs segments kept by Spark SQL | 940,231 | 940,231 |
| segments in reducer output (sum n_segments) | 940,231 | 940,231 |
| combiner records in -> out (shuffle reduction) | 940231 -> 25666 | n/a |
| reduce output records (= distinct keys) | 16,251 | 16,251 |

## Hadoop job counters (group `porto` + framework)
| counter | value |
|---|---|
| map_input_records | 20,001 |
| map_output_records | 940,231 |
| segments_gps_jump | 2,292 |
| segments_outside_bbox | 17,308 |
| segments_total | 959,831 |
| skip_header | 1 |
| skip_lt_2_points | 471 |
| trips_used | 19,529 |
| Map input records | 20,001 |
| Map output records | 940,231 |
| Combine input records | 940,231 |
| Combine output records | 25,666 |
| Reduce input records | 25,666 |
| Reduce output records | 16,251 |
| Reduce shuffle bytes | 728,307 |
| Launched map tasks | 2 |
| Launched reduce tasks | 2 |

## Five busiest keys side by side
| row | col | hour | mr_n | spark_n | mr_km | spark_km | mr_speed | spark_speed |
|---|---|---|---|---|---|---|---|---|
| 19 | 37 | 11 | 2,774 | 2,774 | 109.18 | 109.18 | 9.45 | 9.45 |
| 19 | 37 | 16 | 2,451 | 2,451 | 98.88 | 98.88 | 9.68 | 9.68 |
| 19 | 37 | 15 | 2,370 | 2,370 | 100.84 | 100.84 | 10.21 | 10.21 |
| 19 | 37 | 10 | 2,251 | 2,251 | 111.50 | 111.50 | 11.89 | 11.89 |
| 19 | 37 | 17 | 2,192 | 2,192 | 85.16 | 85.16 | 9.32 | 9.32 |

## Five keys with the largest total_km difference
| row | col | hour | mr_n | spark_n | mr_km | spark_km | mr_speed | spark_speed | abs_km_diff | rel_km_diff |
|---|---|---|---|---|---|---|---|---|---|---|
| 19 | 42 | 12 | 1,480 | 1,480 | 76.93 | 76.93 | 12.47 | 12.48 | 0.00 | 0.00 |
| 19 | 36 | 16 | 1,322 | 1,322 | 49.88 | 49.88 | 9.06 | 9.06 | 0.00 | 0.00 |
| 20 | 34 | 17 | 1,328 | 1,328 | 87.30 | 87.30 | 15.78 | 15.78 | 0.00 | 0.00 |
| 19 | 37 | 16 | 2,451 | 2,451 | 98.88 | 98.88 | 9.68 | 9.68 | 0.00 | 0.00 |
| 23 | 40 | 17 | 587 | 587 | 25.18 | 25.18 | 10.29 | 10.29 | 0.00 | 0.00 |
