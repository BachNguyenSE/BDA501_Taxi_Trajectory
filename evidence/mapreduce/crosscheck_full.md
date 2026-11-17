# MapReduce cross-check (full) — verdict: **PASS**

Input: `data/raw/porto/train.csv`. Hadoop Streaming job on YARN/HDFS (`src/mapreduce/`) vs an independent
Spark SQL query with identical semantics (query text in `crosscheck_full.json`).
Measured, not inferred.

## Key-by-key comparison (key = grid row, grid col, local hour)
| keys_in_both | n_segments_exact | total_km_within_tol | max_abs_km_diff | max_abs_speed_diff_kmh | keys_mapreduce | keys_spark_sql | keys_only_in_mapreduce | keys_only_in_spark_sql | max_rel_km_diff |
|---|---|---|---|---|---|---|---|---|---|
| 40,618 | 40,618 | 40,618 | 2.26e-03 | 6.00e-04 | 40,618 | 40,618 | 0 | 0 | 0.00 |

Segment counts must match exactly. total_km may differ by at most n_segments x 1e-06 km
per key: the mapper writes every segment's km as text with 6 decimals (error <= 5e-7 km) and each
combine re-writes partial sums the same way. Measured separately: Python and Spark haversine agree
to 1 ulp; the 6-decimal rounding has a mean bias of about +1.2e-8 km per segment, which is why
MapReduce totals are slightly higher on the busiest keys.

## Hadoop counters vs Spark SQL
| check | hadoop | spark_sql |
|---|---|---|
| segments emitted by mapper vs segments kept by Spark SQL | 80,025,093 | 80,025,093 |
| segments in reducer output (sum n_segments) | 80,025,093 | 80,025,093 |
| combiner records in -> out (shuffle reduction) | 80844635 -> 1216102 | n/a |
| reduce output records (= distinct keys) | 40,618 | 40,618 |

## Hadoop job counters (group `porto` + framework)
| counter | value |
|---|---|
| map_input_records | 1,710,671 |
| map_output_records | 80,025,093 |
| segments_gps_jump | 199,852 |
| segments_outside_bbox | 1,478,713 |
| segments_total | 81,703,658 |
| skip_header | 1 |
| skip_lt_2_points | 36,508 |
| skip_missing_data | 10 |
| trips_used | 1,674,152 |
| Map input records | 1,710,671 |
| Map output records | 80,025,093 |
| Combine input records | 80,844,635 |
| Combine output records | 1,216,102 |
| Reduce input records | 396,560 |
| Reduce output records | 40,618 |
| Reduce shuffle bytes | 11,597,361 |
| Launched map tasks | 15 |
| Launched reduce tasks | 2 |

## Five busiest keys side by side
| row | col | hour | mr_n | spark_n | mr_km | spark_km | mr_speed | spark_speed |
|---|---|---|---|---|---|---|---|---|
| 19 | 37 | 15 | 193,618 | 193,618 | 8,173.62 | 8,173.61 | 10.13 | 10.13 |
| 19 | 37 | 16 | 192,177 | 192,177 | 7,657.43 | 7,657.42 | 9.56 | 9.56 |
| 19 | 37 | 11 | 191,943 | 191,943 | 8,633.06 | 8,633.05 | 10.79 | 10.79 |
| 19 | 37 | 17 | 186,017 | 186,017 | 7,337.64 | 7,337.63 | 9.47 | 9.47 |
| 19 | 37 | 4 | 176,886 | 176,886 | 10,777.28 | 10,777.28 | 14.62 | 14.62 |

## Five keys with the largest total_km difference
| row | col | hour | mr_n | spark_n | mr_km | spark_km | mr_speed | spark_speed | abs_km_diff | rel_km_diff |
|---|---|---|---|---|---|---|---|---|---|---|
| 19 | 36 | 16 | 95,074 | 95,074 | 4,392.74 | 4,392.73 | 11.09 | 11.09 | 0.00 | 0.00 |
| 19 | 37 | 15 | 193,618 | 193,618 | 8,173.62 | 8,173.61 | 10.13 | 10.13 | 0.00 | 0.00 |
| 19 | 36 | 11 | 102,451 | 102,451 | 5,172.00 | 5,172.00 | 12.12 | 12.12 | 0.00 | 0.00 |
| 19 | 37 | 17 | 186,017 | 186,017 | 7,337.64 | 7,337.63 | 9.47 | 9.47 | 0.00 | 0.00 |
| 19 | 42 | 16 | 91,274 | 91,274 | 4,926.08 | 4,926.08 | 12.95 | 12.95 | 0.00 | 0.00 |
