# B_benchmark — controlled experiment: join strategy for Q3

**Question.** Q3 joins `trip_features` (1,647,347 rows) with `zone_hour_speed`
(72,142 rows). Does broadcasting the small side beat a sort-merge join?
**Factor changed:** `spark.sql.autoBroadcastJoinThreshold` (default vs -1). Everything else fixed:
same curated Parquet (full dataset), same session, one warm-up then 3 timed runs per variant.
**Expected before running:** broadcast avoids shuffling the 1.6M-row side, so less shuffle and lower runtime.
Environment: Docker Desktop VM 12 vCPU / 8 GB, Spark 4.0.1 `local[*]`, AQE on. Measured.

| variant | join_strategy | runtime_s_median | runtime_s_runs | shuffle_write_MB | shuffle_read_MB | stages |
|---|---|---|---|---|---|---|
| broadcast_allowed (default) | BroadcastHashJoin | 0.29 | [0.326, 0.284, 0.295] | 0.48 | 0.48 | 4 |
| broadcast_disabled | SortMergeJoin | 0.33 | [0.334, 0.327, 0.316] | 36.55 | 36.55 | 9 |

Query text and both formatted plans: `C_query_plan.txt`.
