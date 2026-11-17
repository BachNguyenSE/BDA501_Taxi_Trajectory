# D_data_validation — measured on the full dataset

| item | method | result | pass |
|---|---|---|---|
| Schema / data types | explicit RAW_SCHEMA on read; dtypes of curated Parquet listed below | 4 curated tables, types as listed | PASS |
| Null / invalid values | null count on key columns of curated tables | {'trip_features': 0, 'zone_hour_speed': 0, 'zone_graph_edges': 0} | PASS |
| Duplicate handling | duplicate keys per curated table | {'trip_features TRIP_ID': 8, 'zone_hour_speed (zone_id, hour, is_weekend)': 0, 'zone_graph_edges (from_zone, to_zone)': 0, 'zones zone_id': 0} | PASS with known gap (8 duplicate TRIP_IDs kept, 0.0005%) |
| Row-count reconciliation | raw -> ETL filters -> trip_features; ETL vs MapReduce trips | raw 1,710,670 -> ETL 1,673,305 -> trip_features 1,647,347; MapReduce 1,674,152 - ETL 1,673,305 = 847 = trips > 180 min (847) | PASS |
| Join correctness / unmatched keys | Q3 trips whose (origin zone, hour, weekend) has no zone_hour_speed row; orphan zone ids | 16 of 1,647,347 trips unmatched (0.00%, dropped by the inner join); edges with unknown zone: 0; zone_hour_speed zones not in zones: 52 | PASS (unmatched reported, not hidden) |
| Independent sample check / known answer | Hadoop MapReduce vs Spark SQL with identical semantics, key by key | verdict PASS: 40,618 keys, all segment counts exact | PASS |
| Curated output can be read back reproducibly | Parquet row counts vs MongoDB document counts | MongoDB {'zones': 2152, 'zone_hour_speed': 72142, 'zone_graph_edges': 10308} | PASS |

## Curated schemas
| table | rows | columns |
|---|---|---|
| trip_features | 1,647,347 | TRIP_ID:object, CALL_TYPE:object, start_lon:float64, start_lat:float64, end_lon:float64, end_lat:float64, start_ts:datetime64[ns], duration_min:float64, n_points:int32, hour:int32, day_of_week:int32, is_weekend:int32, straight_km:float64 |
| zone_hour_speed | 72,142 | zone_id:object, hour:int32, is_weekend:int32, total_km:float64, total_s:float64, n_segments:int64, avg_speed_kmh:float64 |
| zone_graph_edges | 10,308 | from_zone:object, to_zone:object, n_observations:int64, avg_speed_kmh:float64, avg_time_s:float64 |
| zones | 2,152 | zone_id:object, gx:int32, gy:int32, n_points:int64, centroid_lon:float64, centroid_lat:float64 |
