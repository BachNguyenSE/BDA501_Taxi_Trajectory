# F_serving_stream — MongoDB write + read-back (measured)

| collection | written by | documents | key | primary read |
|---|---|---|---|---|
| zones | src.serving.load_mongo (batch) | 2,152 | zone_id (unique index) | bulk read at API start |
| zone_hour_speed | src.serving.load_mongo (batch) | 72,142 | (zone_id, hour, is_weekend) index | bulk read at API start; point read below |
| zone_graph_edges | src.serving.load_mongo (batch) | 10,308 | from_zone index | bulk read -> networkx graph |
| zone_speed_live | src.streaming.live_zone_speed (stream, upsert) | 3,328 | _id = zone_id + window_start; (window_end, zone_id) index | per request: route zones in the latest closed window |

| read | p50_ms | p95_ms | plan |
|---|---|---|---|
| point read zone_hour_speed by (zone_id, hour, is_weekend), 200 random keys | 0.15 | 0.19 | IXSCAN |
| POST /estimate end-to-end (model + Dijkstra + live lookup), 50 OD pairs | 4.19 | 6.30 | batch tables in RAM; zone_speed_live queried per request |

Busiest live document read back: `{'_id': '-1718_8229|2014-05-14T16:00:00+00:00', 'avg_speed_kmh': 9.37312332861353, 'n_segments': 246, 'window_start': datetime.datetime(2014, 5, 14, 16, 0), 'zone_id': '-1718_8229'}`.
Streaming ground truth vs observed: see `evidence/streaming/summary.md` (42,617 messages, 78/78 malformed,
394/394 duplicates, 186/189 late dropped). Source is a REPLAY of 2014-05-14 16:00–19:00, not a live feed.
