# Streaming run — Kafka replay -> Structured Streaming -> MongoDB

Source: REPLAY of historical trips (['2014-05-14T16:00:00+01:00', '2014-05-14T19:00:00+01:00']), not a live feed.
Trigger `5 seconds`, watermark 10 minutes, tumbling window 15 minutes,
maxOffsetsPerTrigger 500. Measured from StreamingQueryProgress + MongoDB.

## Ground truth (producer) vs observed (consumer)
| check | producer | consumer |
|---|---|---|
| messages consumed (quality query) vs produced | 42,617 | 42,617 |
| malformed payloads detected vs injected | 78 | 78 |
| duplicate copies dropped by dropDuplicatesWithinWatermark vs injected | 394 | 394 |
| rows dropped as later than the 10 minutes watermark vs late fixes injected | 189 | 186 |
| segments aggregated into zone windows vs expected on-time valid segments | 41,019 | 41,022 |

## Live query
| micro_batches | input_rows | dropped_duplicates | max_batch_duration_ms | last_watermark |
|---|---|---|---|---|
| 87 | 42,617 | 394 | 998 | 2014-05-14T18:34:11.000Z |
Dropped by watermark per stateful operator: {'stateStoreSave': 0, 'dedupeWithinWatermark': 186}

Spark only guarantees one direction: rows within the watermark delay are never dropped; rows later
than it MAY be dropped. Since Spark 3.5 late rows are filtered with the watermark of the PREVIOUS
micro-batch (`watermark_used` below is the one used for state eviction). Measured case: the 3 accepted
late fixes (segment start 18:17:07, 18:22:11, 18:26:26 UTC) arrived in batch 87, whose late-row filter
was batch 86's watermark 18:10:26 — all three are after it, and their window [18:15, 18:30) was still open.

## Last 8 micro-batches (watermark used vs event-time span)
| batch | input_rows | event_min | event_max | watermark_used | dropped_late |
|---|---|---|---|---|---|
| 80 | 499 | 2014-05-14T17:23:32.000Z | 2014-05-14T18:00:40.000Z | 2014-05-14T17:48:25.000Z | 1 |
| 81 | 499 | 2014-05-14T17:30:54.000Z | 2014-05-14T18:03:05.000Z | 2014-05-14T17:50:40.000Z | 4 |
| 82 | 499 | 2014-05-14T17:28:54.000Z | 2014-05-14T18:05:59.000Z | 2014-05-14T17:53:05.000Z | 3 |
| 83 | 498 | 2014-05-14T17:37:59.000Z | 2014-05-14T18:09:38.000Z | 2014-05-14T17:55:59.000Z | 2 |
| 84 | 499 | 2014-05-14T17:32:55.000Z | 2014-05-14T18:14:09.000Z | 2014-05-14T17:59:38.000Z | 3 |
| 85 | 498 | 2014-05-14T17:35:50.000Z | 2014-05-14T18:20:26.000Z | 2014-05-14T18:04:09.000Z | 4 |
| 86 | 499 | 2014-05-14T17:42:15.000Z | 2014-05-14T18:39:56.000Z | 2014-05-14T18:10:26.000Z | 19 |
| 87 | 68 | 2014-05-14T18:17:07.000Z | 2014-05-14T18:44:11.000Z | 2014-05-14T18:29:56.000Z | 0 |

## MongoDB `zone_speed_live`
| collection | zone_window_docs | segments | distinct_zones | window_range_utc |
|---|---|---|---|---|
| zone_speed_live | 3,328 | 41,022 | 668 | ['2014-05-14 15:00:00', '2014-05-14 18:45:00'] |
