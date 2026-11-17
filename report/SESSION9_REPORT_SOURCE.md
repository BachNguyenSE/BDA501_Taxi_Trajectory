# SOURCE FOR AI: BDA501 Session 9 Technical Readiness Report — Porto Taxi ETA

> Instructions for the AI writing the report: use ONLY the facts and numbers below. Every number was
> measured on the real full dataset unless marked otherwise. Keep [I] Implemented / [P] Proposed labels.
> Do not claim real-time, cluster deployment, or high accuracy. Write in the Session 9 lab-manual structure
> (sections 1–10, Challenge Card F). Language: Vietnamese, technical terms may stay in English.

## 0. Team entry
- Project: Taxi Travel-Time, Congestion & Route Estimation from GPS Trajectories
- Members: Nguyễn Thanh Hoàng · Nguyễn Xuân Bách
- Dataset: Porto taxi trajectories, ECML/PKDD 2015 (UCI #339, CC BY 4.0), train.csv 1.94 GB,
  1,710,670 trips, 83,409,386 GPS points, 448 taxis, 2013-07-01 → 2014-06-30, one fix every 15 s
- NYC TLC 2025 (24.1M trips) was evaluated and REJECTED: zone IDs only, no GPS path (architecture correction)
- Task (corrected scope): **known-destination ETA** — user gives A, B, departure time → predicted travel
  time + likely congested zones + suggested route (sequence of 0.005° grid zones)
- Challenge card: CARD F (Porto Taxi GPS Trip Duration & ETA)

## 1. Architecture — [I] implemented / [P] proposed (Deliverable A)
Lanes: Sources → Ingestion → Storage → Processing → Serving
| Component | Status | Detail |
|---|---|---|
| Porto CSV source | [I] | scripts/download_data.sh |
| Live taxi fleet GPS feed | [P] design-only | dashed box |
| Data lake: data/raw (CSV) + HDFS copy + data/curated (Parquet) | [I] | Parquet tables: zones, zone_hour_speed, zone_graph_edges, trip_features |
| Hadoop MapReduce on YARN + HDFS (Hadoop 3.5.0, Streaming) | [I] | **cross-check only**, not a serving path |
| Spark ETL (PySpark 4.0.1) | [I] | src/etl/porto_etl.py |
| Spark SQL analytics + query plans + join experiment | [I] | src/analytics/spark_sql_analytics.py |
| Spark ML (Linear baseline vs GBT) + sklearn GBT for serving | [I] | src/ml/train_travel_time.py; models in outputs/models/ |
| Kafka 3.9.1 topic `porto.gps.pings`, 3 partitions, key = TAXI_ID | [I] | lives in the STORAGE lane (durable log) |
| Replay producer (train.csv → Kafka + injected junk) | [I] | **replay, not live**; src/streaming/producer.py |
| Spark Structured Streaming (watermark 10 min · dedup · 15-min windows) | [I] | src/streaming/live_zone_speed.py |
| MongoDB 8.0.15 (batch tables + zone_speed_live) | [I] | src/serving/load_mongo.py |
| FastAPI + Leaflet UI (localhost:8000) | [I] | map picks A/B; route shown as schematic zone grid |
| Multi-node cluster / cloud | [P] design-only | everything runs in Docker Compose on ONE host (12 vCPU, 8 GB VM) |

Arrows: batch (ETL, retrain) vs streaming (replay, 15-min windows) vs cross-check (MapReduce↔Spark SQL).
Boundary: batch builds the historical prior and trains the model; streaming only writes fresh
observations to a SEPARATE collection; nothing in streaming trains a model (no online learning).
Live layer only flags zones "slower than usual"; it never changes the ETA.

Data flow of one item: CSV row (trip) → Spark explodes POLYLINE into fixes → 15-s segments (both fixes in
bbox, ≤150 km/h, zone = segment midpoint, hour = segment start, Europe/Lisbon) → Parquet aggregates →
MongoDB → API (Dijkstra on zone graph + GBT prediction) → UI.

One-technology deletion test (Kafka): remove it and the streaming job could read the CSV directly; demo still
works, but lost: offset-based recovery, producer/consumer decoupling, multiple independent consumers
(we run 2 queries on the same topic). Throughput is NOT the reason: 448 taxis ≈ 30 msg/s.

## 2. MapReduce (Hadoop Streaming on YARN/HDFS) — measured
- Key = grid row-col-hour; value = (1, km, 15 s); combiner = same reducer (sums are associative);
  average speed computed only at the end (distance-weighted).
- Sample (20k trips): 14 s, 940,231 segments, combiner 940,231 → 25,666 records, 16,251 keys.
- Full (1.71M trips): **68 s**, 15 map + 2 reduce tasks, 80,025,093 segments emitted,
  combiner 80,844,635 → 1,216,102 records (−98.5%), shuffle 11.6 MB, 40,618 keys.
  Counters: segments_gps_jump 199,852; segments_outside_bbox 1,478,713; trips_used 1,674,152.
- Cross-check vs independent Spark SQL with identical semantics: **PASS** on sample and full —
  40,618/40,618 keys, segment counts exact on every key; km differs ≤ n_segments × 1e-6 km
  (max 2.26e-3 km on a key; max relative 5.2e-7). Root cause measured: Python vs Spark haversine agree to
  1 ulp (2.2e-16); the mapper's 6-decimal text serialisation has mean bias +1.19e-8 km/segment.
- Bugs found by actually running it: (1) Python `import core` failed on YARN because Streaming symlinks
  -files into the task dir but Python resolves the symlink → fixed with sys.path; (2) float `//` vs
  Spark FLOOR(a/b) could differ on grid lines → switched to floor(a/b).

## 3. Performance & scalability
### 3.1 Controlled experiment (B_benchmark) — measured
Q3 = trip_features (1,647,347 rows) JOIN zone_hour_speed (72,142 rows). Factor: spark.sql.autoBroadcastJoinThreshold.
| variant | join | median runtime | shuffle write | stages |
|---|---|---|---|---|
| default | BroadcastHashJoin | 0.29 s | 0.48 MB | 4 |
| broadcast disabled (-1) | SortMergeJoin | 0.33 s | 36.55 MB | 9 |
Interpretation: broadcast avoids shuffling the 1.6M-row side (76× less shuffle); runtime gap small because
it is one machine with data in memory — on a cluster shuffle crosses the network. Stops scaling when the small
side no longer fits executor memory (broadcast threshold). Limitation: single host, cannot prove cluster scaling.
Second scale data point: MapReduce sample 14 s vs full 68 s for ~85× more data (fixed job overhead dominates small input).
### 3.2 Query-plan audit (C_query_plan.txt)
Broadcast plan: BroadcastExchange + BroadcastHashJoin, one Exchange hashpartitioning(origin_zone, 64) for GROUP BY.
SortMergeJoin plan: two extra Exchange hashpartitioning on both join sides. AQE on, 64 shuffle partitions.
Partitioning determined by join/group keys; cache not useful for one-shot queries; Parquet columnar reads only needed columns.

## 4. Validation
### 4.1 Data processing (D_data_validation) — all PASS, measured
- Schema: explicit RAW_SCHEMA, no inferSchema
- Nulls on key columns of curated tables: 0
- Duplicates: 8 duplicate TRIP_IDs in trip_features (0.0005%) kept — known gap; 0 duplicate keys elsewhere
- Row counts: raw 1,710,670 → ETL 1,673,305 → trip_features 1,647,347; MapReduce 1,674,152 − ETL 1,673,305
  = 847 = exactly the trips > 180 min removed by ETL
- Join unmatched keys: 16 of 1,647,347 trips (0.00%); 52 zone_hour_speed zones are midpoint-only cells
- Independent check: MapReduce vs Spark SQL PASS
- Read-back: Parquet counts = MongoDB counts (zones 2,152; zone_hour_speed 72,142; edges 10,308)
- Data-quality finding: MISSING_DATA flags only 10 trips, but 174,912 trips (10.22%) contain a >150 km/h
  jump (223,060 segments) → internal GPS gaps not caught; label (n_points−1)×15 s under-states those trips.
- ETL bug fixed: points outside bbox were filtered BEFORE lag(), joining non-consecutive fixes as 15 s.
### 4.2 ML (random 80/20 split, seed 42, full data) — measured
Features (all available before departure): straight-line km, hour, day of week, weekend flag, bearing.
| model | RMSE min | MAE min | R² |
|---|---|---|---|
| naive mean (12.01 min) | 9.07 | 5.45 | — |
| Spark Linear Regression | 8.22 | 4.35 | 0.20 |
| Spark GBT (maxIter 60, depth 6, 1.65M rows) | 7.95 | 4.05 | 0.25 |
| sklearn Linear | 8.01 | 4.30 | 0.19 |
| sklearn GBT (150 trees, depth 4) — SERVED | 7.80 | 4.02 | 0.24 |
GBT beats naive mean by 26% MAE / 14% RMSE but R² 0.24 is weak (median trip 10.25 min).

### 4.2b Temporal hold-out (Card F test F2) — measured (evidence/session9/E_model_validation.md)
Split: train = trips starting before 2014-05-01 UTC (1,343,537), test = 2014-05-01 → 2014-06-30 (303,810).
All baseline statistics computed on TRAIN only. Error = predicted − actual.
| model | MAE min | RMSE min | mean signed error | median abs % error | % over-estimated | % under-estimated |
|---|---|---|---|---|---|---|
| naive mean (train) | 5.40 | 9.07 | −0.05 | 37.2% | 62.2% | 37.8% |
| historical speed: straight_km / median speed(hour, weekend) | 5.46 | 9.91 | +0.48 | 31.3% | 51.8% | 48.2% |
| linear regression | 4.35 | 8.16 | +0.16 | 29.7% | 66.4% | 33.6% |
| GBT (HistGradientBoosting, full train period) | **4.02** | **7.87** | +0.07 | **25.5%** | 65.2% | 34.8% |
Interpretation:
- GBT beats the historical-speed baseline by 26% MAE on future months → the model is useful.
- Temporal MAE 4.02 = random-split MAE 4.02 → no sign of time leakage or drift within 2013–2014.
- The simple "distance / typical speed" baseline is no better than the mean on MAE: straight-line
  distance is a poor proxy for the route actually driven.
- Errors are skewed: ~65% of trips over-estimated by a little, a minority of long trips under-estimated by a
  lot (mean signed error ≈ 0) → RMSE ≫ MAE.
- Note: the temporal GBT is HistGradientBoostingRegressor (same family as the served sklearn GBT, faster);
  the exact single-threaded sklearn GBT was too slow for the deadline.
### 4.3 Evidence vs claim
- Claim: pipeline runs end-to-end on 1.7M trips → evidence: etl_stats.json, Mongo counts → does NOT prove cluster scale.
- Claim: MapReduce correct → key-by-key PASS vs Spark SQL → does NOT prove MapReduce is faster/needed.
- Claim: ETA beats naive → MAE 4.02 vs 5.45 → does NOT prove accurate ETA (R² 0.24, random split).

## 5. NoSQL & streaming
### 5.1 MongoDB access pattern
- zones (_id zone_id unique), zone_hour_speed (index zone_id,hour,is_weekend), zone_graph_edges (index from_zone):
  written once by batch loader; read in bulk at API start into networkx graph (latency choice).
- zone_speed_live: upserted by streaming, _id = zone_id|window_start (idempotent → effectively-once sink),
  index (window_end, zone_id); read PER REQUEST: route zones in the latest closed window.
- Trade-off: single node, no replica set; Mongo does NOT do routing ($near not used) — Dijkstra in Python.
- Measured read-back latency (evidence/session9/F_serving_stream.md):
  point read zone_hour_speed by (zone_id, hour, is_weekend), 200 random keys: p50 0.15 ms, p95 0.19 ms, plan IXSCAN;
  POST /estimate end-to-end (model + Dijkstra + live lookup), 50 OD pairs: p50 4.19 ms, p95 6.30 ms.
### 5.2 Streaming — measured (REPLAY of 2014-05-14 16:00–19:00 local, speedup 60×)
- 754 trips, 42,145 fixes, 42,617 messages; producer and consumer run as separate containers concurrently
- Event time = segment start; watermark 10 min; dropDuplicatesWithinWatermark(trip_id, seq); 15-min tumbling
  window per zone; update mode + foreachBatch upsert; checkpoint data/checkpoints/
| check | injected | observed |
|---|---|---|
| messages | 42,617 | 42,617 |
| malformed | 78 | 78 detected |
| duplicate copies | 394 | 394 dropped |
| late fixes (+30 min) | 189 | 186 dropped |
| segments aggregated | 41,019 expected | 41,022 |
- 87 micro-batches, max batch 998 ms; 3,328 zone-window docs, 668 zones.
- The 3 late fixes accepted: Spark guarantees only that data within the watermark is kept; since Spark 3.5 late
  rows are filtered with the PREVIOUS batch's watermark (batch 86: 18:10:26 UTC) and the 3 fixes (18:17–18:26)
  were after it, window [18:15,18:30) still open. Consumer lag observed (maxOffsetsPerTrigger 500 / 5 s ≈ 100 msg/s
  < producer ~180 msg/s).
- Latency justification: historical prior cannot see an incident 10 min ago; freshness bounded by replay rate.
- Limitation: live coverage sparse (few zones reach ≥5 segments per window).

## 6. Deployment design [P] / reproducibility [I]
- Design: Spark on YARN or Kubernetes (driver in cluster mode, executors scale with partitions), data on HDFS/S3,
  Kafka 3 brokers RF=3, Mongo replica set, checkpoints on durable storage, logs/metrics via Spark UI + YARN UI.
- Reproducibility: pinned versions (requirements.txt, Dockerfiles), seed 42, exact scripts:
  run_eda.sh, run_mapreduce.sh [--full], run_pipeline.sh, spark_sql_analytics, run_streaming.sh, run_demo.sh;
  13 unit tests pass; no credentials embedded.
- Limitations: weak R²; straight-line distance not route distance; label noise 10.22%; zone_hour_speed built from
  all periods (not a model feature, would leak if used); single host; data from 2013–2014; slowest zone
  (1.5 km/h at 41.2425,-8.6725) is likely the airport taxi queue — "slow" ≠ "congested".

## 7. Card F answers
- F1 Destination leakage: B is a user input at request time; in training it is the last GPS fix → task named
  known-destination ETA. Historical zone speeds are not model features.
- F1 table for 3 real test-period trips: evidence/session9/H_project_card.md (each feature labelled pre-departure /
  ambiguous / after completion).
- F2 Temporal baseline: GBT MAE 4.02 vs historical-speed baseline 5.46 and naive 5.40 on May–June 2014 (see 4.2b).
- F3 Execution truth (measured): one real trip from the test period, raw CSV row → Spark ETL → trip_features →
  POST /estimate: predicted **9.4 min** vs actual **10.50 min**, 8 ms, route 5 zones, 1 congested zone.
  Path: CSV → Spark ETL → Parquet → model → FastAPI [I]; Kafka is not on the ETA path. Caveat: the served model was
  trained on a random split that includes May–June, so this trip may have been seen; out-of-sample numbers = 4.2b.
- One-minute defense: yes, every model input (A, B, time) exists at booking time.

Card result: [x] Verified (F1, F2, F3 measured) — with one correction: task renamed known-destination ETA.
Evidence files: evidence/session9/E_model_validation.md, evidence/session9/H_project_card.md.

## 8. Final Technical Completeness Check (manual section 8)
| Required item | Ready? | Evidence |
|---|---|---|
| Problem & Big Data justification | [x] | 1.94 GB, 83.4M GPS points, 80M segments; NYC rejected for lack of GPS paths |
| End-to-end architecture | [x] | Section 1 table with [I]/[P]; live feed + cluster marked [P] |
| Project-related MapReduce | [x] | Hadoop Streaming on YARN/HDFS, combiner −98.5%, key-by-key PASS vs Spark SQL |
| PySpark DataFrames / Spark SQL | [x] | src/etl/porto_etl.py (explode, window lag, joins, aggregates); src/analytics |
| Curated Parquet / columnar output | [x] | 4 Parquet tables; columnar reads, not partitioned (small aggregates; trip_features 1.6M rows) |
| At least two analytical queries | [x] | Q1 peak bottlenecks, Q2 hourly speed profile, Q3 origin-zone join + formatted plans |
| Performance/scalability experiment | [x] | Broadcast vs SortMergeJoin (0.48 vs 36.55 MB shuffle); MapReduce sample 14 s vs full 68 s |
| Spark ML / large-scale analytics | [x] | Linear vs GBT, random + temporal hold-out, naive + historical-speed baselines |
| NoSQL serving | [x] | 4 collections, indexes, IXSCAN point read p50 0.15 ms, /estimate p50 4.19 ms |
| Streaming consideration | [x] | Kafka replay → Structured Streaming, watermark/dedup/window/checkpoint, ground-truth reconciliation |
| Cloud/cluster deployment design | [x] DESIGN REVIEW | Section 6 (not executed on a cluster) |
| Validation | [x] | D_data_validation: 7/7 PASS |
| Reproducibility | [x] | pinned images, seed 42, scripts, 13 unit tests |
| Final report + README + code package | [ ] | README updated; final report being written from this file |
| Team contribution statement | [ ] | to fill by the team (who owns ETL / MapReduce+streaming / ML+serving) |
| Demo evidence | [x] | localhost:8000 UI + F3 trace; screenshots to add |

Red-flag checklist (answer = NO for all):
- Unexplained technologies: NO — each has a role; MapReduce explicitly cross-check only.
- "Real-time" claim from replay: NO — labelled REPLAY, freshness bounded by replay rate.
- Target computed from features: NO — label is duration; features are A, B, time only.
- Split/features expose future: NO — temporal hold-out gives same MAE (4.02); zone speeds not used as features.
- Metric mismatch: NO — MAE/RMSE/signed error for regression.
- NoSQL justified only as "faster": NO — access patterns + indexes + measured latency.
- No controlled performance result: NO — join experiment measured.
- Cannot reproduce: NO — scripts from raw data.
- Implemented and proposed mixed: NO — [I]/[P] labels everywhere.

## 9. Session 10 Readiness Self-Assessment (manual section 9)
| Criterion | Weight | Self-score | Evidence | Gap / action |
|---|---|---|---|---|
| Accuracy & Correctness | 25 | 20 | MapReduce↔Spark SQL exact on 40,618 keys; 7/7 data checks; streaming reconciled to ground truth; 3 bugs found and fixed by running | ETA accuracy modest (R² 0.24, MAE 4.02 min); 10.22% noisy labels not yet filtered |
| Creativity & Problem-Solving | 15 | 12 | Known-destination framing; Lambda design (batch prior + live layer); junk injection to measure dedup/watermark; schematic zone-route UI | Routing is zone-level, not road-network |
| Completeness of Product | 25 | 21 | All required components implemented and evidenced, incl. MapReduce on YARN, Spark SQL, ML, MongoDB, Kafka streaming, UI | Cluster deployment design-only; final report + contribution statement pending |
| Presentation & Expression | 10 | 7 | Architecture with [I]/[P], evidence files A–I, demo UI | Slides and screenshots not done yet |
| Application of Theory | 20 | 17 | Combiner associativity, shuffle/Exchange in plans, broadcast vs SMJ, event-time vs processing time, watermark semantics (Spark 3.5 previous-batch rule), idempotent sink, temporal validation | CAP/replication discussed only as design |
| Timeliness & Requirements | 5 | 4 | Evidence naming per section 6.4; README run commands | Report delivered close to deadline |
| **Total** | **100** | **81** | | |

Self-assessed readiness total: **81 / 100**.

## Top three corrections before Session 10
| Priority | Weakness found | Concrete fix | Done? |
|---|---|---|---|
| 1 | 10.22% of labels under-stated by GPS gaps | Drop or flag trips with a >150 km/h jump when training; re-run temporal evaluation | [ ] |
| 2 | Straight-line distance is a weak feature (R² 0.24) | Add route-graph travel time as a feature, with graph weights built from TRAIN-period trips only (avoid leakage) | [ ] |
| 3 | No team contribution statement / screenshots | Fill ownership per component; screenshot UI, YARN UI, Spark UI, Mongo query | [ ] |

## Likely Q&A
| Question | Evidence-based answer |
|---|---|
| Where does destination B come from at prediction time? | User input; in training it is the last GPS fix, so the task is known-destination ETA, not destination prediction. |
| Is the system real-time? | No. Kafka carries a REPLAY of 2014-05-14; streaming flags zones, ETA is batch-trained. Live fleet feed is [P]. |
| Why MapReduce if Spark does the same? | It is a cross-check workload: identical semantics, key-by-key PASS on 40,618 keys, combiner cuts shuffle 98.5%. Not on the serving path. |
| Is the model useful with R² 0.24? | Yes vs baselines: temporal MAE 4.02 vs 5.46 (historical speed) and 5.40 (mean); limited by straight-line distance and label noise. |
| Why did 3 late events get through? | Spark guarantees only that rows within the watermark are kept; late rows are filtered with the previous batch's watermark (18:10:26) and the 3 fixes were after it. |

## Team sign-off
- [x] We can reproduce the main result from our documented steps.
- [x] We can distinguish implemented components from proposed/design-only components.
- [x] We have measured at least one performance/scalability factor.
- [x] We have validated our key analytical/model claims against evidence or a baseline.
- [ ] Every member can defend their attributed technical component (team to confirm).

## 10. Session 9 readiness slide (5 lines)
1. Architecture truth: NYC rejected; MapReduce = cross-check only; Kafka = replay; live feed & cluster [P].
2. Performance: broadcast vs sort-merge join → 0.48 MB vs 36.55 MB shuffle.
3. Validation: MapReduce↔Spark SQL 40,618/40,618 keys PASS; temporal hold-out GBT MAE 4.02 vs historical-speed baseline 5.46.
4. Operations: Kafka replay + watermark/dedup measured (394/394 dup, 186/189 late); Docker-reproducible.
5. Limitation: R² 0.24 and 10.22% of labels under-stated by GPS gaps.
