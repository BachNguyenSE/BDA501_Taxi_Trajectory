# Taxi Travel-Time, Congestion & Route Estimation from GPS Trajectories
BDA501 Big Data — Final Project (40%) · Team: Nguyễn Thanh Hoàng · Trần Thanh Long · Nguyễn Xuân Bách

The user enters **point A, point B and a departure time**; the system returns an **estimated travel
time**, the **areas likely to be congested** and a **suggested route** (sequence of areas). Everything is
learned from historical taxi GPS trajectories (~83 M GPS points) with Hadoop MapReduce, PySpark,
Spark SQL, Spark ML, Kafka + Structured Streaming and MongoDB.

## Status — [I] implemented & run · [P] proposed / design-only
| Step | Component | Status | Evidence |
|---|---|---|---|
| 0 | Dataset feasibility study (NYC TLC → Porto) | [I] | `evidence/eda/` |
| 1 | MapReduce zone×hour speed — Hadoop Streaming on YARN + HDFS (`src/mapreduce/`) | [I] sample + full; **cross-check only**, not a serving path | `evidence/mapreduce/` |
| 2 | Spark ETL → curated Parquet (zones, zone_hour_speed, zone_graph_edges, trip_features) | [I] full | `evidence/eda/etl_stats.json` |
| 3 | Spark SQL analytics + query plans + controlled join experiment (`src/analytics/`) | [I] full | `evidence/analytics/`, `evidence/session9/B_*, C_*` |
| 4 | Spark ML travel-time regression (Linear baseline vs GBT; sklearn GBT for serving) | [I] full | `evidence/ml/` |
| 5 | MongoDB serving + routing API + Leaflet UI (`src/serving/`, `static/`) | [I] | `localhost:8000` |
| 6 | Kafka replay producer → Structured Streaming → MongoDB `zone_speed_live` (`src/streaming/`) | [I] **replay** of 2014-05-14 16:00–19:00 | `evidence/streaming/` |
| — | Live taxi-fleet GPS feed | [P] design-only | — |
| — | Multi-node cluster / cloud deployment | [P] design-only (all services run in Docker on one host) | — |

Batch vs streaming boundary: batch (steps 1–4) builds the historical prior and trains the model;
streaming (step 6) only writes fresh observations to a separate collection. Nothing in the streaming
path trains a model or overwrites a batch table (no online learning).

## Run
```bash
./scripts/download_data.sh            # one-time: Porto CSV (1.9 GB) + NYC parquet
docker compose build spark            # one-time: Spark image (Python 3.12, JDK 17, PySpark 4.0.1)

./scripts/run_eda.sh                  # step 0  dataset profiling
./scripts/run_mapreduce.sh [--full]   # step 1  Hadoop Streaming on YARN/HDFS + Spark SQL cross-check
./scripts/run_pipeline.sh [--sample]  # steps 2, 4, 5a  ETL -> ML -> load MongoDB
docker compose run --rm --no-deps spark python -m src.analytics.spark_sql_analytics   # step 3
./scripts/run_streaming.sh            # step 6  Kafka replay -> Structured Streaming -> MongoDB
./scripts/run_demo.sh                 # step 5  FastAPI + UI at http://localhost:8000
docker run --rm -v "$PWD":/workspace -w /workspace bda501-taxi-spark:1.0 python -m pytest -q tests
```
Measured on a Docker Desktop VM (12 vCPU, 8 GB): MapReduce full 68 s (15 maps, 2 reduces);
streaming replay of 3 h at 60× ≈ 4 min. Run the Hadoop profile and Spark jobs one at a time — the VM
does not have memory for both.

## What each step proves (headline numbers)
- **MapReduce** — 80,025,093 segments → 40,618 (cell, hour) keys; the combiner cuts reducer input from
  80.8 M to 1.2 M records. An independent Spark SQL query with identical semantics agrees on **every key**
  (segment counts exact; km within n_segments × 1e-6 km, the bound set by 6-decimal text serialisation).
- **ETL ↔ MapReduce reconciliation** — 1,674,152 trips in MapReduce vs 1,673,305 in the ETL: the 847
  trips over 180 min that the ETL removes on purpose.
- **Spark SQL experiment** — Q3 (1.6 M trips ⋈ 72 k zone-hours): BroadcastHashJoin shuffles 0.48 MB vs
  36.55 MB for SortMergeJoin (2 extra Exchanges); runtime 0.29 s vs 0.33 s on one machine.
- **ML** — sklearn GBT MAE 4.02 min / RMSE 7.80 vs naive-mean baseline MAE 5.45 / RMSE 9.07; R² 0.24.
- **Streaming** — 42,617 messages: 78/78 malformed detected, 394/394 duplicates dropped,
  186/189 late fixes dropped by the 10-min watermark (3 got through: Spark only guarantees rows within
  the watermark are kept; rows later than it *may* be dropped — see `evidence/streaming/summary.md`).

## Datasets
| Dataset | Role | Source / licence |
|---|---|---|
| NYC TLC Yellow Taxi 2025-01..06 (24.1 M trips) | Original candidate — **rejected**: zone IDs only, no GPS path | nyc.gov TLC trip record data |
| Porto taxi trajectories, ECML/PKDD 2015 (1.71 M trips) | **Selected** — GPS fix every 15 s | UCI ML Repository #339, CC BY 4.0 |

Raw data is **not** committed (`data/raw/` is git-ignored); `scripts/download_data.sh` fetches it.

## Environment
- Docker Desktop (≥ 8 GB RAM). Pinned inside containers: Python 3.12, OpenJDK 17, PySpark 4.0.1
  (`requirements.txt`), Hadoop 3.5.0 (`docker/hadoop/`, same image as the course lab), MongoDB 8.0.15,
  Apache Kafka 3.9.1 (KRaft, single node, topic `porto.gps.pings`, 3 partitions, key = TAXI_ID)
- Compose profiles: default (spark, mongo, api) · `hadoop` (HDFS + YARN) · `streaming` (Kafka)
- Seed `42` (`conf/pipeline.yaml`, `PYTHONHASHSEED`); Spark session time zone UTC

## Layout
```
conf/pipeline.yaml          central config (paths, bbox, grid, seed, Spark, Mongo, streaming)
docker/{spark,hadoop}/      images; docker-compose.yml with hadoop/streaming profiles
src/eda/                    step 0 profiling
src/mapreduce/              step 1 mapper / combiner / reducer + Spark SQL cross-check
src/etl/                    step 2 Spark ETL
src/analytics/              step 3 Spark SQL queries, plans, join experiment
src/ml/                     step 4 Spark ML + sklearn
src/serving/                step 5 MongoDB loader, routing graph, FastAPI
src/streaming/              step 6 Kafka replay producer + Structured Streaming consumer
static/                     Leaflet UI
tests/                      unit tests (MapReduce contract, producer ground truth)
evidence/                   machine-readable outputs, logs, query plans
```
