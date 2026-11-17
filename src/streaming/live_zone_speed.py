"""Step 6b — Spark Structured Streaming: Kafka GPS fixes -> live zone speed in MongoDB (speed layer).

Two independent queries read the same topic, each with its own checkpoint/offsets:
  quality   stateless: per micro-batch message count and malformed (unparseable) count
  live      event-time pipeline:
              parse -> withWatermark(segment start, 10 min)
                    -> dropDuplicatesWithinWatermark(trip_id, seq)   (Kafka is at-least-once)
                    -> keep segments with both fixes in the bbox and <= 150 km/h
                    -> window(15 min tumbling) x zone of the segment midpoint
                    -> sum km / sum s  (distance-weighted speed, same rule as the batch layer)
            update mode + foreachBatch idempotent upsert keyed by (zone_id, window_start), so a
            micro-batch replayed after a crash rewrites the same documents (effectively-once sink).
The batch layer (zone_hour_speed) is never modified, and no model is trained here: this job
only produces fresh observations for the serving layer to compare with the historical prior.

Run (inside the spark container, kafka + mongo up):
    python -m src.streaming.live_zone_speed --reset                      # drain the topic, then stop
    python -m src.streaming.live_zone_speed --trigger "5 seconds" --idle-stop-seconds 60   # live
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from datetime import datetime, timezone

from pymongo import ASCENDING, UpdateOne
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.evidence import md_table, write_json
from src.common.mongo import get_db
from src.common.spark_session import get_spark, load_config, project_path
from src.eda.porto_profile import haversine_km

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
PING_SCHEMA = T.StructType([
    T.StructField("taxi_id", T.StringType()), T.StructField("trip_id", T.StringType()),
    T.StructField("seq", T.IntegerType()), T.StructField("event_ts", T.LongType()),
    T.StructField("lon", T.DoubleType()), T.StructField("lat", T.DoubleType()),
    T.StructField("prev_lon", T.DoubleType()), T.StructField("prev_lat", T.DoubleType()),
    T.StructField("s_since_prev", T.DoubleType()),
])


def build_queries(spark, cfg: dict, trigger: dict, checkpoint_root: str):
    sc, pc = cfg["streaming"], cfg["porto"]
    bbox, grid = pc["bbox"], cfg["etl"]["grid_deg"]
    db = get_db()
    live_coll, quality_coll = db[sc["live_coll"]], db[sc["quality_coll"]]

    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", sc["bootstrap"]))
              .option("subscribe", sc["topic"])
              .option("startingOffsets", "earliest")
              .option("maxOffsetsPerTrigger", sc["max_offsets_per_trigger"])
              .option("failOnDataLoss", "false")
              .load())
    parsed = source.select(F.from_json(F.col("value").cast("string"), PING_SCHEMA).alias("e"))
    is_valid = (F.col("e.trip_id").isNotNull() & F.col("e.seq").isNotNull() & F.col("e.event_ts").isNotNull()
                & F.col("e.lon").isNotNull() & F.col("e.lat").isNotNull())

    # ---------- query 1: data quality (stateless) ----------
    def write_quality(df, batch_id: int) -> None:
        r = df.agg(F.count("*").alias("messages"),
                   F.sum((~F.coalesce(is_valid, F.lit(False))).cast("int")).alias("malformed")).first()
        if r["messages"]:
            quality_coll.update_one({"_id": batch_id}, {"$set": {
                "messages": r["messages"], "malformed": r["malformed"] or 0,
                "at": datetime.now(timezone.utc)}}, upsert=True)

    quality = (parsed.writeStream.queryName("quality").foreachBatch(write_quality)
               .option("checkpointLocation", f"{checkpoint_root}/quality").trigger(**trigger).start())

    # ---------- query 2: live zone speed (event time, watermark, dedup, window) ----------
    seg_km = haversine_km("array(prev_lon, prev_lat)", "array(lon, lat)")
    inside = lambda lon, lat: (F.col(lon).between(bbox["lon_min"], bbox["lon_max"])  # noqa: E731
                               & F.col(lat).between(bbox["lat_min"], bbox["lat_max"]))
    segments = (
        parsed.filter(is_valid).select("e.*")
        # event time of a segment = when it STARTED (same convention as the batch layer)
        .withColumn("event_time", F.timestamp_seconds(F.col("event_ts") - F.coalesce("s_since_prev", F.lit(0.0))))
        .withWatermark("event_time", sc["watermark"])
        .dropDuplicatesWithinWatermark(["trip_id", "seq"])
        .filter(F.col("prev_lon").isNotNull() & inside("lon", "lat") & inside("prev_lon", "prev_lat"))
        .withColumn("km", F.expr(seg_km))
        .filter(F.col("km") * 3600.0 / F.col("s_since_prev") <= pc["max_segment_speed_kmh"])
        .withColumn("zone_id", F.concat_ws(
            "_",
            F.floor((F.col("lon") + F.col("prev_lon")) / 2 / grid).cast("int"),
            F.floor((F.col("lat") + F.col("prev_lat")) / 2 / grid).cast("int")))
    )
    windowed = (segments.groupBy(F.window("event_time", sc["window"]), "zone_id")
                .agg(F.sum("km").alias("total_km"), F.sum("s_since_prev").alias("total_s"),
                     F.count("*").alias("n_segments"), F.approx_count_distinct("taxi_id").alias("n_taxis")))

    def upsert_live(df, batch_id: int) -> None:
        rows = df.collect()  # only the zone-windows updated in this micro-batch
        if not rows:
            return
        now = datetime.now(timezone.utc)
        ops = []
        for r in rows:
            ws = r["window"]["start"].replace(tzinfo=timezone.utc)
            we = r["window"]["end"].replace(tzinfo=timezone.utc)
            ops.append(UpdateOne({"_id": f"{r['zone_id']}|{ws.isoformat()}"}, {"$set": {
                "zone_id": r["zone_id"], "window_start": ws, "window_end": we,
                "total_km": r["total_km"], "total_s": r["total_s"], "n_segments": r["n_segments"],
                "n_taxis": r["n_taxis"], "avg_speed_kmh": r["total_km"] * 3600.0 / r["total_s"],
                "batch_id": batch_id, "updated_at": now}}, upsert=True))
        live_coll.bulk_write(ops, ordered=False)

    live = (windowed.writeStream.queryName("live_zone_speed").outputMode("update").foreachBatch(upsert_live)
            .option("checkpointLocation", f"{checkpoint_root}/live_zone_speed").trigger(**trigger).start())
    return quality, live, db


def summarize_progress(progress: list[dict]) -> dict:
    out = {"micro_batches": 0, "input_rows": 0, "dropped_by_watermark": {}, "dropped_duplicates": 0,
           "max_batch_duration_ms": 0}
    for p in progress:
        if not p.get("numInputRows"):
            continue
        out["micro_batches"] += 1
        out["input_rows"] += p["numInputRows"]
        out["max_batch_duration_ms"] = max(out["max_batch_duration_ms"], p.get("durationMs", {}).get("triggerExecution", 0))
        for op in p.get("stateOperators", []):
            name = op.get("operatorName", "?")
            out["dropped_by_watermark"][name] = out["dropped_by_watermark"].get(name, 0) + op.get("numRowsDroppedByWatermark", 0)
            out["dropped_duplicates"] += op.get("customMetrics", {}).get("numDroppedDuplicateRows", 0)
    out["last_watermark"] = next((p["eventTime"].get("watermark") for p in reversed(progress)
                                  if p.get("eventTime", {}).get("watermark")), None)
    # Per micro-batch: the watermark a batch filters with is computed from EARLIER batches, so a
    # batch spanning more event time than the watermark delay can let late rows through.
    out["batches"] = [{"batch": p["batchId"], "input_rows": p["numInputRows"],
                       "event_min": p.get("eventTime", {}).get("min"), "event_max": p.get("eventTime", {}).get("max"),
                       "watermark_used": p.get("eventTime", {}).get("watermark"),
                       "dropped_late": sum(op.get("numRowsDroppedByWatermark", 0) for op in p.get("stateOperators", []))}
                      for p in progress if p.get("numInputRows")]
    return out


def main() -> None:
    cfg = load_config()
    sc = cfg["streaming"]
    ap = argparse.ArgumentParser()
    ap.add_argument("--trigger", default="availableNow",
                    help='"availableNow" (drain the topic, then stop) or a processing-time interval like "5 seconds"')
    ap.add_argument("--idle-stop-seconds", type=int, default=60,
                    help="processing-time mode: stop after this long without new input")
    ap.add_argument("--reset", action="store_true", help="delete checkpoints and the live/quality collections first")
    args = ap.parse_args()

    checkpoint_root = str(project_path(sc["checkpoint_dir"]))
    db = get_db()
    if args.reset:
        shutil.rmtree(checkpoint_root, ignore_errors=True)
        db[sc["live_coll"]].drop()
        db[sc["quality_coll"]].drop()
    db[sc["live_coll"]].create_index([("window_end", ASCENDING), ("zone_id", ASCENDING)])

    spark = get_spark("streaming-live-zone-speed", extra_conf={
        "spark.jars.packages": KAFKA_PACKAGE,
        "spark.jars.ivy": str(project_path(".ivy2")),
        "spark.sql.shuffle.partitions": "6",
        "spark.sql.streaming.numRecentProgressUpdates": "5000",
    })
    trigger = {"availableNow": True} if args.trigger == "availableNow" else {"processingTime": args.trigger}
    t0 = time.time()
    quality, live, db = build_queries(spark, cfg, trigger, checkpoint_root)

    if args.trigger == "availableNow":
        quality.awaitTermination()
        live.awaitTermination()
    else:
        seen_data, idle_since = False, time.time()
        while live.isActive:
            time.sleep(5)
            lp = live.lastProgress
            if lp and lp.get("numInputRows"):
                seen_data, idle_since = True, time.time()
            elif seen_data and time.time() - idle_since > args.idle_stop_seconds:
                break
        quality.stop()
        live.stop()
    for q in (quality, live):
        if q.exception():
            raise RuntimeError(f"query {q.name} failed: {q.exception()}")

    live_stats = summarize_progress(live.recentProgress)
    quality_stats = summarize_progress(quality.recentProgress)
    qdocs = list(db[sc["quality_coll"]].find({}, {"_id": 0, "messages": 1, "malformed": 1}))
    agg = next(iter(db[sc["live_coll"]].aggregate([{"$group": {
        "_id": None, "docs": {"$sum": 1}, "segments": {"$sum": "$n_segments"},
        "zones": {"$addToSet": "$zone_id"}, "first": {"$min": "$window_start"}, "last": {"$max": "$window_end"}}}])), {})

    manifest_path = project_path("evidence/streaming/producer_manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    kinds = manifest.get("messages_by_kind", {})
    rows = [
        {"check": "messages consumed (quality query) vs produced",
         "producer": manifest.get("messages_total"), "consumer": sum(d["messages"] for d in qdocs)},
        {"check": "malformed payloads detected vs injected",
         "producer": kinds.get("malformed"), "consumer": sum(d["malformed"] for d in qdocs)},
        {"check": "duplicate copies dropped by dropDuplicatesWithinWatermark vs injected",
         "producer": kinds.get("duplicate_copies"), "consumer": live_stats["dropped_duplicates"]},
        {"check": f"rows dropped as later than the {sc['watermark']} watermark vs late fixes injected",
         "producer": kinds.get("late"), "consumer": sum(live_stats["dropped_by_watermark"].values())},
        {"check": "segments aggregated into zone windows vs expected on-time valid segments",
         "producer": manifest.get("expected_on_time_segments"), "consumer": agg.get("segments")},
    ]
    result = {
        "trigger": args.trigger, "wall_clock_s": round(time.time() - t0, 1),
        "watermark": sc["watermark"], "window": sc["window"], "max_offsets_per_trigger": sc["max_offsets_per_trigger"],
        "live_query": live_stats, "quality_query": quality_stats,
        "mongo": {"collection": sc["live_coll"], "zone_window_docs": agg.get("docs"), "segments": agg.get("segments"),
                  "distinct_zones": len(agg.get("zones", [])), "window_range_utc": [str(agg.get("first")), str(agg.get("last"))]},
        "reconciliation": rows,
    }
    evid = project_path("evidence/streaming")
    write_json(evid / "consumer_metrics.json", result)
    md = f"""# Streaming run — Kafka replay -> Structured Streaming -> MongoDB

Source: REPLAY of historical trips ({manifest.get('replay_window_local')}), not a live feed.
Trigger `{args.trigger}`, watermark {sc['watermark']}, tumbling window {sc['window']},
maxOffsetsPerTrigger {sc['max_offsets_per_trigger']}. Measured from StreamingQueryProgress + MongoDB.

## Ground truth (producer) vs observed (consumer)
{md_table(rows)}

## Live query
{md_table([{k: v for k, v in live_stats.items() if k not in ('dropped_by_watermark', 'batches')}])}
Dropped by watermark per stateful operator: {live_stats['dropped_by_watermark']}

Spark only guarantees one direction: rows within the watermark delay are never dropped; rows later
than it MAY be dropped. Since Spark 3.5 late rows are filtered with the watermark of the PREVIOUS
micro-batch (`watermark_used` below is the one used for state eviction). Measured case: the 3 accepted
late fixes (segment start 18:17:07, 18:22:11, 18:26:26 UTC) arrived in batch 87, whose late-row filter
was batch 86's watermark 18:10:26 — all three are after it, and their window [18:15, 18:30) was still open.

## Last 8 micro-batches (watermark used vs event-time span)
{md_table(live_stats['batches'][-8:])}

## MongoDB `{sc['live_coll']}`
{md_table([result['mongo']])}
"""
    (evid / "summary.md").write_text(md, encoding="utf-8")
    print(md)
    spark.stop()


if __name__ == "__main__":
    main()
