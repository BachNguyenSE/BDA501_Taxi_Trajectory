"""Step 2 — Spark ETL: raw Porto CSV -> curated Parquet.

Produces four curated tables under `data/curated/` (configurable in `conf/pipeline.yaml`):

    zones              one row per occupied grid cell: zone_id, gx, gy, centroid lon/lat
    zone_hour_speed    avg speed per zone x local-hour x {weekday,weekend}, distance-weighted
    zone_graph_edges   directed edges between zones actually traversed by trips, with
                       avg speed / avg traversal time -> the routing graph
    trip_features      one row per clean trip: start/end point, start time, duration,
                       distance -> training table for the ML travel-time model

Run (inside the spark container):
    python -m src.etl.porto_etl              # full train.csv
    python -m src.etl.porto_etl --sample      # data/sample/porto_sample.csv (fast iteration)
"""
from __future__ import annotations

import argparse

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from src.common.evidence import write_json
from src.common.spark_session import get_spark, load_config, project_path
from src.eda.porto_profile import RAW_SCHEMA, haversine_km


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", action="store_true", help="use data/sample/porto_sample.csv instead of the full file")
    args = parser.parse_args()

    cfg = load_config()
    pc, ec = cfg["porto"], cfg["etl"]
    bbox, step, vmax = pc["bbox"], pc["sampling_interval_s"], pc["max_segment_speed_kmh"]
    grid_deg = ec["grid_deg"]
    curated = project_path(cfg["paths"]["curated"])
    raw_path = project_path(cfg["paths"]["porto_sample"] if args.sample else cfg["paths"]["porto_raw"])

    spark = get_spark("etl-porto")
    raw = spark.read.csv(str(raw_path), header=True, schema=RAW_SCHEMA, quote='"', escape='"')

    trips = (
        raw.withColumn("poly", F.from_json("POLYLINE", "array<array<double>>"))
        .withColumn("n_points", F.coalesce(F.size("poly"), F.lit(-1)))
        .withColumn("start_ts", F.timestamp_seconds("TIMESTAMP"))
        .withColumn("duration_min", F.greatest(F.col("n_points") - 1, F.lit(0)) * step / 60.0)
        .filter(
            (F.col("MISSING_DATA") != "True")
            & (F.col("n_points") >= 2)
            & (F.col("duration_min") <= ec["max_trip_duration_min"])
        )
    )

    # ---------- points: one row per GPS fix (ALL fixes; the bbox test is a flag, not a filter) ----------
    # Filtering before the window function would let lag() pair two non-consecutive fixes
    # and still charge them 15 s. Segments are therefore built first and kept only when
    # BOTH endpoints are inside the bbox — the same rule as the MapReduce mapper.
    points = (
        trips.select("TRIP_ID", "CALL_TYPE", "TIMESTAMP", F.posexplode("poly").alias("seq", "p"))
        .select(
            "TRIP_ID", "CALL_TYPE", "seq",
            F.col("p")[0].alias("lon"), F.col("p")[1].alias("lat"),
            F.timestamp_seconds(F.col("TIMESTAMP") + F.col("seq") * step).alias("ts"),
        )
        .withColumn("in_bbox",
                    F.col("lon").between(bbox["lon_min"], bbox["lon_max"])
                    & F.col("lat").between(bbox["lat_min"], bbox["lat_max"]))
        .withColumn("gx", F.floor(F.col("lon") / grid_deg).cast("int"))
        .withColumn("gy", F.floor(F.col("lat") / grid_deg).cast("int"))
        .withColumn("zone_id", F.concat_ws("_", F.col("gx"), F.col("gy")))
    )

    # ---------- segments: consecutive fixes of one trip, attributed to the zone of their midpoint
    # and to the local hour in which they START (identical semantics to src/mapreduce) ----------
    w = Window.partitionBy("TRIP_ID").orderBy("seq")
    seg_dist = haversine_km("array(prev_lon, prev_lat)", "array(lon, lat)")
    segments = (
        points.withColumn("prev_lon", F.lag("lon").over(w))
        .withColumn("prev_lat", F.lag("lat").over(w))
        .withColumn("prev_zone", F.lag("zone_id").over(w))
        .withColumn("prev_ts", F.lag("ts").over(w))
        .withColumn("prev_in_bbox", F.lag("in_bbox").over(w))
        .filter(F.col("prev_lon").isNotNull() & F.col("in_bbox") & F.col("prev_in_bbox"))
        .withColumn("distance_km", F.expr(seg_dist))
        .withColumn("duration_s", F.lit(float(step)))
        .withColumn("speed_kmh", F.col("distance_km") * 3600.0 / F.col("duration_s"))
        .filter(F.col("speed_kmh") <= vmax)
        .withColumn("mid_zone", F.concat_ws(
            "_",
            F.floor((F.col("lon") + F.col("prev_lon")) / 2 / grid_deg).cast("int"),
            F.floor((F.col("lat") + F.col("prev_lat")) / 2 / grid_deg).cast("int")))
        .withColumn("start_local", F.from_utc_timestamp("prev_ts", "Europe/Lisbon"))
        .withColumn("hour", F.hour("start_local"))
        .withColumn("is_weekend", F.dayofweek("start_local").isin(1, 7).cast("int"))
        .select("mid_zone", "hour", "is_weekend", "distance_km", "duration_s", "prev_zone", "zone_id")
        # ~80M rows: deserialized MEMORY_AND_DISK re-reads spilled blocks into the 5 GB heap and OOMs
        .persist(StorageLevel.DISK_ONLY)
    )

    # ---------- zones: occupied grid cells + centroid ----------
    zones = (
        points.filter("in_bbox").groupBy("zone_id", "gx", "gy")
        .count()
        .withColumn("centroid_lon", (F.col("gx") + 0.5) * grid_deg)
        .withColumn("centroid_lat", (F.col("gy") + 0.5) * grid_deg)
        .withColumnRenamed("count", "n_points")
    )
    zones.write.mode("overwrite").parquet(str(curated / "zones"))

    # ---------- zone_hour_speed: distance-weighted avg speed per zone x hour x weekday/weekend ----------
    zone_hour_speed = (
        segments.groupBy(F.col("mid_zone").alias("zone_id"), "hour", "is_weekend")
        .agg(
            F.sum("distance_km").alias("total_km"),
            F.sum("duration_s").alias("total_s"),
            F.count("*").alias("n_segments"),
        )
        .withColumn("avg_speed_kmh", F.col("total_km") * 3600.0 / F.col("total_s"))
    )
    zone_hour_speed.write.mode("overwrite").parquet(str(curated / "zone_hour_speed"))

    # ---------- zone_graph_edges: directed edges between adjacent zones a trip actually crossed ----------
    zone_graph_edges = (
        segments.filter(F.col("prev_zone") != F.col("zone_id"))
        .groupBy(F.col("prev_zone").alias("from_zone"), F.col("zone_id").alias("to_zone"))
        .agg(
            F.sum("distance_km").alias("total_km"),
            F.sum("duration_s").alias("total_s"),
            F.count("*").alias("n_observations"),
        )
        .withColumn("avg_speed_kmh", F.col("total_km") * 3600.0 / F.col("total_s"))
        .withColumn("avg_time_s", F.col("total_s") / F.col("n_observations"))
        .drop("total_km", "total_s")
    )
    zone_graph_edges.write.mode("overwrite").parquet(str(curated / "zone_graph_edges"))

    # ---------- trip_features: one row per clean trip, for the ML model ----------
    first_last = (
        points.filter("in_bbox").groupBy("TRIP_ID")
        .agg(
            F.min_by(F.struct("lon", "lat"), "seq").alias("start_pt"),
            F.max_by(F.struct("lon", "lat"), "seq").alias("end_pt"),
            F.count("*").alias("n_points_in_bbox"),
        )
    )
    trip_features = (
        trips.join(first_last, "TRIP_ID")
        .select(
            "TRIP_ID", "CALL_TYPE",
            F.col("start_pt.lon").alias("start_lon"), F.col("start_pt.lat").alias("start_lat"),
            F.col("end_pt.lon").alias("end_lon"), F.col("end_pt.lat").alias("end_lat"),
            "start_ts", "duration_min", "n_points",
        )
        .withColumn("local_ts", F.from_utc_timestamp("start_ts", "Europe/Lisbon"))
        .withColumn("hour", F.hour("local_ts"))
        .withColumn("day_of_week", F.dayofweek("local_ts"))
        .withColumn("is_weekend", (F.dayofweek("local_ts").isin(1, 7)).cast("int"))
        .withColumn("straight_km", F.expr(haversine_km("array(start_lon, start_lat)", "array(end_lon, end_lat)")))
        .filter(F.col("straight_km") > 0.05)  # drop trips that start and end at ~the same point
        .drop("local_ts", "start_pt", "end_pt")
    )
    trip_features.write.mode("overwrite").parquet(str(curated / "trip_features"))

    stats = {
        "source": str(raw_path),
        "trips_in": trips.count(),
        "segments_kept": segments.count(),
        "zones": zones.count(),
        "zone_hour_speed_rows": zone_hour_speed.count(),
        "zone_graph_edges": zone_graph_edges.count(),
        "trip_features_rows": trip_features.count(),
    }
    write_json(project_path(cfg["paths"]["evidence_eda"]) / "etl_stats.json", stats)
    print(stats)
    segments.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
