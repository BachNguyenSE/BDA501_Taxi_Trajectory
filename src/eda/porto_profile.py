"""Step 0b — Profiling of the SELECTED dataset (Porto taxi trajectories, ECML/PKDD 2015).

Answers the same feasibility questions as `nyc_tlc_profile.py`, plus the data-quality
facts the ETL must handle (empty polylines, GPS jumps, [lon, lat] order, DAY_TYPE).

Per-trip metrics are computed with Spark higher-order array functions on the POLYLINE
array (no explode → no shuffle); only the spatial-density step explodes points.

Run (inside the spark container):
    python -m src.eda.porto_profile
Outputs: evidence/eda/porto/{profile.json, summary.md, *.png}
"""
from __future__ import annotations

import matplotlib.pyplot as plt
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.evidence import md_table, save_fig, write_json
from src.common.spark_session import get_spark, load_config, project_path

RAW_SCHEMA = T.StructType([
    T.StructField("TRIP_ID", T.StringType()),
    T.StructField("CALL_TYPE", T.StringType()),
    T.StructField("ORIGIN_CALL", T.StringType()),
    T.StructField("ORIGIN_STAND", T.StringType()),
    T.StructField("TAXI_ID", T.StringType()),
    T.StructField("TIMESTAMP", T.LongType()),
    T.StructField("DAY_TYPE", T.StringType()),
    T.StructField("MISSING_DATA", T.StringType()),
    T.StructField("POLYLINE", T.StringType()),
])
GRID_DEG = 0.005  # ~420 m (lon) x 555 m (lat) at Porto's latitude; EDA only


def haversine_km(p1: str, p2: str) -> str:
    """SQL expression: great-circle distance between two [lon, lat] array elements."""
    return (
        f"2 * 6371.0 * asin(sqrt(pow(sin(radians({p2}[1] - {p1}[1]) / 2), 2) + "
        f"cos(radians({p1}[1])) * cos(radians({p2}[1])) * pow(sin(radians({p2}[0] - {p1}[0]) / 2), 2)))"
    )


def main() -> None:
    cfg = load_config()
    pc = cfg["porto"]
    bbox, step, vmax = pc["bbox"], pc["sampling_interval_s"], pc["max_segment_speed_kmh"]
    out = project_path(cfg["paths"]["evidence_eda"]) / "porto"
    raw_path = project_path(cfg["paths"]["porto_raw"])
    spark = get_spark("eda-porto")

    raw = spark.read.csv(str(raw_path), header=True, schema=RAW_SCHEMA, quote='"', escape='"')
    seg_dist = haversine_km("poly[i - 1]", "poly[i]")
    in_bbox = (f"p[0] BETWEEN {bbox['lon_min']} AND {bbox['lon_max']} "
               f"AND p[1] BETWEEN {bbox['lat_min']} AND {bbox['lat_max']}")

    # `parsed` keeps the nested POLYLINE array and is NOT cached: caching ~80 M nested points
    # caused java.lang.OutOfMemoryError with a 5 GB driver heap. Only the slim per-trip
    # metrics (`trips`) are persisted; steps that need the points re-read the CSV.
    parsed = (
        raw.withColumn("poly", F.from_json("POLYLINE", "array<array<double>>"))
        .withColumn("n_points", F.coalesce(F.size("poly"), F.lit(-1)))
        .withColumn("duration_min", F.greatest(F.col("n_points") - 1, F.lit(0)) * step / 60.0)
        .withColumn("start_ts", F.timestamp_seconds("TIMESTAMP"))
        .withColumn("local_ts", F.from_utc_timestamp("start_ts", "Europe/Lisbon"))
        # segment distances (km) between consecutive 15 s fixes, computed inside the array
        .withColumn("seg_km", F.expr(
            f"CASE WHEN n_points >= 2 THEN transform(sequence(1, n_points - 1), i -> {seg_dist}) "
            "ELSE array() END"))
        .withColumn("length_km", F.expr("aggregate(seg_km, 0D, (acc, x) -> acc + x)"))
        .withColumn("n_segments", F.size("seg_km"))
        .withColumn("n_jump_segments", F.expr(f"size(filter(seg_km, d -> d * 3600.0 / {step} > {vmax}))"))
        .withColumn("n_points_outside_bbox", F.expr(f"size(filter(poly, p -> NOT ({in_bbox})))"))
        .withColumn("first_point_lat_lon_swapped", F.expr(
            # get() returns NULL for empty arrays; poly[0] would raise under Spark 4 ANSI mode
            f"coalesce(get(get(poly, 0), 0) BETWEEN {bbox['lat_min']} AND {bbox['lat_max']}, false)"))
        .drop("POLYLINE")
    )
    trips = parsed.drop("poly", "seg_km").persist(StorageLevel.MEMORY_AND_DISK)

    # ---------- 1. Volume ----------
    vol = trips.agg(
        F.count("*").alias("trips"),
        F.countDistinct("TRIP_ID").alias("distinct_trip_ids"),
        F.countDistinct("TAXI_ID").alias("distinct_taxis"),
        F.sum(F.greatest("n_points", F.lit(0))).alias("gps_points"),
        F.sum("n_segments").alias("gps_segments"),
        F.min("start_ts").alias("first_trip_utc"),
        F.max("start_ts").alias("last_trip_utc"),
        F.sum((F.col("n_points") == -1).cast("int")).alias("unparseable_polyline"),
    ).first().asDict()
    file_gb = round(raw_path.stat().st_size / 1e9, 2)

    # ---------- 2. Categorical columns ----------
    def dist(col: str) -> list[dict]:
        return [r.asDict() for r in trips.groupBy(col).count().orderBy(F.desc("count")).collect()]

    call_type, day_type, missing = dist("CALL_TYPE"), dist("DAY_TYPE"), dist("MISSING_DATA")
    null_like = trips.agg(*[
        F.sum((F.col(c).isNull() | (F.col(c) == "")).cast("int")).alias(c)
        for c in ["ORIGIN_CALL", "ORIGIN_STAND", "TAXI_ID"]
    ]).first().asDict()

    # ---------- 3. Data quality rules ----------
    total = vol["trips"]
    rules = {
        "missing_data_true": F.col("MISSING_DATA") == "True",
        "polyline_empty_or_1_point": F.col("n_points") < 2,
        "duration_gt_180_min": F.col("duration_min") > 180,
        "has_gps_jump_segment": F.col("n_jump_segments") > 0,
        "has_point_outside_porto_bbox": F.col("n_points_outside_bbox") > 0,
        "first_point_looks_lat_lon_swapped": F.col("first_point_lat_lon_swapped"),
    }
    dq_row = trips.agg(*[F.sum(c.cast("int")).alias(n) for n, c in rules.items()]).first().asDict()
    dq = [{"rule": k, "trips": int(v or 0), "pct": round(100 * (v or 0) / total, 3)} for k, v in dq_row.items()]
    jump_seg = trips.agg(F.sum("n_jump_segments")).first()[0]
    clean_trips = trips.filter((F.col("MISSING_DATA") != "True") & (F.col("n_points") >= 2)
                               & (F.col("duration_min") <= 180)).count()

    q = trips.filter(F.col("n_points") >= 2).select(
        F.expr("percentile_approx(n_points, array(0.05, 0.5, 0.95, 0.99))").alias("n_points"),
        F.expr("percentile_approx(duration_min, array(0.05, 0.5, 0.95, 0.99))").alias("duration_min"),
        F.expr("percentile_approx(length_km, array(0.05, 0.5, 0.95, 0.99))").alias("length_km"),
    ).first().asDict()
    quantiles = {k: dict(zip(["p05", "p50", "p95", "p99"], v)) for k, v in q.items()}

    # ---------- 4. Temporal profile ----------
    by_hour = trips.groupBy(F.hour("local_ts").alias("hour")).count().orderBy("hour").toPandas()
    by_dow = trips.groupBy(F.date_format("local_ts", "E").alias("dow"),
                           F.dayofweek("local_ts").alias("d")).count().orderBy("d").toPandas()
    by_month = trips.groupBy(F.date_format("local_ts", "yyyy-MM").alias("month")).count().orderBy("month").toPandas()

    # ---------- 5. Spatial richness: explode all points onto a grid ----------
    pts = (parsed.filter(F.col("n_points") >= 1)
           .select(F.explode("poly").alias("p"))
           .select(F.col("p")[0].alias("lon"), F.col("p")[1].alias("lat"))
           .filter(F.expr(f"lon BETWEEN {bbox['lon_min']} AND {bbox['lon_max']} "
                          f"AND lat BETWEEN {bbox['lat_min']} AND {bbox['lat_max']}")))
    grid = pts.groupBy(F.floor(F.col("lon") / GRID_DEG).alias("gx"),
                       F.floor(F.col("lat") / GRID_DEG).alias("gy")).count()
    grid_stats = grid.agg(
        F.count("*").alias("cells_with_data"),
        F.sum((F.col("count") >= 1000).cast("int")).alias("cells_ge_1000_points"),
        F.expr("percentile_approx(count, 0.5)").alias("median_points_per_cell"),
    ).first().asDict()
    grid_pd = grid.toPandas()

    example = (parsed.filter((F.col("n_points").between(60, 120)) & (F.col("n_jump_segments") == 0)
                            & (F.col("n_points_outside_bbox") == 0))
               .orderBy("TRIP_ID").select("TRIP_ID", "poly", "duration_min", "length_km").first())

    # ---------- 6. Requirement feasibility ----------
    feasibility = [
        {"requirement": "Trip duration / ETA prediction", "feasible": "YES",
         "evidence": f"label = (n_points-1) x {step}s; start point + start time known at request time"},
        {"requirement": "Locate congested AREAS along the way", "feasible": "YES",
         "evidence": f"{vol['gps_segments']:,} timed 15 s segments → speed per area per hour"},
        {"requirement": "Suggest a ROUTE (sequence of areas)", "feasible": "YES",
         "evidence": "consecutive fixes give area-to-area transitions → weighted travel graph"},
        {"requirement": "Speed per area per hour", "feasible": "YES",
         "evidence": f"{grid_stats['cells_with_data']:,} grid cells ({GRID_DEG}°) observed; "
                     f"{grid_stats['cells_ge_1000_points']:,} with ≥1,000 points"},
    ]

    write_json(out / "profile.json", {
        "dataset": "Porto taxi trajectories (ECML/PKDD 2015), train.csv",
        "file_gb": file_gb,
        "volume": vol,
        "call_type": call_type, "day_type": day_type, "missing_data": missing,
        "null_or_empty": null_like,
        "data_quality": dq, "gps_jump_segments": jump_seg,
        "trips_passing_basic_rules": clean_trips,
        "quantiles": quantiles,
        "trips_by_month": by_month.to_dict(orient="records"),
        "grid_deg": GRID_DEG, "grid": grid_stats,
        "example_trip": {"TRIP_ID": example["TRIP_ID"], "points": len(example["poly"]),
                         "duration_min": example["duration_min"], "length_km": example["length_km"]},
        "feasibility": feasibility,
    })

    # ---------- Figures ----------
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.5))
    axes[0].bar(by_hour["hour"], by_hour["count"] / 1e3, color="#55A868")
    axes[0].set(title="Trips by start hour (Europe/Lisbon)", xlabel="hour", ylabel="trips (thousands)")
    axes[1].bar(by_dow["dow"], by_dow["count"] / 1e3, color="#55A868")
    axes[1].set(title="Trips by weekday", ylabel="trips (thousands)")
    save_fig(fig, out / "trips_by_time.png")

    fig, ax = plt.subplots(figsize=(7, 6))
    sc = ax.scatter((grid_pd["gx"] + 0.5) * GRID_DEG, (grid_pd["gy"] + 0.5) * GRID_DEG,
                    c=grid_pd["count"].clip(lower=1).apply(lambda v: __import__("math").log10(v)),
                    s=6, marker="s", cmap="magma")
    fig.colorbar(sc, ax=ax, label="log10(GPS points per cell)")
    ax.set(title=f"GPS point density on a {GRID_DEG}° grid (all trips)", xlabel="longitude", ylabel="latitude")
    ax.set_aspect(1 / 0.75)  # cos(41°) ≈ 0.75
    save_fig(fig, out / "gps_density_grid.png")

    fig, ax = plt.subplots(figsize=(6, 6))
    lons, lats = [p[0] for p in example["poly"]], [p[1] for p in example["poly"]]
    ax.plot(lons, lats, "-o", ms=3, color="#C44E52")
    ax.annotate("start", (lons[0], lats[0])); ax.annotate("end", (lons[-1], lats[-1]))
    ax.set(title=f"One trip = {len(lons)} GPS fixes every {step}s\n"
                 f"({example['duration_min']:.1f} min, {example['length_km']:.1f} km)",
           xlabel="longitude", ylabel="latitude")
    ax.set_aspect(1 / 0.75)
    save_fig(fig, out / "example_trajectory.png")

    fig, ax = plt.subplots(figsize=(8, 3.5))
    hist = trips.filter(F.col("n_points") >= 2).select(
        F.least(F.col("duration_min"), F.lit(90.0)).alias("d")
    ).sample(fraction=0.05, seed=cfg["seed"]).toPandas()
    ax.hist(hist["d"], bins=90, color="#55A868")
    ax.set(title="Trip duration (5% sample, clipped at 90 min)", xlabel="minutes", ylabel="trips")
    save_fig(fig, out / "duration_hist.png")

    summary = f"""# Porto taxi trajectories (ECML/PKDD 2015) — dataset profile

Generated by `python -m src.eda.porto_profile`. All numbers below are computed, not estimated.

## Volume
| metric | value |
|---|---|
| file size | {file_gb} GB (CSV) |
| trips (rows) | {vol['trips']:,} |
| distinct TRIP_ID | {vol['distinct_trip_ids']:,} |
| distinct taxis | {vol['distinct_taxis']:,} |
| **GPS points** | **{vol['gps_points']:,}** |
| timed 15 s segments | {vol['gps_segments']:,} |
| period (UTC) | {vol['first_trip_utc']} → {vol['last_trip_utc']} |
| unparseable POLYLINE | {vol['unparseable_polyline']:,} |

## Distributions
**CALL_TYPE** {md_table(call_type)}

**DAY_TYPE** (provided column) {md_table(day_type)}

**MISSING_DATA** {md_table(missing)}

Null/empty: {null_like}

## Trip size quantiles (trips with ≥ 2 points)
{md_table([{"metric": k, **v} for k, v in quantiles.items()])}

## Data quality rules
{md_table(dq)}

Total GPS-jump segments (> {vmax} km/h within {step}s): {jump_seg:,}.
Trips passing the basic rules (not MISSING_DATA, ≥ 2 points, ≤ 180 min): {clean_trips:,} ({100*clean_trips/total:.2f}%).

## Spatial richness
- {grid_stats['cells_with_data']:,} grid cells of {GRID_DEG}° contain GPS points;
  {grid_stats['cells_ge_1000_points']:,} cells have ≥ 1,000 points (median {grid_stats['median_points_per_cell']:,} points/cell).
- Example trip `{example['TRIP_ID']}`: {len(example['poly'])} fixes, {example['duration_min']:.1f} min, {example['length_km']:.1f} km
  (`example_trajectory.png`).

## Requirement feasibility
{md_table(feasibility)}
"""
    (out / "summary.md").write_text(summary, encoding="utf-8")
    print(summary)
    spark.stop()


if __name__ == "__main__":
    main()
