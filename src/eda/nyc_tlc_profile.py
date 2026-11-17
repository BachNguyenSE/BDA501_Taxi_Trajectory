"""Step 0a — Feasibility profiling of the ORIGINAL candidate dataset (NYC TLC Yellow Taxi, Jan–Jun 2025).

Goal: produce evidence for the dataset decision. The project needs (1) trip ETA,
(2) WHERE congestion happens and (3) route suggestions. This script measures what the
NYC TLC records actually contain and whether each requirement can be answered.

Run (inside the spark container):
    python -m src.eda.nyc_tlc_profile
Outputs: evidence/eda/nyc_tlc/{profile.json, summary.md, *.png}
"""
from __future__ import annotations

import re
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.evidence import md_table, save_fig, write_json
from src.common.spark_session import get_spark, load_config, project_path

import matplotlib.pyplot as plt

# Columns kept for profiling, with ONE target type each. Monthly files do not always
# agree on physical types (e.g. int32 vs int64), so every file is cast before the union.
COLUMNS = {
    "VendorID": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "double",
    "trip_distance": "double",
    "RatecodeID": "double",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "total_amount": "double",
    "cbd_congestion_fee": "double",
}
LATLON_PATTERN = re.compile(r"(lat|lon|lng|longitude|latitude|polyline|geom)", re.I)
MAX_DURATION_MIN = 180


def main() -> None:
    cfg = load_config()
    raw_dir = project_path(cfg["paths"]["nyc_raw"])
    out = project_path(cfg["paths"]["evidence_eda"]) / "nyc_tlc"
    spark = get_spark("eda-nyc")

    # ---------- 1. Per-file schema & schema drift ----------
    files = sorted(raw_dir.glob("yellow_tripdata_2025-*.parquet"))
    per_file, frames = [], []
    for f in files:
        month = f.stem.split("_")[-1]
        df = spark.read.parquet(str(f))
        per_file.append({
            "month": month,
            "file_mb": round(f.stat().st_size / 1e6, 1),
            "columns": len(df.columns),
            "schema": {fld.name: fld.dataType.simpleString() for fld in df.schema.fields},
        })
        cols = [
            (F.col(c) if c in df.columns else F.lit(None)).cast(t).alias(c)
            for c, t in COLUMNS.items()
        ]
        frames.append(df.select(*cols).withColumn("source_month", F.lit(month)))

    all_cols = sorted({c for p in per_file for c in p["schema"]})
    drift = []
    for c in all_cols:
        types = {p["month"]: p["schema"].get(c, "MISSING") for p in per_file}
        if len(set(types.values())) > 1:
            drift.append({"column": c, **types})

    trips: DataFrame = reduce(lambda a, b: a.unionByName(b), frames).cache()

    # ---------- 2. Volume ----------
    rows_per_month = {r["source_month"]: r["count"] for r in trips.groupBy("source_month").count().collect()}
    total_rows = sum(rows_per_month.values())

    # ---------- 3. Spatial granularity (the decisive question) ----------
    latlon_cols = [c for c in all_cols if LATLON_PATTERN.search(c)]
    zones = spark.read.option("header", True).csv(str(raw_dir / "taxi_zone_lookup.csv")) \
        .withColumn("LocationID", F.col("LocationID").cast("int"))
    n_zone_lookup = zones.count()
    spatial = trips.agg(
        F.countDistinct("PULocationID").alias("distinct_pickup_zones"),
        F.countDistinct("DOLocationID").alias("distinct_dropoff_zones"),
        F.countDistinct("PULocationID", "DOLocationID").alias("distinct_od_pairs"),
        F.avg((F.col("PULocationID") == F.col("DOLocationID")).cast("int")).alias("share_same_zone_trips"),
    ).first().asDict()

    # ---------- 4. Data quality ----------
    t = trips.withColumn(
        "duration_min",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0,
    )
    period_start, period_end = cfg["nyc"]["period_start"], cfg["nyc"]["period_end"]
    unknown_zone_ids = [r["LocationID"] for r in zones.filter(
        F.col("Borough").isin("Unknown", "N/A") | F.col("Zone").isNull() | (F.col("Zone") == "N/A")
    ).collect()]
    rules = {
        "null_pickup_or_dropoff_time": F.col("tpep_pickup_datetime").isNull() | F.col("tpep_dropoff_datetime").isNull(),
        "null_passenger_count": F.col("passenger_count").isNull(),
        "duration_le_0": F.col("duration_min") <= 0,
        f"duration_gt_{MAX_DURATION_MIN}_min": F.col("duration_min") > MAX_DURATION_MIN,
        "pickup_outside_period": (F.col("tpep_pickup_datetime") < F.lit(period_start).cast("timestamp"))
                                 | (F.col("tpep_pickup_datetime") >= F.lit(period_end).cast("timestamp")),
        "trip_distance_le_0": F.col("trip_distance") <= 0,
        "trip_distance_gt_100_mi": F.col("trip_distance") > 100,
        "unknown_zone_id": F.col("PULocationID").isin(unknown_zone_ids) | F.col("DOLocationID").isin(unknown_zone_ids),
        "negative_total_amount": F.col("total_amount") < 0,
    }
    # Informational rules are reported but do not exclude a trip from duration analysis
    # (passenger count and refunds do not affect travel time).
    info_only = {"null_passenger_count", "negative_total_amount"}
    dq_row = t.agg(*[F.sum(cond.cast("int")).alias(name) for name, cond in rules.items()]).first().asDict()
    any_bad = reduce(lambda a, b: a | b,
                     [F.coalesce(c, F.lit(False)) for n, c in rules.items() if n not in info_only])
    clean_rows = t.filter(~any_bad).count()
    duplicate_rows = total_rows - trips.dropDuplicates().count()
    dq = [{"rule": k, "action": "flag only" if k in info_only else "exclude",
           "rows": int(v or 0), "pct": round(100 * (v or 0) / total_rows, 3)} for k, v in dq_row.items()]

    # ---------- 5. What CAN be derived: OD-level average speed by hour ----------
    valid = t.filter(~any_bad).withColumn("hour", F.hour("tpep_pickup_datetime")) \
             .withColumn("speed_mph", F.col("trip_distance") / (F.col("duration_min") / 60.0))
    top_od = (valid.filter(F.col("PULocationID") != F.col("DOLocationID"))
                   .groupBy("PULocationID", "DOLocationID").count()
                   .orderBy(F.desc("count")).limit(1).first())
    names = {r["LocationID"]: f"{r['Zone']} ({r['Borough']})" for r in zones.collect()}
    od_speed = (valid.filter((F.col("PULocationID") == top_od["PULocationID"])
                             & (F.col("DOLocationID") == top_od["DOLocationID"]))
                     .groupBy("hour").agg(F.count("*").alias("trips"),
                                          F.expr("percentile_approx(speed_mph, 0.5)").alias("median_speed_mph"),
                                          F.expr("percentile_approx(duration_min, 0.5)").alias("median_duration_min"))
                     .orderBy("hour").toPandas())
    hourly = valid.groupBy("hour").count().orderBy("hour").toPandas()
    dur_sample = valid.select("duration_min").sample(fraction=0.02, seed=cfg["seed"]).toPandas()

    # ---------- 6. Requirement feasibility matrix ----------
    od_label = f"{names.get(top_od['PULocationID'])} → {names.get(top_od['DOLocationID'])}"
    busy = od_speed[od_speed["trips"] >= 1000]  # ignore thin night hours for the min
    slowest, fastest = busy.loc[busy["median_speed_mph"].idxmin()], od_speed.loc[od_speed["median_speed_mph"].idxmax()]
    speed_sentence = (
        f"Median speed ranges from {slowest['median_speed_mph']:.1f} mph at {int(slowest['hour'])}:00 "
        f"(hours with ≥1,000 trips) to {fastest['median_speed_mph']:.1f} mph at {int(fastest['hour'])}:00, "
        f"so congestion is visible in aggregate"
    )
    feasibility = [
        {"requirement": "Trip duration / ETA prediction", "feasible": "YES",
         "evidence": "pickup/dropoff timestamps + PU/DO zone IDs give a clean regression label"},
        {"requirement": "Locate congested AREAS along the way", "feasible": "NO",
         "evidence": f"no coordinate columns ({len(latlon_cols)} lat/lon/polyline columns); only 2 zone IDs per trip, "
                     "so slowness cannot be attributed to any intermediate area"},
        {"requirement": "Suggest a ROUTE (sequence of areas)", "feasible": "NO",
         "evidence": "no intermediate positions → no zone-to-zone transition graph can be learned"},
        {"requirement": "Speed per area per hour", "feasible": "PARTIAL",
         "evidence": f"only an average speed per OD pair (e.g. {od_label}); "
                     f"{spatial['share_same_zone_trips']*100:.1f}% of trips start and end in the same zone"},
    ]

    # ---------- Persist evidence ----------
    write_json(out / "profile.json", {
        "dataset": "NYC TLC Yellow Taxi Trip Records 2025-01..2025-06",
        "files": [{k: v for k, v in p.items() if k != "schema"} for p in per_file],
        "schema_union": all_cols,
        "schema_drift": drift,
        "rows_per_month": rows_per_month,
        "total_rows": total_rows,
        "coordinate_columns_found": latlon_cols,
        "zone_lookup_rows": n_zone_lookup,
        "spatial": spatial,
        "data_quality": dq,
        "duplicate_rows": duplicate_rows,
        "rows_passing_all_rules": clean_rows,
        "top_od_pair": {"pu": top_od["PULocationID"], "do": top_od["DOLocationID"],
                        "label": od_label, "trips": top_od["count"]},
        "top_od_speed_by_hour": od_speed.to_dict(orient="records"),
        "feasibility": feasibility,
    })

    fig, ax = plt.subplots(figsize=(8, 3.5))
    ax.bar(hourly["hour"], hourly["count"] / 1e6, color="#4C72B0")
    ax.set(title="NYC Yellow Taxi Jan–Jun 2025: trips by pickup hour", xlabel="hour (local)", ylabel="trips (millions)")
    save_fig(fig, out / "trips_by_hour.png")

    fig, ax = plt.subplots(figsize=(8, 3.5))
    ax.hist(dur_sample["duration_min"].clip(upper=90), bins=90, color="#4C72B0")
    ax.set(title="Trip duration (2% sample, clipped at 90 min)", xlabel="minutes", ylabel="trips")
    save_fig(fig, out / "duration_hist.png")

    fig, ax = plt.subplots(figsize=(8, 3.5))
    ax.plot(od_speed["hour"], od_speed["median_speed_mph"], marker="o", color="#C44E52")
    ax.set(title=f"Median speed by hour — busiest OD pair\n{od_label}", xlabel="pickup hour", ylabel="mph")
    save_fig(fig, out / "top_od_speed_by_hour.png")

    summary = f"""# NYC TLC Yellow Taxi (Jan–Jun 2025) — dataset feasibility profile

Generated by `python -m src.eda.nyc_tlc_profile`. All numbers below are computed, not estimated.

## Volume
{md_table([{"month": m, "rows": n} for m, n in sorted(rows_per_month.items())])}

**Total rows:** {total_rows:,} · exact duplicate rows: {duplicate_rows:,} · rows passing all *exclude* rules: {clean_rows:,} ({100*clean_rows/total_rows:.2f}%)

## Schema drift across monthly files
{md_table(drift) if drift else "No type/column differences between months."}

## Spatial granularity — the decisive finding
- Coordinate-like columns (lat/lon/polyline/geometry): **{latlon_cols or "none"}**
- Location is only `PULocationID` / `DOLocationID` ({n_zone_lookup} rows in the zone lookup);
  {spatial['distinct_pickup_zones']} distinct pickup zones, {spatial['distinct_od_pairs']:,} distinct OD pairs.
- **Each trip carries exactly two location facts** (start zone, end zone); the path in between is unknown.
- {spatial['share_same_zone_trips']*100:.1f}% of trips start and end in the same zone (zero route information).

## Data quality rules
{md_table(dq)}

## What the data CAN show
Busiest OD pair: {od_label} ({top_od['count']:,} clean trips) — see `top_od_speed_by_hour.png`.
{speed_sentence}, but the data cannot say **where** along the way the slowdown happens.

## Requirement feasibility
{md_table(feasibility)}

**Conclusion:** NYC TLC supports zone-level ETA regression only. The project's congestion-area and
route-suggestion goals require intermediate GPS positions, so the dataset was replaced by a GPS
trajectory dataset (see `evidence/eda/porto/summary.md`).
"""
    (out / "summary.md").write_text(summary, encoding="utf-8")
    print(summary)
    spark.stop()


if __name__ == "__main__":
    main()
