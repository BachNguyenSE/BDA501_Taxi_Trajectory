"""Step 1 validation — recompute the MapReduce job with Spark SQL and compare key by key.

The Spark SQL query below implements exactly the mapper's rules (MISSING_DATA, >= 2 fixes,
both endpoints inside the bbox, 15 s jump > max speed dropped, cell of the segment MIDPOINT,
local hour of the segment START with the trip-level UTC offset) so that every
(row, col, hour) key must agree. Hadoop job counters are compared with the same counts
computed by Spark.

Run (inside the spark container; normally called by scripts/run_mapreduce.sh):
    python -m src.mapreduce.crosscheck --input data/sample/porto_sample.csv \
        --mr-output data/curated/mapreduce/zone_hour_speed_sample.tsv \
        --job-log evidence/mapreduce/job_sample.log --name sample
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.evidence import md_table, write_json
from src.common.spark_session import get_spark, project_path
from src.eda.porto_profile import RAW_SCHEMA
from src.mapreduce.core import COUNTER_GROUP, Params

MR_SCHEMA = T.StructType([
    T.StructField("cell", T.StringType()), T.StructField("row", T.IntegerType()),
    T.StructField("col", T.IntegerType()), T.StructField("hour", T.IntegerType()),
    T.StructField("center_lat", T.DoubleType()), T.StructField("center_lon", T.DoubleType()),
    T.StructField("n_segments", T.LongType()), T.StructField("total_km", T.DoubleType()),
    T.StructField("total_s", T.LongType()), T.StructField("avg_speed_kmh", T.DoubleType()),
])
# The mapper writes each segment's km as text with 6 decimals and every combine re-writes partial
# sums the same way; each write is off by <= 5e-7 km, and a key has at most n_segments of each.
# Measured: Python vs Spark haversine differ by <= 1 ulp; the 6-decimal rounding has a mean bias
# of ~+1.2e-8 km per segment, which is what accumulates on busy keys.
KM_TOLERANCE_PER_SEGMENT = 1e-6


def reference_sql(p: Params) -> str:
    hav = ("2 * 6371.0 * asin(sqrt(pow(sin((radians(lat2) - radians(lat1)) / 2), 2) + "
           "cos(radians(lat1)) * cos(radians(lat2)) * pow(sin(radians(lon2 - lon1) / 2), 2)))")
    inside = lambda lon, lat: (f"{lon} BETWEEN {p.lon_min} AND {p.lon_max} "  # noqa: E731
                               f"AND {lat} BETWEEN {p.lat_min} AND {p.lat_max}")
    return f"""
WITH trips AS (
  SELECT TIMESTAMP AS ts, from_json(POLYLINE, 'array<array<double>>') AS poly
  FROM raw_trips
  WHERE coalesce(MISSING_DATA, '') <> 'True' AND TIMESTAMP IS NOT NULL
), usable AS (
  SELECT ts, poly,
         unix_timestamp(from_utc_timestamp(timestamp_seconds(ts), 'Europe/Lisbon')) - ts AS utc_offset_s
  FROM trips
  WHERE size(poly) >= 2
), segments AS (
  SELECT ts + utc_offset_s AS local0, i,
         poly[i - 1][0] AS lon1, poly[i - 1][1] AS lat1, poly[i][0] AS lon2, poly[i][1] AS lat2
  FROM usable LATERAL VIEW explode(sequence(1, size(poly) - 1)) s AS i
), classified AS (
  SELECT *,
         ({inside('lon1', 'lat1')}) AND ({inside('lon2', 'lat2')}) AS both_inside,
         {hav} AS km
  FROM segments
)
SELECT CAST(floor(((lat1 + lat2) / 2 - {p.lat_min}) / {p.grid_deg}) AS INT) AS row,
       CAST(floor(((lon1 + lon2) / 2 - {p.lon_min}) / {p.grid_deg}) AS INT) AS col,
       CAST(pmod(floor((local0 + (i - 1) * {p.step_s}) / 3600), 24) AS INT) AS hour,
       count(*) AS n_segments,
       sum(km) AS total_km,
       count(*) * {p.step_s} AS total_s
FROM classified
WHERE both_inside AND km <= {p.max_segment_km}
GROUP BY 1, 2, 3
"""


def parse_counters(log_text: str) -> dict[str, int]:
    """Read the `porto` counter group printed by Hadoop at job completion."""
    out: dict[str, int] = {}
    in_group = False
    for line in log_text.splitlines():
        stripped = line.strip()
        if stripped == COUNTER_GROUP:
            in_group = True
            continue
        if in_group:
            m = re.fullmatch(r"([a-z_0-9]+)=(\d+)", stripped)
            if not m:
                break
            out[m.group(1)] = int(m.group(2))
    for name in ("Map input records", "Map output records", "Combine input records",
                 "Combine output records", "Reduce input records", "Reduce output records",
                 "Reduce shuffle bytes", "Launched map tasks", "Launched reduce tasks"):
        m = re.search(rf"\b{re.escape(name)}=(\d+)", log_text)
        if m:
            out[name] = int(m.group(1))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--mr-output", required=True)
    ap.add_argument("--job-log", required=True)
    ap.add_argument("--name", required=True)
    args = ap.parse_args()

    p = Params()
    spark = get_spark(f"mr-crosscheck-{args.name}")
    raw = spark.read.csv(str(project_path(args.input)), header=True, schema=RAW_SCHEMA, quote='"', escape='"')
    raw.createOrReplaceTempView("raw_trips")
    sql = reference_sql(p)
    ref = spark.sql(sql).cache()
    mr = spark.read.csv(str(project_path(args.mr_output)), sep="\t", schema=MR_SCHEMA)

    joined = (mr.alias("m").join(ref.alias("s"), ["row", "col", "hour"], "full_outer")
              .select("row", "col", "hour",
                      F.col("m.n_segments").alias("mr_n"), F.col("s.n_segments").alias("spark_n"),
                      F.col("m.total_km").alias("mr_km"), F.col("s.total_km").alias("spark_km"),
                      F.col("m.avg_speed_kmh").alias("mr_speed"),
                      (F.col("s.total_km") / F.col("s.total_s") * 3600).alias("spark_speed"))
              .cache())
    both = joined.filter(F.col("mr_n").isNotNull() & F.col("spark_n").isNotNull())
    s = both.agg(
        F.count("*").alias("keys_in_both"),
        F.sum((F.col("mr_n") == F.col("spark_n")).cast("int")).alias("n_segments_exact"),
        F.sum((F.abs(F.col("mr_km") - F.col("spark_km")) <= F.col("mr_n") * KM_TOLERANCE_PER_SEGMENT)
              .cast("int")).alias("total_km_within_tol"),
        F.max(F.abs(F.col("mr_km") - F.col("spark_km"))).alias("max_abs_km_diff"),
        F.max(F.abs(F.col("mr_speed") - F.col("spark_speed"))).alias("max_abs_speed_diff_kmh"),
    ).first().asDict()
    s["keys_mapreduce"] = mr.count()
    s["keys_spark_sql"] = ref.count()
    s["keys_only_in_mapreduce"] = joined.filter(F.col("spark_n").isNull()).count()
    s["keys_only_in_spark_sql"] = joined.filter(F.col("mr_n").isNull()).count()
    totals = ref.agg(F.sum("n_segments").alias("n"), F.sum("total_km").alias("km")).first()
    mr_totals = mr.agg(F.sum("n_segments").alias("n"), F.sum("total_km").alias("km")).first()
    verdict = ("PASS" if s["keys_only_in_mapreduce"] == 0 and s["keys_only_in_spark_sql"] == 0
               and s["n_segments_exact"] == s["keys_in_both"] and s["total_km_within_tol"] == s["keys_in_both"]
               else "FAIL")

    counters = parse_counters(Path(project_path(args.job_log)).read_text(encoding="utf-8"))
    counter_rows = [
        {"check": "segments emitted by mapper vs segments kept by Spark SQL",
         "hadoop": counters.get("map_output_records"), "spark_sql": int(totals["n"])},
        {"check": "segments in reducer output (sum n_segments)",
         "hadoop": int(mr_totals["n"]), "spark_sql": int(totals["n"])},
        {"check": "combiner records in -> out (shuffle reduction)",
         "hadoop": f"{counters.get('Combine input records')} -> {counters.get('Combine output records')}",
         "spark_sql": "n/a"},
        {"check": "reduce output records (= distinct keys)",
         "hadoop": counters.get("Reduce output records"), "spark_sql": s["keys_spark_sql"]},
    ]
    sample = [r.asDict() for r in joined.orderBy(F.desc("spark_n")).limit(5).collect()]
    worst = [r.asDict() for r in both.withColumn("abs_km_diff", F.abs(F.col("mr_km") - F.col("spark_km")))
             .withColumn("rel_km_diff", F.expr("try_divide(abs_km_diff, spark_km)"))  # parked taxis: 0 km
             .orderBy(F.desc("abs_km_diff")).limit(5).collect()]
    s["max_rel_km_diff"] = max((w["rel_km_diff"] or 0.0) for w in worst) if worst else 0.0

    evid = project_path("evidence/mapreduce")
    write_json(evid / f"crosscheck_{args.name}.json", {
        "input": args.input, "verdict": verdict, "summary": s,
        "total_segments": {"mapreduce": int(mr_totals["n"]), "spark_sql": int(totals["n"])},
        "total_km": {"mapreduce": mr_totals["km"], "spark_sql": totals["km"]},
        "hadoop_counters": counters, "km_tolerance_per_segment": KM_TOLERANCE_PER_SEGMENT, "reference_sql": sql,
    })
    md = f"""# MapReduce cross-check ({args.name}) — verdict: **{verdict}**

Input: `{args.input}`. Hadoop Streaming job on YARN/HDFS (`src/mapreduce/`) vs an independent
Spark SQL query with identical semantics (query text in `crosscheck_{args.name}.json`).
Measured, not inferred.

## Key-by-key comparison (key = grid row, grid col, local hour)
{md_table([{**s, "max_abs_km_diff": f"{s['max_abs_km_diff']:.2e}",
            "max_abs_speed_diff_kmh": f"{s['max_abs_speed_diff_kmh']:.2e}",
            "max_rel_km_diff": f"{s['max_rel_km_diff']:.2e}"}])}

Segment counts must match exactly. total_km may differ by at most n_segments x {KM_TOLERANCE_PER_SEGMENT} km
per key: the mapper writes every segment's km as text with 6 decimals (error <= 5e-7 km) and each
combine re-writes partial sums the same way. Measured separately: Python and Spark haversine agree
to 1 ulp; the 6-decimal rounding has a mean bias of about +1.2e-8 km per segment, which is why
MapReduce totals are slightly higher on the busiest keys.

## Hadoop counters vs Spark SQL
{md_table(counter_rows)}

## Hadoop job counters (group `{COUNTER_GROUP}` + framework)
{md_table([{"counter": k, "value": v} for k, v in counters.items()])}

## Five busiest keys side by side
{md_table(sample)}

## Five keys with the largest total_km difference
{md_table(worst)}
"""
    (evid / f"crosscheck_{args.name}.md").write_text(md, encoding="utf-8")
    print(md)
    spark.stop()


if __name__ == "__main__":
    main()
