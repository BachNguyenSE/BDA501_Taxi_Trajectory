"""Step 3 — Spark SQL analytics on the curated Parquet, query plans, and one controlled experiment.

Queries (results -> evidence/analytics/):
  Q1  weekday evening-peak bottlenecks: slowest zones 17:00-19:59 (distance-weighted speed)
  Q2  city-wide speed profile by local hour, weekday vs weekend
  Q3  trips by origin zone joined with that zone's historical speed at the departure hour
      (1.6M-row fact table x 72k-row zone_hour_speed: the join the experiment varies)

Controlled experiment (evidence/session9/B_benchmark.*): Q3 with the default join planning
(zone_hour_speed is small enough to broadcast) vs broadcast disabled
(spark.sql.autoBroadcastJoinThreshold = -1 -> SortMergeJoin). Everything else is fixed: same
data, same session, warm-up run first, 3 timed runs each. Shuffle bytes come from the Spark
REST API for the jobs of each run. Formatted plans -> evidence/session9/C_query_plan.txt.

Run (inside the spark container, after src.etl.porto_etl):
    python -m src.analytics.spark_sql_analytics
"""
from __future__ import annotations

import json
import statistics
import time
import urllib.request

from src.common.evidence import md_table, write_json
from src.common.spark_session import get_spark, load_config, project_path

Q1 = """
SELECT z.zone_id, round(z.centroid_lat, 4) AS lat, round(z.centroid_lon, 4) AS lon,
       round(sum(s.total_km) * 3600 / sum(s.total_s), 2) AS peak_speed_kmh,
       sum(s.n_segments) AS n_segments
FROM zone_hour_speed s JOIN zones z ON s.zone_id = z.zone_id
WHERE s.is_weekend = 0 AND s.hour BETWEEN 17 AND 19
GROUP BY z.zone_id, z.centroid_lat, z.centroid_lon
HAVING sum(s.n_segments) >= 2000
ORDER BY peak_speed_kmh ASC
LIMIT 10
"""

Q2 = """
SELECT hour, CASE is_weekend WHEN 1 THEN 'weekend' ELSE 'weekday' END AS day_type,
       round(sum(total_km) * 3600 / sum(total_s), 2) AS city_speed_kmh,
       sum(n_segments) AS n_segments
FROM zone_hour_speed
GROUP BY hour, is_weekend
ORDER BY is_weekend, hour
"""

Q3 = """
WITH t AS (
  SELECT concat_ws('_', CAST(floor(start_lon / {g}) AS INT), CAST(floor(start_lat / {g}) AS INT)) AS origin_zone,
         hour, is_weekend, duration_min, straight_km
  FROM trip_features
)
SELECT t.origin_zone, count(*) AS trips,
       round(avg(t.duration_min), 2) AS avg_duration_min,
       round(avg(t.straight_km), 2) AS avg_straight_km,
       round(avg(s.avg_speed_kmh), 2) AS origin_hist_speed_kmh
FROM t JOIN zone_hour_speed s
  ON t.origin_zone = s.zone_id AND t.hour = s.hour AND t.is_weekend = s.is_weekend
GROUP BY t.origin_zone
HAVING count(*) >= 1000
ORDER BY trips DESC
LIMIT 15
"""


def _get(url: str):
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read())


def shuffle_bytes(spark, group: str) -> dict:
    """Sum shuffle read/write over the stages of the jobs in one job group (Spark REST API)."""
    sc = spark.sparkContext
    base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    stage_ids = {sid for j in _get(f"{base}/jobs") if j.get("jobGroup") == group for sid in j["stageIds"]}
    read = write = 0
    for sid in stage_ids:
        for attempt in _get(f"{base}/stages/{sid}"):
            read += attempt.get("shuffleReadBytes", 0)
            write += attempt.get("shuffleWriteBytes", 0)
    return {"shuffle_read_bytes": read, "shuffle_write_bytes": write, "stages": len(stage_ids)}


def join_strategy(plan: str) -> str:
    for name in ("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin", "BroadcastNestedLoopJoin"):
        if name in plan:
            return name
    return "?"


def main() -> None:
    cfg = load_config()
    curated = project_path(cfg["paths"]["curated"])
    out_a, out_s9 = project_path("evidence/analytics"), project_path("evidence/session9")
    out_a.mkdir(parents=True, exist_ok=True)
    out_s9.mkdir(parents=True, exist_ok=True)

    spark = get_spark("spark-sql-analytics")
    for name in ("zones", "zone_hour_speed", "zone_graph_edges", "trip_features"):
        spark.read.parquet(str(curated / name)).createOrReplaceTempView(name)
    q3 = Q3.format(g=cfg["etl"]["grid_deg"])

    results = {}
    for name, sql in (("Q1_peak_bottlenecks", Q1), ("Q2_hourly_speed_profile", Q2), ("Q3_origin_zone_trips", q3)):
        pdf = spark.sql(sql).toPandas()
        pdf.to_csv(out_a / f"{name}.csv", index=False)
        results[name] = pdf

    # ---------------- query plans (formatted) ----------------
    plans = []
    variants = {"broadcast_allowed (default)": None, "broadcast_disabled": "-1"}
    default_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    for label, threshold in variants.items():
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", threshold or default_threshold)
        df = spark.sql(q3)
        df.collect()  # execute so AQE finalises the plan
        plan = df._jdf.queryExecution().executedPlan().toString()
        formatted = spark.sql(f"EXPLAIN FORMATTED {q3}").first()[0]
        plans.append((label, join_strategy(plan), formatted, plan))
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", default_threshold)
    q1_plan = spark.sql(f"EXPLAIN FORMATTED {Q1}").first()[0]
    with open(out_s9 / "C_query_plan.txt", "w", encoding="utf-8") as f:
        f.write("C_query_plan — Spark 4.0.1, local[*], curated Parquet from the FULL dataset. Measured.\n\n")
        for label, strategy, formatted, final in plans:
            f.write(f"===== Q3 [{label}] — join strategy: {strategy} =====\n{q3}\n")
            f.write(f"--- EXPLAIN FORMATTED (planned) ---\n{formatted}\n")
            f.write(f"--- final adaptive plan after execution ---\n{final}\n\n")
        f.write(f"===== Q1 — EXPLAIN FORMATTED =====\n{Q1}\n{q1_plan}\n")

    # ---------------- controlled experiment: join strategy ----------------
    runs = []
    for label, threshold in variants.items():
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", threshold or default_threshold)
        spark.sql(q3).collect()  # warm-up (file listing, JIT, OS cache)
        times, shuffles = [], []
        for i in range(3):
            group = f"{label}-run{i}"
            spark.sparkContext.setJobGroup(group, group)
            t0 = time.perf_counter()
            spark.sql(q3).collect()
            times.append(time.perf_counter() - t0)
            shuffles.append(shuffle_bytes(spark, group))
        runs.append({
            "variant": label,
            "join_strategy": next(s for lbl, s, _, _ in plans if lbl == label),
            "runtime_s_median": round(statistics.median(times), 3),
            "runtime_s_runs": [round(t, 3) for t in times],
            "shuffle_write_MB": round(statistics.median(s["shuffle_write_bytes"] for s in shuffles) / 1e6, 2),
            "shuffle_read_MB": round(statistics.median(s["shuffle_read_bytes"] for s in shuffles) / 1e6, 2),
            "stages": shuffles[0]["stages"],
        })
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", default_threshold)

    rows = {name: spark.table(name).count() for name in ("trip_features", "zone_hour_speed")}
    write_json(out_s9 / "B_benchmark.json", {
        "question": "Does broadcasting the 72k-row zone_hour_speed table beat a sort-merge join for Q3?",
        "environment": "Docker Desktop VM 12 vCPU / 8 GB, Spark 4.0.1 local[*], driver 5g, shuffle partitions "
                       f"{spark.conf.get('spark.sql.shuffle.partitions')}, AQE on",
        "input_rows": rows, "factor": "spark.sql.autoBroadcastJoinThreshold (default vs -1)",
        "runs": runs, "kind": "measured",
    })
    b_md = f"""# B_benchmark — controlled experiment: join strategy for Q3

**Question.** Q3 joins `trip_features` ({rows['trip_features']:,} rows) with `zone_hour_speed`
({rows['zone_hour_speed']:,} rows). Does broadcasting the small side beat a sort-merge join?
**Factor changed:** `spark.sql.autoBroadcastJoinThreshold` (default vs -1). Everything else fixed:
same curated Parquet (full dataset), same session, one warm-up then 3 timed runs per variant.
**Expected before running:** broadcast avoids shuffling the 1.6M-row side, so less shuffle and lower runtime.
Environment: Docker Desktop VM 12 vCPU / 8 GB, Spark 4.0.1 `local[*]`, AQE on. Measured.

{md_table(runs)}

Query text and both formatted plans: `C_query_plan.txt`.
"""
    (out_s9 / "B_benchmark.md").write_text(b_md, encoding="utf-8")

    summary = "# Spark SQL analytics (curated Parquet, full dataset)\n\n" + "\n\n".join(
        f"## {name}\n```sql\n{sql.strip()}\n```\n{md_table(results[name].to_dict(orient='records'))}"
        for name, sql in (("Q1_peak_bottlenecks", Q1), ("Q2_hourly_speed_profile", Q2), ("Q3_origin_zone_trips", q3))
    )
    (out_a / "summary.md").write_text(summary, encoding="utf-8")
    print(b_md)
    for label, strategy, _, _ in plans:
        print(f"Q3 [{label}] -> {strategy}")
    spark.stop()


if __name__ == "__main__":
    main()
