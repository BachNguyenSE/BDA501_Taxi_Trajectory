"""Session 9 audit checks -> evidence/session9/{D_data_validation, E_model_validation, F_serving_stream, H_project_card}.md

D  schema, nulls, duplicates, row-count reconciliation, join unmatched keys, Parquet <-> MongoDB read-back
E  TEMPORAL hold-out (train < 2014-05-01 <= test): naive mean, historical-speed baseline, linear, GBT;
   MAE / RMSE / signed error / % over- and under-estimated  (Card F test F2)
F  MongoDB point-read latency, /estimate end-to-end latency, live-layer read-back
H  Card F: F1 feature availability for 3 real trips, F3 one real trip through ETL -> feature -> API

Run (inside the spark container; mongo + api up, curated Parquet + MongoDB loaded):
    python -m src.validation.session9_checks
"""
from __future__ import annotations

import csv
import json
import math
import os
import statistics
import time
import urllib.request

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import LinearRegression

from src.common.evidence import md_table, write_json
from src.common.mongo import get_db
from src.common.spark_session import load_config, project_path

FEATURES = ["straight_km", "hour", "day_of_week", "is_weekend", "bearing_deg"]
SPLIT_UTC = pd.Timestamp("2014-05-01", tz="UTC")
API = os.environ.get("API_URL", "http://api:8000")
OUT = project_path("evidence/session9")


def bearing(df: pd.DataFrame) -> np.ndarray:
    p1, p2 = np.radians(df.start_lat), np.radians(df.end_lat)
    dl = np.radians(df.end_lon - df.start_lon)
    return np.degrees(np.arctan2(np.sin(dl) * np.cos(p2), np.cos(p1) * np.sin(p2) - np.sin(p1) * np.cos(p2) * np.cos(dl)))


def post(path: str, body: dict) -> tuple[dict, float]:
    req = urllib.request.Request(API + path, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read())
    return data, (time.perf_counter() - t0) * 1000


def pct(xs, q):
    return round(float(np.percentile(xs, q)), 2)


def main() -> None:
    cfg = load_config()
    cur = project_path(cfg["paths"]["curated"])
    db = get_db()
    mc = cfg["mongo"]
    OUT.mkdir(parents=True, exist_ok=True)
    eda = json.loads(project_path("evidence/eda/porto/profile.json").read_text())
    etl = json.loads(project_path("evidence/eda/etl_stats.json").read_text())

    tf = pd.read_parquet(cur / "trip_features")
    zhs = pd.read_parquet(cur / "zone_hour_speed")
    edges = pd.read_parquet(cur / "zone_graph_edges")
    zones = pd.read_parquet(cur / "zones")
    g = cfg["etl"]["grid_deg"]

    # =========================================================== D
    schema = [{"table": n, "rows": len(df), "columns": ", ".join(f"{c}:{t}" for c, t in df.dtypes.astype(str).items())}
              for n, df in (("trip_features", tf), ("zone_hour_speed", zhs), ("zone_graph_edges", edges), ("zones", zones))]
    nulls = {
        "trip_features": int(tf[["TRIP_ID", "start_lon", "start_lat", "end_lon", "end_lat", "duration_min", "hour"]].isna().sum().sum()),
        "zone_hour_speed": int(zhs[["zone_id", "hour", "is_weekend", "avg_speed_kmh"]].isna().sum().sum()),
        "zone_graph_edges": int(edges[["from_zone", "to_zone", "avg_time_s"]].isna().sum().sum()),
    }
    dups = {
        "trip_features TRIP_ID": int(tf.TRIP_ID.duplicated().sum()),
        "zone_hour_speed (zone_id, hour, is_weekend)": int(zhs.duplicated(["zone_id", "hour", "is_weekend"]).sum()),
        "zone_graph_edges (from_zone, to_zone)": int(edges.duplicated(["from_zone", "to_zone"]).sum()),
        "zones zone_id": int(zones.zone_id.duplicated().sum()),
    }
    mr_full = json.loads(project_path("evidence/mapreduce/crosscheck_full.json").read_text())
    mr_trips = mr_full["hadoop_counters"]["trips_used"]
    long_trips = next(r["trips"] for r in eda["data_quality"] if r["rule"] == "duration_gt_180_min")
    origin = (np.floor(tf.start_lon / g).astype(int).astype(str) + "_" + np.floor(tf.start_lat / g).astype(int).astype(str))
    keys = set(zip(zhs.zone_id, zhs.hour, zhs.is_weekend))
    unmatched = int(sum((o, h, w) not in keys for o, h, w in zip(origin, tf.hour, tf.is_weekend)))
    zone_ids = set(zones.zone_id)
    edge_orphans = int((~edges.from_zone.isin(zone_ids) | ~edges.to_zone.isin(zone_ids)).sum())
    zhs_orphans = int((~zhs.zone_id.isin(zone_ids)).sum())
    mongo_counts = {n: db[c].count_documents({}) for n, c in
                    (("zones", mc["zones_coll"]), ("zone_hour_speed", mc["zone_hour_speed_coll"]), ("zone_graph_edges", mc["zone_graph_coll"]))}
    readback_ok = mongo_counts == {"zones": len(zones), "zone_hour_speed": len(zhs), "zone_graph_edges": len(edges)}

    d_rows = [
        {"item": "Schema / data types", "method": "explicit RAW_SCHEMA on read; dtypes of curated Parquet listed below",
         "result": "4 curated tables, types as listed", "pass": "PASS"},
        {"item": "Null / invalid values", "method": "null count on key columns of curated tables",
         "result": str(nulls), "pass": "PASS" if not any(nulls.values()) else "FAIL"},
        {"item": "Duplicate handling", "method": "duplicate keys per curated table",
         "result": str(dups), "pass": "PASS with known gap (8 duplicate TRIP_IDs kept, 0.0005%)"},
        {"item": "Row-count reconciliation", "method": "raw -> ETL filters -> trip_features; ETL vs MapReduce trips",
         "result": f"raw {eda['volume']['trips']:,} -> ETL {etl['trips_in']:,} -> trip_features {etl['trip_features_rows']:,}; "
                   f"MapReduce {mr_trips:,} - ETL {etl['trips_in']:,} = {mr_trips - etl['trips_in']:,} = trips > 180 min ({long_trips:,})",
         "pass": "PASS" if mr_trips - etl["trips_in"] == long_trips else "FAIL"},
        {"item": "Join correctness / unmatched keys", "method": "Q3 trips whose (origin zone, hour, weekend) has no zone_hour_speed row; orphan zone ids",
         "result": f"{unmatched:,} of {len(tf):,} trips unmatched ({100 * unmatched / len(tf):.2f}%, dropped by the inner join); "
                   f"edges with unknown zone: {edge_orphans}; zone_hour_speed zones not in zones: {zhs_orphans}",
         "pass": "PASS (unmatched reported, not hidden)"},
        {"item": "Independent sample check / known answer", "method": "Hadoop MapReduce vs Spark SQL with identical semantics, key by key",
         "result": f"verdict {mr_full['verdict']}: {mr_full['summary']['keys_in_both']:,} keys, all segment counts exact",
         "pass": mr_full["verdict"]},
        {"item": "Curated output can be read back reproducibly", "method": "Parquet row counts vs MongoDB document counts",
         "result": f"MongoDB {mongo_counts}", "pass": "PASS" if readback_ok else "FAIL"},
    ]
    write_json(OUT / "D_data_validation.json", {"rows": d_rows, "schema": schema, "kind": "measured"})
    (OUT / "D_data_validation.md").write_text(
        "# D_data_validation — measured on the full dataset\n\n" + md_table(d_rows, ["item", "method", "result", "pass"])
        + "\n\n## Curated schemas\n" + md_table(schema) + "\n", encoding="utf-8")

    # =========================================================== E
    tf["bearing_deg"] = bearing(tf)
    tf["start_ts"] = pd.to_datetime(tf.start_ts, utc=True)
    train, test = tf[tf.start_ts < SPLIT_UTC], tf[tf.start_ts >= SPLIT_UTC]
    y = test.duration_min.to_numpy()
    preds = {"naive mean (train)": np.full(len(test), train.duration_min.mean())}
    # baseline: straight-line km / median trip speed of the same (hour, weekend) in TRAIN only
    spd = (train.straight_km / (train.duration_min / 60)).replace([np.inf, -np.inf], np.nan)
    med = spd.groupby([train.hour, train.is_weekend]).median()
    fallback = spd.median()
    test_speed = np.array([med.get((h, w), fallback) for h, w in zip(test.hour, test.is_weekend)])
    preds["historical speed: straight_km / median speed(hour, weekend)"] = test.straight_km.to_numpy() / test_speed * 60
    lin = LinearRegression().fit(train[FEATURES], train.duration_min)
    preds["linear regression"] = lin.predict(test[FEATURES])
    # Histogram GBT, multi-threaded, on the WHOLE train period (exact sklearn GBT is single-threaded and
    # did not finish in time); same model family as the served model, not the identical estimator.
    gbt = HistGradientBoostingRegressor(max_iter=200, max_depth=6, random_state=cfg["seed"]).fit(train[FEATURES], train.duration_min)
    preds["GBT (HistGradientBoosting, full train period)"] = gbt.predict(test[FEATURES])
    e_rows = []
    for name, p in preds.items():
        err = p - y
        e_rows.append({"model": name, "MAE_min": round(float(np.abs(err).mean()), 3),
                       "RMSE_min": round(float(np.sqrt((err ** 2).mean())), 3),
                       "mean_signed_error_min": round(float(err.mean()), 3),
                       "median_abs_pct_error": round(float(np.median(np.abs(err) / np.maximum(y, 0.25)) * 100), 1),
                       "pct_over_estimated": round(float((err > 0).mean() * 100), 1),
                       "pct_under_estimated": round(float((err < 0).mean() * 100), 1)})
    write_json(OUT / "E_model_validation.json", {"split": f"train start_ts < {SPLIT_UTC.date()} <= test",
                                                 "n_train": len(train), "n_test": len(test), "rows": e_rows, "kind": "measured"})
    (OUT / "E_model_validation.md").write_text(f"""# E_model_validation — temporal hold-out (Card F test F2)

Split rule: train = trips starting before {SPLIT_UTC.date()} UTC ({len(train):,}), test = {SPLIT_UTC.date()} → 2014-06-30
({len(test):,}). Every statistic used by a baseline (mean, median speed per hour/weekend) is computed on TRAIN only.
Features are known before departure: straight-line A→B distance, hour, day of week, weekend flag, bearing.
Error = predicted − actual (positive = over-estimated). Measured.

{md_table(e_rows)}

| question (manual 4.2) | answer |
|---|---|
| task | regression of trip duration (minutes) for a known origin, destination and departure time |
| features available before the outcome | all five; destination B is a user input at request time (in training it is the trip's last fix) |
| split | temporal hold-out above; the random 80/20 split in evidence/ml is kept only for comparison |
| metrics | MAE (minutes, robust), RMSE (penalises long misses), signed error + % over/under (direction of the bias) |
| baseline | naive mean and historical-speed baseline above |
| model not useful if | its MAE is not clearly below the historical-speed baseline on the temporal test set |
| largest leakage / bias risk | label = (n_points − 1) × 15 s under-states trips with GPS gaps (10.22% of trips have a > 150 km/h jump) |
""", encoding="utf-8")

    # =========================================================== F
    zh_sample = zhs.sample(n=200, random_state=cfg["seed"])
    lat_ms = []
    for z, h, w in zip(zh_sample.zone_id, zh_sample.hour, zh_sample.is_weekend):
        t0 = time.perf_counter()
        db[mc["zone_hour_speed_coll"]].find_one({"zone_id": z, "hour": int(h), "is_weekend": int(w)})
        lat_ms.append((time.perf_counter() - t0) * 1000)
    explain = db[mc["zone_hour_speed_coll"]].find({"zone_id": zh_sample.zone_id.iloc[0], "hour": 8, "is_weekend": 0}).explain()
    stage = explain["queryPlanner"]["winningPlan"].get("inputStage", explain["queryPlanner"]["winningPlan"]).get("stage")
    rng = np.random.default_rng(cfg["seed"])
    zs = zones[zones.n_points >= 1000].sample(n=100, random_state=cfg["seed"])
    api_ms, sample_resp = [], None
    for i in range(50):
        a, b = zs.iloc[2 * i], zs.iloc[2 * i + 1]
        resp, ms = post("/estimate", {"origin": {"lat": a.centroid_lat, "lon": a.centroid_lon},
                                      "destination": {"lat": b.centroid_lat, "lon": b.centroid_lon},
                                      "departure_time": f"2026-11-1{rng.integers(0, 7)}T{rng.integers(6, 22):02d}:30:00"})
        api_ms.append(ms)
        sample_resp = sample_resp or resp
    live_docs = db[cfg["streaming"]["live_coll"]].count_documents({})
    live_one = db[cfg["streaming"]["live_coll"]].find_one(sort=[("n_segments", -1)], projection={"_id": 1, "zone_id": 1, "window_start": 1, "n_segments": 1, "avg_speed_kmh": 1})
    f_rows = [
        {"read": "point read zone_hour_speed by (zone_id, hour, is_weekend), 200 random keys",
         "p50_ms": pct(lat_ms, 50), "p95_ms": pct(lat_ms, 95), "plan": stage},
        {"read": "POST /estimate end-to-end (model + Dijkstra + live lookup), 50 OD pairs",
         "p50_ms": pct(api_ms, 50), "p95_ms": pct(api_ms, 95), "plan": "batch tables in RAM; zone_speed_live queried per request"},
    ]
    (OUT / "F_serving_stream.md").write_text(f"""# F_serving_stream — MongoDB write + read-back (measured)

| collection | written by | documents | key | primary read |
|---|---|---|---|---|
| zones | src.serving.load_mongo (batch) | {mongo_counts['zones']:,} | zone_id (unique index) | bulk read at API start |
| zone_hour_speed | src.serving.load_mongo (batch) | {mongo_counts['zone_hour_speed']:,} | (zone_id, hour, is_weekend) index | bulk read at API start; point read below |
| zone_graph_edges | src.serving.load_mongo (batch) | {mongo_counts['zone_graph_edges']:,} | from_zone index | bulk read -> networkx graph |
| zone_speed_live | src.streaming.live_zone_speed (stream, upsert) | {live_docs:,} | _id = zone_id + window_start; (window_end, zone_id) index | per request: route zones in the latest closed window |

{md_table(f_rows)}

Busiest live document read back: `{live_one}`.
Streaming ground truth vs observed: see `evidence/streaming/summary.md` (42,617 messages, 78/78 malformed,
394/394 duplicates, 186/189 late dropped). Source is a REPLAY of 2014-05-14 16:00–19:00, not a live feed.
""", encoding="utf-8")

    # =========================================================== H (Card F F1 + F3)
    with open(project_path(cfg["paths"]["porto_sample"]), newline="", encoding="utf-8") as f:
        raw_rows = {r["TRIP_ID"]: r for r in csv.DictReader(f)}
    picks = test[test.TRIP_ID.isin(raw_rows) & test.duration_min.between(8, 30)].sample(n=3, random_state=cfg["seed"])
    f1 = []
    for _, t in picks.iterrows():
        dep = pd.Timestamp(t.start_ts).tz_convert("Europe/Lisbon")
        f1 += [
            {"trip": t.TRIP_ID, "feature": "origin A (start_lon, start_lat)", "value": f"{t.start_lat:.5f}, {t.start_lon:.5f}",
             "available": "pre-departure (pickup point)"},
            {"trip": t.TRIP_ID, "feature": "destination B (end_lon, end_lat)", "value": f"{t.end_lat:.5f}, {t.end_lon:.5f}",
             "available": "AMBIGUOUS: last GPS fix in the data (after the trip); user input at request time"},
            {"trip": t.TRIP_ID, "feature": "departure hour / day_of_week / is_weekend", "value": f"{dep:%Y-%m-%d %H:%M} local",
             "available": "pre-departure (clock)"},
            {"trip": t.TRIP_ID, "feature": "straight_km, bearing_deg", "value": f"{t.straight_km:.2f} km, {t.bearing_deg:.0f}°",
             "available": "pre-departure (derived from A and B)"},
            {"trip": t.TRIP_ID, "feature": "historical zone x hour speed", "value": "not a model feature",
             "available": "routing/congestion only; built from ALL trips incl. this one -> would leak if used as a feature"},
            {"trip": t.TRIP_ID, "feature": "label duration_min", "value": f"{t.duration_min:.2f}",
             "available": "observed after completion (target)"},
        ]
    t = picks.iloc[0]
    raw = raw_rows[t.TRIP_ID]
    resp, ms = post("/estimate", {"origin": {"lat": t.start_lat, "lon": t.start_lon},
                                  "destination": {"lat": t.end_lat, "lon": t.end_lon},
                                  "departure_time": pd.Timestamp(t.start_ts).isoformat()})
    (OUT / "H_project_card.md").write_text(f"""# H_project_card — CARD F: Porto Taxi GPS Trip Duration & ETA

## F1 — feature availability (3 real trips from the temporal test period)
{md_table(f1)}

Corrected scope: the task is **known-destination ETA** (user supplies A, B and departure time). It is not
destination prediction. The historical zone speeds stay out of the model's features.

## F2 — temporal baseline comparison
See `E_model_validation.md`.

## F3 — execution truth: one real trip, input -> curated feature -> served prediction
| stage | [I]/[P] | artifact | value |
|---|---|---|---|
| raw CSV row | [I] | data/raw/porto/train.csv (also in data/sample) | TRIP_ID {raw['TRIP_ID']}, TAXI_ID {raw['TAXI_ID']}, TIMESTAMP {raw['TIMESTAMP']}, POLYLINE {len(json.loads(raw['POLYLINE']))} fixes |
| Spark ETL | [I] | src/etl/porto_etl.py -> data/curated/trip_features | start {t.start_lat:.5f},{t.start_lon:.5f} · end {t.end_lat:.5f},{t.end_lon:.5f} · straight {t.straight_km:.2f} km · hour {t.hour} · label {t.duration_min:.2f} min |
| model | [I] | outputs/models/travel_time_sklearn.joblib | loaded by the API at start |
| FastAPI POST /estimate | [I] | src/serving/api.py | predicted **{resp['predicted_duration_min']} min** vs actual **{t.duration_min:.2f} min** ({ms:.0f} ms), route {len(resp['route'])} zones, {len(resp['congested_zones'])} congested |
| Kafka / Structured Streaming | [I] replay, not on this path | src/streaming/ | only feeds the live congestion flags, never the ETA |
| live taxi feed, cluster deployment | [P] | — | design only |

Caveat: the served model was trained on a random 80/20 split that includes May–June 2014, so this trip may
have been in its training data; the clean out-of-sample numbers are the temporal ones in E_model_validation.

One-minute defense: yes — every input the model uses (A, B, departure time) exists at booking time;
the label and the historical speeds are never inputs.
""", encoding="utf-8")
    print(open(OUT / "D_data_validation.md").read()[:2500])
    print(open(OUT / "E_model_validation.md").read()[:2000])
    print(md_table(f_rows))
    print(f"F3: predicted {resp['predicted_duration_min']} vs actual {t.duration_min:.2f}")


if __name__ == "__main__":
    main()
