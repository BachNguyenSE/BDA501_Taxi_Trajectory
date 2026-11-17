"""Step 6a — Replay producer: historical Porto trips -> Kafka, as a fleet's GPS units would send them.

This is a REPLAY of recorded data, not a live feed (the live fleet feed is design-only).
Each message is one GPS fix of one taxi, keyed by TAXI_ID (per-vehicle ordering within a
partition), emitted in event-time order and paced by `speedup`. A unit also reports its
previous fix, so a consumer can compute the 15 s segment without per-trip state.

Junk is injected on purpose, with a fixed seed, so the consumer's handling can be measured:
  duplicate  the same fix re-sent 0-30 s (event time) later   -> must be dropped by dedup
  late       the fix is held back `late_minutes`               -> must be dropped by the watermark
  malformed  an unparseable payload                            -> must be counted and skipped
The ground truth (what was sent, what a correct consumer must aggregate) is written to
evidence/streaming/producer_manifest.json.

Run (inside the spark container, kafka must be up):
    python -m src.streaming.producer --reset            # replay window from conf/pipeline.yaml
    python -m src.streaming.producer --sample --speedup 600
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from src.common.evidence import write_json
from src.common.spark_session import load_config, project_path

LOCAL_TZ = ZoneInfo("Europe/Lisbon")
csv.field_size_limit(10**8)


def haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 2 * 6371.0 * math.asin(math.sqrt(a))


def load_pings(path: str, start_epoch: int, end_epoch: int, step_s: int) -> tuple[list[dict], int]:
    """All fixes of the trips that START in [start_epoch, end_epoch), sorted by event time."""
    pings, trips = [], 0
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["MISSING_DATA"] == "True":
                continue
            ts = int(row["TIMESTAMP"])
            if not start_epoch <= ts < end_epoch:
                continue
            poly = json.loads(row["POLYLINE"])
            if len(poly) < 2:
                continue
            trips += 1
            for seq, (lon, lat) in enumerate(poly):
                prev = poly[seq - 1] if seq else None
                pings.append({
                    "taxi_id": row["TAXI_ID"], "trip_id": row["TRIP_ID"], "seq": seq,
                    "event_ts": ts + seq * step_s, "lon": lon, "lat": lat,
                    "prev_lon": prev[0] if prev else None, "prev_lat": prev[1] if prev else None,
                    "s_since_prev": step_s if prev else None,
                })
    pings.sort(key=lambda p: (p["event_ts"], p["trip_id"], p["seq"]))
    return pings, trips


def is_valid_segment(p: dict, bbox: dict, max_speed_kmh: float) -> bool:
    """The consumer's segment rule, evaluated on the producer side to build the ground truth."""
    if p["prev_lon"] is None:
        return False
    inside = lambda lon, lat: (bbox["lon_min"] <= lon <= bbox["lon_max"]  # noqa: E731
                               and bbox["lat_min"] <= lat <= bbox["lat_max"])
    if not (inside(p["lon"], p["lat"]) and inside(p["prev_lon"], p["prev_lat"])):
        return False
    km = haversine_km(p["prev_lon"], p["prev_lat"], p["lon"], p["lat"])
    return km * 3600.0 / p["s_since_prev"] <= max_speed_kmh


@dataclass(frozen=True)
class Junk:
    duplicate_rate: float
    late_rate: float
    late_minutes: int
    malformed_rate: float


def schedule(pings: list[dict], junk: Junk, seed: int) -> tuple[list[tuple[float, str, dict | bytes]], dict]:
    """(emit_at_event_ts, kind, payload) for every message; each fix gets at most one junk kind."""
    rng = random.Random(seed)
    out: list[tuple[float, str, dict | bytes]] = []
    n = {"on_time": 0, "late": 0, "duplicate_copies": 0, "malformed": 0}
    for p in pings:
        r = rng.random()
        if r < junk.late_rate:
            out.append((p["event_ts"] + junk.late_minutes * 60, "late", p))
            n["late"] += 1
            continue
        out.append((p["event_ts"], "on_time", p))
        n["on_time"] += 1
        if r < junk.late_rate + junk.duplicate_rate:
            out.append((p["event_ts"] + rng.uniform(0, 30), "duplicate", p))
            n["duplicate_copies"] += 1
        if rng.random() < junk.malformed_rate:
            garbage = json.dumps(p)[: rng.randint(5, 40)].encode()  # truncated JSON
            out.append((p["event_ts"] + 0.5, "malformed", garbage))
            n["malformed"] += 1
    out.sort(key=lambda m: m[0])
    return out, n


def ensure_topic(bootstrap: str, topic: str, partitions: int, reset: bool) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="porto-replay-admin")
    if reset:
        try:
            admin.delete_topics([topic])
            time.sleep(3)
        except UnknownTopicOrPartitionError:
            pass
    for _ in range(10):
        try:
            admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
            break
        except TopicAlreadyExistsError:
            if not reset:
                break
            time.sleep(2)  # deletion is asynchronous
    admin.close()


def main() -> None:
    cfg = load_config()
    sc, pc = cfg["streaming"], cfg["porto"]
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", action="store_true", help="read data/sample/porto_sample.csv")
    ap.add_argument("--start-local", default=sc["replay_start_local"])
    ap.add_argument("--hours", type=float, default=sc["replay_hours"])
    ap.add_argument("--speedup", type=float, default=sc["speedup"])
    ap.add_argument("--reset", action="store_true", help="delete and recreate the topic first")
    args = ap.parse_args()

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", sc["bootstrap"])
    path = str(project_path(cfg["paths"]["porto_sample"] if args.sample else cfg["paths"]["porto_raw"]))
    start = datetime.fromisoformat(args.start_local).replace(tzinfo=LOCAL_TZ)
    end = start + timedelta(hours=args.hours)
    junk = Junk(**sc["junk"])

    t_load = time.time()
    pings, trips = load_pings(path, int(start.timestamp()), int(end.timestamp()), pc["sampling_interval_s"])
    if not pings:
        raise SystemExit(f"no trips start in {start} .. {end} in {path}")
    messages, counts = schedule(pings, junk, cfg["seed"])
    late_ids = {(p["trip_id"], p["seq"]) for _, kind, p in messages if kind == "late"}
    valid = [p for p in pings if is_valid_segment(p, pc["bbox"], pc["max_segment_speed_kmh"])]
    expected_on_time_segments = sum((p["trip_id"], p["seq"]) not in late_ids for p in valid)
    print(f"[producer] {trips} trips, {len(pings)} fixes, {len(messages)} messages "
          f"(loaded in {time.time() - t_load:.0f}s) -> {sc['topic']} @ {bootstrap}")

    from kafka import KafkaProducer
    ensure_topic(bootstrap, sc["topic"], sc["partitions"], args.reset)
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", linger_ms=20,
                             key_serializer=str.encode,
                             value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode())

    t0_event, t0_wall = messages[0][0], time.time()
    for emit_at, _kind, payload in messages:
        wait = (emit_at - t0_event) / args.speedup - (time.time() - t0_wall)
        if wait > 0:
            time.sleep(wait)
        key = payload["taxi_id"] if isinstance(payload, dict) else "malformed"
        producer.send(sc["topic"], key=key, value=payload)
    producer.flush()
    producer.close()
    wall = time.time() - t0_wall

    manifest = {
        "kind": "REPLAY of historical data (not a live feed)",
        "source": path, "topic": sc["topic"], "partitions": sc["partitions"], "key": "TAXI_ID",
        "replay_window_local": [start.isoformat(), end.isoformat()],
        "speedup": args.speedup, "wall_clock_s": round(wall, 1), "seed": cfg["seed"],
        "junk_config": sc["junk"],
        "trips": trips, "unique_fixes": len(pings), "messages_total": len(messages),
        "messages_by_kind": counts,
        "valid_segments_total": len(valid),
        "expected_on_time_segments": expected_on_time_segments,
        "late_valid_segments": len(valid) - expected_on_time_segments,
        "event_time_range_utc": [datetime.fromtimestamp(pings[0]["event_ts"], timezone.utc).isoformat(),
                                 datetime.fromtimestamp(pings[-1]["event_ts"], timezone.utc).isoformat()],
    }
    write_json(project_path("evidence/streaming/producer_manifest.json"), manifest)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
