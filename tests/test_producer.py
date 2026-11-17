from src.streaming.producer import Junk, is_valid_segment, schedule

BBOX = {"lon_min": -8.80, "lon_max": -8.45, "lat_min": 41.05, "lat_max": 41.30}


def ping(seq, lon=-8.61, lat=41.15, prev=(-8.6101, 41.15)):
    return {"taxi_id": "1", "trip_id": "t", "seq": seq, "event_ts": 1_000 + 15 * seq, "lon": lon, "lat": lat,
            "prev_lon": prev[0] if prev else None, "prev_lat": prev[1] if prev else None,
            "s_since_prev": 15 if prev else None}


def test_segment_rule_matches_the_consumer():
    assert is_valid_segment(ping(1), BBOX, 150)
    assert not is_valid_segment(ping(0, prev=None), BBOX, 150)            # first fix has no segment
    assert not is_valid_segment(ping(1, lon=-9.0), BBOX, 150)             # outside bbox
    assert not is_valid_segment(ping(1, lon=-8.50), BBOX, 150)            # ~9 km in 15 s


def test_schedule_is_deterministic_and_counts_add_up():
    pings = [ping(i) for i in range(1, 5001)]
    junk = Junk(duplicate_rate=0.05, late_rate=0.02, late_minutes=30, malformed_rate=0.01)
    m1, n1 = schedule(pings, junk, seed=42)
    m2, n2 = schedule(pings, junk, seed=42)
    assert n1 == n2 and [k for _, k, _ in m1] == [k for _, k, _ in m2]
    assert n1["on_time"] + n1["late"] == len(pings)                        # each fix sent on time XOR late
    assert len(m1) == len(pings) + n1["duplicate_copies"] + n1["malformed"]
    assert all(t1 <= t2 for (t1, _, _), (t2, _, _) in zip(m1, m1[1:]))      # emitted in event-time order


def test_late_fixes_are_held_back_by_late_minutes():
    pings = [ping(i) for i in range(1, 2001)]
    msgs, _ = schedule(pings, Junk(0.0, 0.05, 30, 0.0), seed=1)
    late = [(t, p) for t, k, p in msgs if k == "late"]
    assert late and all(t == p["event_ts"] + 30 * 60 for t, p in late)
