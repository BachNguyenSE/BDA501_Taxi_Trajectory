"""Step 1 — shared logic of the zone x hour speed MapReduce job.

Standard library only, so `mapper.py` / `reducer.py` run unchanged under Hadoop
Streaming on any worker node (no PySpark, no PyYAML needed there).

Record flow
-----------
    CSV line (one Porto trip)
      └─ map ──> one record per valid 15 s GPS segment
                 key   = "<row>-<col>-<hour>"   (grid cell of the segment midpoint,
                                                 local Porto hour of the segment start)
                 value = (1, distance_km, duration_s)
      └─ combine / reduce ──> per key: (n_segments, total_km, total_s)
                 avg_speed_kmh = total_km / total_s * 3600   (distance-weighted, i.e. the
                                                              correct "space-mean" speed)
"""
from __future__ import annotations

import csv
import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Iterator
from zoneinfo import ZoneInfo

EARTH_R_KM = 6371.0
LOCAL_TZ = ZoneInfo("Europe/Lisbon")  # Porto local time (UTC+0 winter / UTC+1 summer)
N_FIELDS = 9  # TRIP_ID,CALL_TYPE,ORIGIN_CALL,ORIGIN_STAND,TAXI_ID,TIMESTAMP,DAY_TYPE,MISSING_DATA,POLYLINE
COUNTER_GROUP = "porto"


@dataclass(frozen=True)
class Params:
    """Job parameters. Defaults mirror conf/pipeline.yaml (porto + mapreduce sections)."""

    lon_min: float = -8.80
    lon_max: float = -8.45
    lat_min: float = 41.05
    lat_max: float = 41.30
    grid_deg: float = 0.005
    step_s: int = 15
    max_speed_kmh: float = 150.0

    @property
    def max_segment_km(self) -> float:
        return self.max_speed_kmh * self.step_s / 3600.0

    def inside(self, lon: float, lat: float) -> bool:
        return self.lon_min <= lon <= self.lon_max and self.lat_min <= lat <= self.lat_max

    def cell_of(self, lon: float, lat: float) -> tuple[int, int]:
        # floor(a / b), not a // b: float floor-division can differ by one cell on a grid
        # line, and Spark computes FLOOR(a / b) — the cross-check needs identical cells.
        return (math.floor((lat - self.lat_min) / self.grid_deg), math.floor((lon - self.lon_min) / self.grid_deg))

    def cell_center(self, row: int, col: int) -> tuple[float, float]:
        """(lat, lon) of a cell's centre."""
        return (self.lat_min + (row + 0.5) * self.grid_deg, self.lon_min + (col + 0.5) * self.grid_deg)

    def to_cli(self) -> list[str]:
        return [
            "--bbox", f"{self.lon_min},{self.lon_max},{self.lat_min},{self.lat_max}",
            "--grid-deg", str(self.grid_deg),
            "--step-s", str(self.step_s),
            "--max-speed-kmh", str(self.max_speed_kmh),
        ]


def add_param_args(parser) -> None:
    d = Params()
    parser.add_argument("--bbox", default=f"{d.lon_min},{d.lon_max},{d.lat_min},{d.lat_max}",
                        help="lon_min,lon_max,lat_min,lat_max")
    parser.add_argument("--grid-deg", type=float, default=d.grid_deg)
    parser.add_argument("--step-s", type=int, default=d.step_s)
    parser.add_argument("--max-speed-kmh", type=float, default=d.max_speed_kmh)


def params_from_args(args) -> Params:
    lon_min, lon_max, lat_min, lat_max = (float(x) for x in args.bbox.split(","))
    return Params(lon_min, lon_max, lat_min, lat_max, args.grid_deg, args.step_s, args.max_speed_kmh)


def haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 2 * EARTH_R_KM * math.asin(math.sqrt(a))


def make_key(row: int, col: int, hour: int) -> str:
    # Fixed width -> lexicographic order == numeric order, and a single tab-free token,
    # so Hadoop's default "key = text before first tab" partitioning works unchanged.
    return f"{row:03d}-{col:03d}-{hour:02d}"


def split_key(key: str) -> tuple[int, int, int]:
    row, col, hour = key.split("-")
    return int(row), int(col), int(hour)


# ----------------------------------------------------------------------------- map
def map_line(line: str, p: Params, c: Counter) -> Iterator[tuple[str, float]]:
    """Map one raw CSV line to (key, distance_km) per valid segment; updates counters."""
    c["map_input_records"] += 1
    try:
        fields = next(csv.reader([line]))
    except (csv.Error, StopIteration):
        c["skip_malformed_line"] += 1
        return
    if not fields:
        c["skip_malformed_line"] += 1
        return
    if fields[0] == "TRIP_ID":
        c["skip_header"] += 1
        return
    if len(fields) != N_FIELDS:
        c["skip_malformed_line"] += 1
        return
    if fields[7] == "True":
        c["skip_missing_data"] += 1
        return
    try:
        ts = int(fields[5])
        poly = json.loads(fields[8])
        pts = [(float(pt[0]), float(pt[1])) for pt in poly]  # Porto order is [lon, lat]
    except (ValueError, TypeError, IndexError):
        c["skip_malformed_line"] += 1
        return
    if len(pts) < 2:
        c["skip_lt_2_points"] += 1
        return
    c["trips_used"] += 1

    # Local hour of segment i = hour(start + (i-1)*15 s). The UTC offset is taken once per
    # trip (trips are <= a few hours; a DST switch inside a trip shifts at most one hour bucket).
    offset = int(datetime.fromtimestamp(ts, LOCAL_TZ).utcoffset().total_seconds())
    local0 = ts + offset
    step, max_km = p.step_s, p.max_segment_km
    for i in range(1, len(pts)):
        lon1, lat1 = pts[i - 1]
        lon2, lat2 = pts[i]
        c["segments_total"] += 1
        if not (p.inside(lon1, lat1) and p.inside(lon2, lat2)):
            c["segments_outside_bbox"] += 1
            continue
        d = haversine_km(lon1, lat1, lon2, lat2)
        if d > max_km:
            c["segments_gps_jump"] += 1
            continue
        row, col = p.cell_of((lon1 + lon2) / 2, (lat1 + lat2) / 2)
        hour = ((local0 + (i - 1) * step) // 3600) % 24
        c["map_output_records"] += 1
        yield make_key(row, col, hour), d


def format_value(n: int, km: float, sec: int) -> str:
    return f"{n}\t{km:.6f}\t{sec}"


# -------------------------------------------------------------- combine / reduce
def parse_kv(line: str) -> tuple[str, int, float, int]:
    key, n, km, sec = line.rstrip("\n").split("\t")
    return key, int(n), float(km), int(sec)


def group_sorted(lines: Iterable[str]) -> Iterator[tuple[str, int, float, int]]:
    """Sum values of consecutive equal keys (the reduce-side contract of MapReduce).

    Raises if a key re-appears after its group closed, i.e. the input was not sorted /
    grouped by key — the guarantee the shuffle phase is supposed to provide.
    """
    closed: set[str] = set()
    cur, n, km, sec = None, 0, 0.0, 0
    for line in lines:
        if not line.strip():
            continue
        key, n_i, km_i, sec_i = parse_kv(line)
        if key != cur:
            if cur is not None:
                closed.add(cur)
                yield cur, n, km, sec
            if key in closed:
                raise ValueError(f"reducer input not grouped by key: {key!r} seen twice")
            cur, n, km, sec = key, 0, 0.0, 0
        n, km, sec = n + n_i, km + km_i, sec + sec_i
    if cur is not None:
        yield cur, n, km, sec


OUTPUT_HEADER = "cell\trow\tcol\thour\tcenter_lat\tcenter_lon\tn_segments\ttotal_km\ttotal_s\tavg_speed_kmh"


def format_output(key: str, n: int, km: float, sec: int, p: Params) -> str:
    row, col, hour = split_key(key)
    lat, lon = p.cell_center(row, col)
    speed = km / sec * 3600.0 if sec else 0.0
    return (f"{row:03d}-{col:03d}\t{row}\t{col}\t{hour}\t{lat:.4f}\t{lon:.4f}\t"
            f"{n}\t{km:.6f}\t{sec}\t{speed:.3f}")
