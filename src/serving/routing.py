"""Step 5b — in-memory routing graph + congestion lookup, backed by MongoDB.

Loaded once at API startup (`src/serving/api.py`): builds a networkx DiGraph from
`zone_graph_edges` (edge weight = avg traversal time in seconds) and keeps `zone_hour_speed`
indexed by (zone_id, hour, is_weekend) so congestion can be evaluated for any requested
departure time without re-querying Mongo per zone.
"""
from __future__ import annotations

import bisect
import math
from dataclasses import dataclass

import networkx as nx

from src.common.mongo import get_db
from src.common.spark_session import load_config


def haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


@dataclass
class Zone:
    zone_id: str
    gx: int
    gy: int
    lon: float
    lat: float


class RoutingGraph:
    """Zone graph + per-hour congestion, loaded from MongoDB into memory."""

    def __init__(self) -> None:
        cfg = load_config()["mongo"]
        db = get_db()

        self.zones: dict[str, Zone] = {
            z["zone_id"]: Zone(z["zone_id"], z["gx"], z["gy"], z["centroid_lon"], z["centroid_lat"])
            for z in db[cfg["zones_coll"]].find({}, {"_id": 0})
        }
        self.graph = nx.DiGraph()
        for z in self.zones.values():
            self.graph.add_node(z.zone_id, lon=z.lon, lat=z.lat)
        for e in db[cfg["zone_graph_coll"]].find({}, {"_id": 0}):
            self.graph.add_edge(e["from_zone"], e["to_zone"], weight=e["avg_time_s"], avg_speed_kmh=e["avg_speed_kmh"])

        # speed[(zone_id, hour, is_weekend)] = avg_speed_kmh ; also keep per-hour percentile thresholds
        self.speed: dict[tuple[str, int, int], float] = {}
        self._speeds_by_bucket: dict[tuple[int, int], list[float]] = {}
        for r in db[cfg["zone_hour_speed_coll"]].find({}, {"_id": 0}):
            key = (r["zone_id"], int(r["hour"]), int(r["is_weekend"]))
            self.speed[key] = r["avg_speed_kmh"]
            bucket = (int(r["hour"]), int(r["is_weekend"]))
            self._speeds_by_bucket.setdefault(bucket, []).append(r["avg_speed_kmh"])

        self._congestion_percentile = load_config()["etl"]["congestion_percentile"]
        self._congestion_threshold: dict[tuple[int, int], float] = {}
        self._speeds_sorted: dict[tuple[int, int], list[float]] = {}
        for bucket, speeds in self._speeds_by_bucket.items():
            speeds = sorted(speeds)
            self._speeds_sorted[bucket] = speeds
            idx = max(0, min(len(speeds) - 1, int(len(speeds) * self._congestion_percentile)))
            self._congestion_threshold[bucket] = speeds[idx]

    def nearest_zone(self, lon: float, lat: float) -> str | None:
        if not self.zones:
            return None
        return min(self.zones.values(), key=lambda z: haversine_km(lon, lat, z.lon, z.lat)).zone_id

    def is_congested(self, zone_id: str, hour: int, is_weekend: int) -> bool:
        bucket = (hour, is_weekend)
        threshold = self._congestion_threshold.get(bucket)
        speed = self.speed.get((zone_id, hour, is_weekend))
        if threshold is None or speed is None:
            return False
        return speed <= threshold

    def congestion_score(self, zone_id: str, hour: int, is_weekend: int) -> float:
        """0 (free-flowing) .. 1 (slowest observed zone for this hour/weekend bucket)."""
        bucket = (hour, is_weekend)
        speeds = self._speeds_sorted.get(bucket)
        speed = self.speed.get((zone_id, hour, is_weekend))
        if not speeds or speed is None or len(speeds) < 2:
            return 0.0
        idx = bisect.bisect_left(speeds, speed)
        return 1.0 - idx / (len(speeds) - 1)

    def shortest_path(self, origin_zone: str, dest_zone: str, max_hops: int) -> list[str] | None:
        if origin_zone == dest_zone:
            return [origin_zone]
        try:
            path = nx.shortest_path(self.graph, origin_zone, dest_zone, weight="weight")
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None
        if len(path) > max_hops:
            return None
        return path

    def route_time_s(self, path: list[str]) -> float:
        total = 0.0
        for a, b in zip(path[:-1], path[1:]):
            total += self.graph[a][b]["weight"]
        return total
