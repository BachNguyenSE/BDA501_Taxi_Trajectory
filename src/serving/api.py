"""Step 5c — FastAPI serving layer: estimate travel time, congestion, route (+ live speed layer).

POST /estimate {origin:{lat,lon}, destination:{lat,lon}, departure_time: ISO8601}
  -> predicted_duration_min (sklearn GBT, trained by src.ml.train_travel_time),
     route (sequence of zone centroids, from the MongoDB-backed graph),
     congested_zones (zones on the route flagged by src.serving.routing.RoutingGraph),
     live (speed layer): for the route's zones, the latest event-time window written by
     src.streaming.live_zone_speed, compared with the historical speed of the same zone at the
     same local hour. The live layer only flags zones; it does NOT change the ETA.

Batch-layer tables are read once at startup; zone_speed_live is queried per request
(point lookup by zone_id in the latest window). Serves the Leaflet UI (`static/`) at "/".
    uvicorn src.serving.api:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import joblib
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.common.mongo import get_db
from src.common.spark_session import load_config, project_path
from src.serving.routing import RoutingGraph, haversine_km

LOCAL_TZ = ZoneInfo("Europe/Lisbon")

cfg = load_config()
models_dir = project_path(cfg["paths"]["models"])
_model = joblib.load(models_dir / "travel_time_sklearn.joblib")
_feature_meta = json.loads((models_dir / "travel_time_sklearn.features.json").read_text(encoding="utf-8"))
_graph = RoutingGraph()
_live = get_db()[cfg["streaming"]["live_coll"]]

app = FastAPI(title="BDA501 Taxi Travel-Time & Routing", version="1.0")


class LatLon(BaseModel):
    lat: float
    lon: float


class EstimateRequest(BaseModel):
    origin: LatLon
    destination: LatLon
    departure_time: datetime


class EstimateResponse(BaseModel):
    predicted_duration_min: float
    straight_line_km: float
    departure_local_hour: int
    is_weekend: bool
    route: list[dict]
    congested_zones: list[dict]
    live: dict


def _live_layer(route: list[dict]) -> dict:
    """Annotate route zones in place with the latest live window; return the layer summary."""
    sc = cfg["streaming"]
    # The newest window is still filling; use the most recent one before it (closed by the watermark).
    recent = list(_live.find({}, {"window_start": 1, "window_end": 1}).sort("window_end", -1).limit(1))
    latest = None
    if recent:
        latest = _live.find_one({"window_end": {"$lt": recent[0]["window_end"]}}, sort=[("window_end", -1)],
                                projection={"window_start": 1, "window_end": 1}) or recent[0]
    if not latest or not route:
        return {"available": False, "note": "no live data — run scripts/run_streaming.sh"}
    ws = latest["window_start"].replace(tzinfo=timezone.utc).astimezone(LOCAL_TZ)
    we = latest["window_end"].replace(tzinfo=timezone.utc).astimezone(LOCAL_TZ)
    docs = {d["zone_id"]: d for d in _live.find(
        {"window_end": latest["window_end"], "zone_id": {"$in": [z["zone_id"] for z in route]},
         "n_segments": {"$gte": sc["min_live_segments"]}},
        {"_id": 0, "zone_id": 1, "avg_speed_kmh": 1, "n_segments": 1, "n_taxis": 1})}
    hour, weekend = ws.hour, int(ws.isoweekday() in (6, 7))
    slower = []
    for z in route:
        d = docs.get(z["zone_id"])
        hist = _graph.speed.get((z["zone_id"], hour, weekend))
        if not d or not hist:
            continue
        z["live_speed_kmh"] = round(d["avg_speed_kmh"], 1)
        z["hist_speed_kmh"] = round(hist, 1)
        z["live_ratio"] = round(d["avg_speed_kmh"] / hist, 2)
        z["live_segments"] = d["n_segments"]
        if z["live_ratio"] < sc["slower_than_usual_ratio"]:
            slower.append(z["zone_id"])
    return {
        "available": True,
        "source": f"Kafka replay -> Structured Streaming ({sc['window']} event-time windows)",
        "window_local": [ws.isoformat(), we.isoformat()],
        "zones_on_route_with_data": len(docs),
        "slower_than_usual": slower,
        "slower_than_usual_ratio": sc["slower_than_usual_ratio"],
    }


def _bearing_deg(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlambda = math.radians(lon2 - lon1)
    y = math.sin(dlambda) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlambda)
    return math.degrees(math.atan2(y, x))


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "zones": len(_graph.zones), "edges": _graph.graph.number_of_edges()}


@app.post("/estimate", response_model=EstimateResponse)
def estimate(req: EstimateRequest) -> EstimateResponse:
    o, d = req.origin, req.destination
    dt = req.departure_time
    local_dt = dt.astimezone(LOCAL_TZ) if dt.tzinfo else dt.replace(tzinfo=LOCAL_TZ)
    hour, dow, is_weekend = local_dt.hour, local_dt.isoweekday() % 7 + 1, int(local_dt.isoweekday() in (6, 7))

    straight_km = haversine_km(o.lon, o.lat, d.lon, d.lat)
    if straight_km < 0.01:
        raise HTTPException(400, "origin and destination are the same point")
    bearing = _bearing_deg(o.lon, o.lat, d.lon, d.lat)

    features = [[straight_km, hour, dow, is_weekend, bearing]]
    assert _feature_meta["feature_cols"] == ["straight_km", "hour", "day_of_week", "is_weekend", "bearing_deg"]
    predicted_min = float(_model.predict(features)[0])

    origin_zone = _graph.nearest_zone(o.lon, o.lat)
    dest_zone = _graph.nearest_zone(d.lon, d.lat)
    route, congested = [], []

    def _zone_entry(zid: str) -> dict:
        z = _graph.zones[zid]
        return {
            "zone_id": zid, "lat": z.lat, "lon": z.lon, "gx": z.gx, "gy": z.gy,
            "congestion_score": round(_graph.congestion_score(zid, hour, is_weekend), 3),
        }

    if origin_zone and dest_zone:
        path = _graph.shortest_path(origin_zone, dest_zone, cfg["serving"]["max_route_hops"])
        if path:
            for zid in path:
                entry = _zone_entry(zid)
                route.append(entry)
                if _graph.is_congested(zid, hour, is_weekend):
                    congested.append(entry)
        else:
            route = [_zone_entry(origin_zone), _zone_entry(dest_zone)]

    return EstimateResponse(
        predicted_duration_min=round(predicted_min, 1),
        straight_line_km=round(straight_km, 2),
        departure_local_hour=hour,
        is_weekend=bool(is_weekend),
        route=route,
        congested_zones=congested,
        live=_live_layer(route),
    )


app.mount("/", StaticFiles(directory=str(project_path("static")), html=True), name="static")
