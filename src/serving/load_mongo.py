"""Step 5a — load the curated routing/congestion tables into MongoDB.

Reads `data/curated/{zones,zone_hour_speed,zone_graph_edges}` (plain Parquet, via pandas —
no Spark session needed) and upserts them into MongoDB collections that the serving API
reads at request time.

Run (inside the spark container, mongo must be up):
    python -m src.serving.load_mongo
"""
from __future__ import annotations

import pandas as pd
from pymongo import ASCENDING

from src.common.mongo import get_db
from src.common.spark_session import load_config, project_path


def main() -> None:
    cfg = load_config()
    mc = cfg["mongo"]
    curated = project_path(cfg["paths"]["curated"])
    db = get_db()

    zones = pd.read_parquet(curated / "zones")
    zone_hour_speed = pd.read_parquet(curated / "zone_hour_speed")
    zone_graph_edges = pd.read_parquet(curated / "zone_graph_edges")

    db[mc["zones_coll"]].drop()
    db[mc["zone_hour_speed_coll"]].drop()
    db[mc["zone_graph_coll"]].drop()

    db[mc["zones_coll"]].insert_many(zones.to_dict(orient="records"))
    db[mc["zones_coll"]].create_index([("zone_id", ASCENDING)], unique=True)

    db[mc["zone_hour_speed_coll"]].insert_many(zone_hour_speed.to_dict(orient="records"))
    db[mc["zone_hour_speed_coll"]].create_index([("zone_id", ASCENDING), ("hour", ASCENDING), ("is_weekend", ASCENDING)])

    db[mc["zone_graph_coll"]].insert_many(zone_graph_edges.to_dict(orient="records"))
    db[mc["zone_graph_coll"]].create_index([("from_zone", ASCENDING)])

    print({
        "zones": db[mc["zones_coll"]].count_documents({}),
        "zone_hour_speed": db[mc["zone_hour_speed_coll"]].count_documents({}),
        "zone_graph_edges": db[mc["zone_graph_coll"]].count_documents({}),
    })


if __name__ == "__main__":
    main()
