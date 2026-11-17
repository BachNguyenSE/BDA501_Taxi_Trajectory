"""MongoDB client factory shared by the loader and the serving API."""
from __future__ import annotations

import os

from pymongo import MongoClient
from pymongo.database import Database

from src.common.spark_session import load_config


def get_db() -> Database:
    cfg = load_config()["mongo"]
    uri = os.environ.get("MONGO_URI", cfg["uri"])
    client = MongoClient(uri)
    return client[cfg["db"]]
