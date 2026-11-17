"""Shared SparkSession factory and config loader."""
from __future__ import annotations

import os
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_config() -> dict:
    with open(PROJECT_ROOT / "conf" / "pipeline.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def project_path(relative: str) -> Path:
    return PROJECT_ROOT / relative


def get_spark(app_suffix: str = "", extra_conf: dict | None = None) -> SparkSession:
    cfg = load_config()["spark"]
    builder = (
        SparkSession.builder.appName(f"{cfg['app_name']}-{app_suffix}".rstrip("-"))
        .master(os.environ.get("SPARK_MASTER", cfg["master"]))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", cfg["driver_memory"]))
        .config("spark.sql.shuffle.partitions", cfg["shuffle_partitions"])
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
    )
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
