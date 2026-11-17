"""Step 4 — Spark ML travel-time regression: baseline (Linear) vs GBT.

Reads `data/curated/trip_features` (written by `src.etl.porto_etl`), engineers the
request-time features (the ones an end user actually supplies: origin, destination,
departure time), trains/evaluates both models with Spark ML, and additionally fits a
lightweight scikit-learn GBT on a sample collected to the driver — this is the model the
FastAPI serving layer loads, so a request does not need to spin up a Spark session.

Run (inside the spark container):
    python -m src.ml.train_travel_time
"""
from __future__ import annotations

import json

import joblib
import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.sql import functions as F
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import LinearRegression as SkLinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from src.common.evidence import md_table, write_json
from src.common.spark_session import get_spark, load_config, project_path
from src.eda.porto_profile import haversine_km

FEATURE_COLS = ["straight_km", "hour", "day_of_week", "is_weekend", "bearing_deg"]
LABEL_COL = "duration_min"


def add_features(df):
    return df.withColumn(
        "bearing_deg",
        F.expr(
            "degrees(atan2("
            "sin(radians(end_lon - start_lon)) * cos(radians(end_lat)),"
            "cos(radians(start_lat)) * sin(radians(end_lat))"
            " - sin(radians(start_lat)) * cos(radians(end_lat)) * cos(radians(end_lon - start_lon))"
            "))",
        ),
    )


def main() -> None:
    cfg = load_config()
    mc = cfg["ml"]
    curated = project_path(cfg["paths"]["curated"])
    models_dir = project_path(cfg["paths"]["models"])
    evidence_dir = project_path(cfg["paths"]["evidence_ml"])
    models_dir.mkdir(parents=True, exist_ok=True)

    spark = get_spark("ml-travel-time")
    df = add_features(spark.read.parquet(str(curated / "trip_features")))
    train_sp, test_sp = df.randomSplit([mc["train_fraction"], 1 - mc["train_fraction"]], seed=cfg["seed"])

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    evaluator_rmse = RegressionEvaluator(labelCol=LABEL_COL, predictionCol="prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol=LABEL_COL, predictionCol="prediction", metricName="mae")
    evaluator_r2 = RegressionEvaluator(labelCol=LABEL_COL, predictionCol="prediction", metricName="r2")

    results = []
    spark_models = {
        "linear_regression": LinearRegression(labelCol=LABEL_COL, featuresCol="features"),
        "gbt_regressor": GBTRegressor(labelCol=LABEL_COL, featuresCol="features", maxIter=60, maxDepth=6, seed=cfg["seed"]),
    }
    for name, estimator in spark_models.items():
        pipeline = Pipeline(stages=[assembler, estimator])
        model = pipeline.fit(train_sp)
        pred = model.transform(test_sp)
        results.append({
            "model": name,
            "rmse_min": round(evaluator_rmse.evaluate(pred), 3),
            "mae_min": round(evaluator_mae.evaluate(pred), 3),
            "r2": round(evaluator_r2.evaluate(pred), 4),
        })
        if name == "gbt_regressor":
            model.write().overwrite().save(str(models_dir / "spark_gbt_travel_time"))

    # ---------- lightweight sklearn model for request-time serving (no Spark needed) ----------
    sample_n = mc["sklearn_sample_rows"]
    pdf = df.select(*FEATURE_COLS, LABEL_COL).sample(fraction=1.0, seed=cfg["seed"]).limit(sample_n).toPandas()
    X = pdf[FEATURE_COLS].to_numpy(dtype=float)
    y = pdf[LABEL_COL].to_numpy(dtype=float)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=1 - mc["train_fraction"], random_state=cfg["seed"])

    sk_baseline = SkLinearRegression().fit(X_train, y_train)
    sk_gbt = GradientBoostingRegressor(n_estimators=150, max_depth=4, random_state=cfg["seed"]).fit(X_train, y_train)

    for name, model in [("sklearn_linear_regression", sk_baseline), ("sklearn_gbt", sk_gbt)]:
        pred = model.predict(X_test)
        results.append({
            "model": name,
            "rmse_min": round(float(np.sqrt(mean_squared_error(y_test, pred))), 3),
            "mae_min": round(float(mean_absolute_error(y_test, pred)), 3),
            "r2": round(float(r2_score(y_test, pred)), 4),
        })

    joblib.dump(sk_gbt, models_dir / "travel_time_sklearn.joblib")
    (models_dir / "travel_time_sklearn.features.json").write_text(
        json.dumps({"feature_cols": FEATURE_COLS, "label_col": LABEL_COL}, indent=2), encoding="utf-8"
    )

    write_json(evidence_dir / "travel_time_model.json", {
        "n_rows": df.count(), "n_sklearn_sample": len(pdf), "feature_cols": FEATURE_COLS, "results": results,
    })
    summary = f"""# Travel-time regression — model comparison

Trained on `data/curated/trip_features` (features = straight-line distance, hour, day of
week, weekend flag, bearing — exactly what a user supplies: origin, destination, departure
time). Train/test split {mc['train_fraction']:.0%}/{1 - mc['train_fraction']:.0%}, seed {cfg['seed']}.

{md_table(results)}

The `sklearn_gbt` model is the one saved to `outputs/models/travel_time_sklearn.joblib`
and loaded by the FastAPI serving layer (`src/serving/api.py`) — fast to load, no Spark
session needed per request. `spark_gbt_travel_time` (saved under `outputs/models/`) is the
full-data Spark ML model kept as training evidence.
"""
    (evidence_dir / "summary.md").write_text(summary, encoding="utf-8")
    print(summary)
    spark.stop()


if __name__ == "__main__":
    main()
