# Travel-time regression — model comparison

Trained on `data/curated/trip_features` (features = straight-line distance, hour, day of
week, weekend flag, bearing — exactly what a user supplies: origin, destination, departure
time). Train/test split 80%/20%, seed 42.

| model | rmse_min | mae_min | r2 |
|---|---|---|---|
| linear_regression | 8.22 | 4.35 | 0.20 |
| gbt_regressor | 7.95 | 4.05 | 0.25 |
| sklearn_linear_regression | 8.01 | 4.30 | 0.19 |
| sklearn_gbt | 7.80 | 4.02 | 0.24 |

The `sklearn_gbt` model is the one saved to `outputs/models/travel_time_sklearn.joblib`
and loaded by the FastAPI serving layer (`src/serving/api.py`) — fast to load, no Spark
session needed per request. `spark_gbt_travel_time` (saved under `outputs/models/`) is the
full-data Spark ML model kept as training evidence.
