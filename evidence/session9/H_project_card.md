# H_project_card — CARD F: Porto Taxi GPS Trip Duration & ETA

## F1 — feature availability (3 real trips from the temporal test period)
| trip | feature | value | available |
|---|---|---|---|
| 1401249804620000514 | origin A (start_lon, start_lat) | 41.16677, -8.60904 | pre-departure (pickup point) |
| 1401249804620000514 | destination B (end_lon, end_lat) | 41.14886, -8.58562 | AMBIGUOUS: last GPS fix in the data (after the trip); user input at request time |
| 1401249804620000514 | departure hour / day_of_week / is_weekend | 2014-05-28 05:03 local | pre-departure (clock) |
| 1401249804620000514 | straight_km, bearing_deg | 2.79 km, 135° | pre-departure (derived from A and B) |
| 1401249804620000514 | historical zone x hour speed | not a model feature | routing/congestion only; built from ALL trips incl. this one -> would leak if used as a feature |
| 1401249804620000514 | label duration_min | 10.50 | observed after completion (target) |
| 1401440725620000235 | origin A (start_lon, start_lat) | 41.15434, -8.64939 | pre-departure (pickup point) |
| 1401440725620000235 | destination B (end_lon, end_lat) | 41.16555, -8.58878 | AMBIGUOUS: last GPS fix in the data (after the trip); user input at request time |
| 1401440725620000235 | departure hour / day_of_week / is_weekend | 2014-05-30 10:05 local | pre-departure (clock) |
| 1401440725620000235 | straight_km, bearing_deg | 5.22 km, 76° | pre-departure (derived from A and B) |
| 1401440725620000235 | historical zone x hour speed | not a model feature | routing/congestion only; built from ALL trips incl. this one -> would leak if used as a feature |
| 1401440725620000235 | label duration_min | 11.00 | observed after completion (target) |
| 1400233248620000517 | origin A (start_lon, start_lat) | 41.16115, -8.60478 | pre-departure (pickup point) |
| 1400233248620000517 | destination B (end_lon, end_lat) | 41.15659, -8.63144 | AMBIGUOUS: last GPS fix in the data (after the trip); user input at request time |
| 1400233248620000517 | departure hour / day_of_week / is_weekend | 2014-05-16 10:40 local | pre-departure (clock) |
| 1400233248620000517 | straight_km, bearing_deg | 2.29 km, -103° | pre-departure (derived from A and B) |
| 1400233248620000517 | historical zone x hour speed | not a model feature | routing/congestion only; built from ALL trips incl. this one -> would leak if used as a feature |
| 1400233248620000517 | label duration_min | 10.00 | observed after completion (target) |

Corrected scope: the task is **known-destination ETA** (user supplies A, B and departure time). It is not
destination prediction. The historical zone speeds stay out of the model's features.

## F2 — temporal baseline comparison
See `E_model_validation.md`.

## F3 — execution truth: one real trip, input -> curated feature -> served prediction
| stage | [I]/[P] | artifact | value |
|---|---|---|---|
| raw CSV row | [I] | data/raw/porto/train.csv (also in data/sample) | TRIP_ID 1401249804620000514, TAXI_ID 20000514, TIMESTAMP 1401249804, POLYLINE 43 fixes |
| Spark ETL | [I] | src/etl/porto_etl.py -> data/curated/trip_features | start 41.16677,-8.60904 · end 41.14886,-8.58562 · straight 2.79 km · hour 5 · label 10.50 min |
| model | [I] | outputs/models/travel_time_sklearn.joblib | loaded by the API at start |
| FastAPI POST /estimate | [I] | src/serving/api.py | predicted **9.4 min** vs actual **10.50 min** (8 ms), route 5 zones, 1 congested |
| Kafka / Structured Streaming | [I] replay, not on this path | src/streaming/ | only feeds the live congestion flags, never the ETA |
| live taxi feed, cluster deployment | [P] | — | design only |

Caveat: the served model was trained on a random 80/20 split that includes May–June 2014, so this trip may
have been in its training data; the clean out-of-sample numbers are the temporal ones in E_model_validation.

One-minute defense: yes — every input the model uses (A, B, departure time) exists at booking time;
the label and the historical speeds are never inputs.
