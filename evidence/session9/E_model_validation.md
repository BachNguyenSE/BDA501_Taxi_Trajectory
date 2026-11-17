# E_model_validation — temporal hold-out (Card F test F2)

Split rule: train = trips starting before 2014-05-01 UTC (1,343,537), test = 2014-05-01 → 2014-06-30
(303,810). Every statistic used by a baseline (mean, median speed per hour/weekend) is computed on TRAIN only.
Features are known before departure: straight-line A→B distance, hour, day of week, weekend flag, bearing.
Error = predicted − actual (positive = over-estimated). Measured.

| model | MAE_min | RMSE_min | mean_signed_error_min | median_abs_pct_error | pct_over_estimated | pct_under_estimated |
|---|---|---|---|---|---|---|
| naive mean (train) | 5.40 | 9.07 | -0.05 | 37.20 | 62.20 | 37.80 |
| historical speed: straight_km / median speed(hour, weekend) | 5.46 | 9.91 | 0.48 | 31.30 | 51.80 | 48.20 |
| linear regression | 4.35 | 8.16 | 0.16 | 29.70 | 66.40 | 33.60 |
| GBT (HistGradientBoosting, full train period) | 4.02 | 7.87 | 0.07 | 25.50 | 65.20 | 34.80 |

| question (manual 4.2) | answer |
|---|---|
| task | regression of trip duration (minutes) for a known origin, destination and departure time |
| features available before the outcome | all five; destination B is a user input at request time (in training it is the trip's last fix) |
| split | temporal hold-out above; the random 80/20 split in evidence/ml is kept only for comparison |
| metrics | MAE (minutes, robust), RMSE (penalises long misses), signed error + % over/under (direction of the bias) |
| baseline | naive mean and historical-speed baseline above |
| model not useful if | its MAE is not clearly below the historical-speed baseline on the temporal test set |
| largest leakage / bias risk | label = (n_points − 1) × 15 s under-states trips with GPS gaps (10.22% of trips have a > 150 km/h jump) |
