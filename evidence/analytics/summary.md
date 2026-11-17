# Spark SQL analytics (curated Parquet, full dataset)

## Q1_peak_bottlenecks
```sql
SELECT z.zone_id, round(z.centroid_lat, 4) AS lat, round(z.centroid_lon, 4) AS lon,
       round(sum(s.total_km) * 3600 / sum(s.total_s), 2) AS peak_speed_kmh,
       sum(s.n_segments) AS n_segments
FROM zone_hour_speed s JOIN zones z ON s.zone_id = z.zone_id
WHERE s.is_weekend = 0 AND s.hour BETWEEN 17 AND 19
GROUP BY z.zone_id, z.centroid_lat, z.centroid_lon
HAVING sum(s.n_segments) >= 2000
ORDER BY peak_speed_kmh ASC
LIMIT 10
```
| zone_id | lat | lon | peak_speed_kmh | n_segments |
|---|---|---|---|---|
| -1735_8248 | 41.24 | -8.67 | 1.50 | 3,609 |
| -1709_8245 | 41.23 | -8.54 | 3.62 | 3,887 |
| -1734_8248 | 41.24 | -8.67 | 4.86 | 2,070 |
| -1720_8227 | 41.14 | -8.60 | 5.81 | 2,674 |
| -1707_8227 | 41.14 | -8.53 | 7.68 | 3,222 |
| -1723_8229 | 41.15 | -8.61 | 9.60 | 382,432 |
| -1711_8243 | 41.22 | -8.55 | 9.72 | 2,375 |
| -1735_8247 | 41.24 | -8.67 | 9.75 | 23,761 |
| -1738_8236 | 41.18 | -8.69 | 10.00 | 5,984 |
| -1722_8229 | 41.15 | -8.61 | 10.40 | 262,763 |

## Q2_hourly_speed_profile
```sql
SELECT hour, CASE is_weekend WHEN 1 THEN 'weekend' ELSE 'weekday' END AS day_type,
       round(sum(total_km) * 3600 / sum(total_s), 2) AS city_speed_kmh,
       sum(n_segments) AS n_segments
FROM zone_hour_speed
GROUP BY hour, is_weekend
ORDER BY is_weekend, hour
```
| hour | day_type | city_speed_kmh | n_segments |
|---|---|---|---|
| 0 | weekday | 30.07 | 1,055,406 |
| 1 | weekday | 31.14 | 946,329 |
| 2 | weekday | 31.02 | 847,791 |
| 3 | weekday | 32.89 | 729,256 |
| 4 | weekday | 39.33 | 1,091,356 |
| 5 | weekday | 40.88 | 1,103,148 |
| 6 | weekday | 37.63 | 1,016,587 |
| 7 | weekday | 33.82 | 1,447,418 |
| 8 | weekday | 24.21 | 3,105,233 |
| 9 | weekday | 24.18 | 4,113,513 |
| 10 | weekday | 24.93 | 3,879,400 |
| 11 | weekday | 24.71 | 3,744,202 |
| 12 | weekday | 24.77 | 3,221,276 |
| 13 | weekday | 25.76 | 3,116,714 |
| 14 | weekday | 23.53 | 3,617,792 |
| 15 | weekday | 22.71 | 3,878,195 |
| 16 | weekday | 22.39 | 3,859,119 |
| 17 | weekday | 21.54 | 4,094,086 |
| 18 | weekday | 19.88 | 3,641,650 |
| 19 | weekday | 22.33 | 3,056,026 |
| 20 | weekday | 26.32 | 2,311,709 |
| 21 | weekday | 28.17 | 1,778,462 |
| 22 | weekday | 27.95 | 1,540,174 |
| 23 | weekday | 28.12 | 1,457,521 |
| 0 | weekend | 26.96 | 907,602 |
| 1 | weekend | 29.00 | 1,018,496 |
| 2 | weekend | 29.91 | 1,045,864 |
| 3 | weekend | 31.56 | 1,091,226 |
| 4 | weekend | 34.36 | 1,392,125 |
| 5 | weekend | 35.61 | 1,253,388 |
| 6 | weekend | 35.25 | 1,157,496 |
| 7 | weekend | 35.27 | 805,446 |
| 8 | weekend | 34.49 | 609,288 |
| 9 | weekend | 31.56 | 651,242 |
| 10 | weekend | 28.93 | 746,147 |
| 11 | weekend | 27.03 | 783,553 |
| 12 | weekend | 27.26 | 832,666 |
| 13 | weekend | 29.67 | 792,590 |
| 14 | weekend | 29.37 | 734,951 |
| 15 | weekend | 26.26 | 786,496 |
| 16 | weekend | 25.74 | 799,945 |
| 17 | weekend | 25.96 | 818,476 |
| 18 | weekend | 25.88 | 806,605 |
| 19 | weekend | 26.48 | 842,624 |
| 20 | weekend | 27.62 | 821,125 |
| 21 | weekend | 27.64 | 823,058 |
| 22 | weekend | 27.36 | 690,558 |
| 23 | weekend | 27.22 | 639,583 |

## Q3_origin_zone_trips
```sql
WITH t AS (
  SELECT concat_ws('_', CAST(floor(start_lon / 0.005) AS INT), CAST(floor(start_lat / 0.005) AS INT)) AS origin_zone,
         hour, is_weekend, duration_min, straight_km
  FROM trip_features
)
SELECT t.origin_zone, count(*) AS trips,
       round(avg(t.duration_min), 2) AS avg_duration_min,
       round(avg(t.straight_km), 2) AS avg_straight_km,
       round(avg(s.avg_speed_kmh), 2) AS origin_hist_speed_kmh
FROM t JOIN zone_hour_speed s
  ON t.origin_zone = s.zone_id AND t.hour = s.hour AND t.is_weekend = s.is_weekend
GROUP BY t.origin_zone
HAVING count(*) >= 1000
ORDER BY trips DESC
LIMIT 15
```
| origin_zone | trips | avg_duration_min | avg_straight_km | origin_hist_speed_kmh |
|---|---|---|---|---|
| -1723_8229 | 179,778 | 10.78 | 3.19 | 13.37 |
| -1718_8229 | 94,680 | 13.13 | 4.26 | 15.26 |
| -1724_8229 | 74,945 | 12.27 | 3.11 | 15.59 |
| -1723_8228 | 56,418 | 11.13 | 2.77 | 17.14 |
| -1722_8230 | 52,939 | 11.44 | 2.98 | 18.23 |
| -1722_8229 | 52,654 | 11.85 | 3.39 | 13.66 |
| -1726_8231 | 47,560 | 11.36 | 2.83 | 15.40 |
| -1722_8228 | 45,500 | 13.06 | 3.63 | 17.69 |
| -1721_8236 | 45,155 | 12.56 | 3.95 | 12.79 |
| -1730_8230 | 37,264 | 11.84 | 3.40 | 19.00 |
| -1721_8232 | 31,203 | 10.44 | 2.71 | 16.70 |
| -1724_8228 | 30,227 | 11.44 | 2.88 | 16.30 |
| -1727_8230 | 28,508 | 11.16 | 3.04 | 24.08 |
| -1728_8231 | 25,316 | 11.67 | 3.20 | 20.17 |
| -1723_8230 | 24,950 | 10.83 | 2.88 | 19.00 |