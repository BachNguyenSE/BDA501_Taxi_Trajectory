# Dataset decision: NYC TLC → Porto taxi trajectories

Source of every number: `evidence/eda/nyc_tlc/profile.json` and `evidence/eda/porto/profile.json`
(re-generate with `./scripts/run_eda.sh`).

## Timeline
1. **Session 5 proposal** (`report/BDA501_Session5_Taxi_Trip_Proposal.pptx`) used NYC TLC Yellow Taxi
   2025-01..06 for zone-level trip-duration regression.
2. The project goal was then extended to the user-facing question *"from A to B at time T: how long,
   where is it congested, which way to go?"*.
3. The NYC data was profiled against these three requirements (step 0a) and failed two of them.
4. Porto GPS trajectories were profiled with the same questions (step 0b) and satisfy all three.

## Side-by-side
| | NYC TLC Yellow 2025-01..06 | Porto ECML/PKDD 2015 |
|---|---|---|
| Records | 24,083,384 trips | 1,710,670 trips |
| Location facts per trip | 2 zone IDs (PU, DO) | median 42 GPS fixes (p95: 103) |
| Total location facts | 48.2 M zone IDs | **83,409,386 GPS points** |
| Spatial unit | 265 predefined taxi zones | lat/lon every 15 s → any grid (2,152 cells at 0.005°) |
| Path between start and end | unknown | 81,704,617 timed 15 s segments |
| Duration label | dropoff − pickup | (points − 1) × 15 s |
| ETA prediction | ✅ | ✅ |
| Congested areas along the route | ❌ | ✅ |
| Route suggestion (area graph) | ❌ | ✅ |

The NYC data does show congestion in aggregate — on its busiest OD pair the median speed falls from
13.6 mph (05:00) to 7.1 mph (10:00) — but it cannot say *where* between the two zones the slowdown occurs.

## Consequences for the pipeline (data-quality findings in Porto)
| Finding | Count | Handling in ETL |
|---|---|---|
| `DAY_TYPE` is constant `"A"` | 1,710,670 / 1,710,670 | drop; derive weekday + PT holiday calendar (join) |
| Empty / single-point polyline | 36,510 trips (2.13%) | drop (no duration label) |
| `MISSING_DATA = True` | 10 trips | drop |
| Duplicate `TRIP_ID` | 81 rows | deduplicate on `TRIP_ID` |
| Duration > 180 min | 847 trips (0.05%) | drop as outliers |
| GPS jump (> 150 km/h in 15 s) | 223,060 segments (0.27%) in 174,912 trips (10.2%) | drop the **segment**, not the trip, for zone speed |
| Points outside the Porto bbox | 12,433 trips (0.73%) | keep trip; ignore out-of-bbox points for zones |
| `[lon, lat]` order | first point never looks swapped (0 trips) | parse as `p[0]=lon, p[1]=lat`; keep the check as a test |
| Taxi-stand metadata | not in the UCI release | enrichment uses holiday calendar + derived zone table |

## Engineering lessons recorded during profiling
- Caching the parsed nested `POLYLINE` arrays (~83 M points) caused `java.lang.OutOfMemoryError`
  with a 5 GB driver heap; persisting only scalar per-trip metrics (`MEMORY_AND_DISK`) fixed it.
- Per-trip metrics are computed with higher-order array functions (`transform`, `filter`, `aggregate`),
  which run inside each partition with no shuffle; only the spatial grid step explodes points.
- Spark 4 ANSI mode raises on `poly[0]` for empty arrays; `get(poly, 0)` returns NULL instead.
