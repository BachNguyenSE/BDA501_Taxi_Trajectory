#!/usr/bin/env bash
# Download both datasets into data/raw/. Safe to re-run (skips files that already exist).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# --- 1. NYC TLC Yellow Taxi Jan–Jun 2025 (original candidate; feasibility study only) ---
NYC="$ROOT/data/raw/nyc_tlc"; mkdir -p "$NYC"
for m in 01 02 03 04 05 06; do
  f="$NYC/yellow_tripdata_2025-$m.parquet"
  [ -s "$f" ] || curl -fL -o "$f" "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-$m.parquet"
done
[ -s "$NYC/taxi_zone_lookup.csv" ] || curl -fL -o "$NYC/taxi_zone_lookup.csv" \
  "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# --- 2. Porto taxi trajectories, ECML/PKDD 2015 (selected dataset) ---
# Source: UCI ML Repository #339 (CC BY 4.0). Same train.csv as Kaggle "crailtap/taxi-trajectory".
PORTO="$ROOT/data/raw/porto"; mkdir -p "$PORTO"
if [ ! -s "$PORTO/train.csv" ]; then
  curl -fL -o "$PORTO/porto_uci.zip" \
    "https://archive.ics.uci.edu/static/public/339/taxi+service+trajectory+prediction+challenge+ecml+pkdd+2015.zip"
  (cd "$PORTO" && unzip -o -q porto_uci.zip && unzip -o -q train.csv.zip && rm -f train.csv.zip)
fi

echo "Row-count check (expected Porto: 1710671 lines incl. header):"
wc -l "$PORTO/train.csv"
ls -la "$NYC" "$PORTO"
