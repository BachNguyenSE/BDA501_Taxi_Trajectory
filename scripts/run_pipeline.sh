#!/usr/bin/env bash
# Steps 2+4+5: ETL -> curated Parquet, ML training, load MongoDB.
# Usage: ./scripts/run_pipeline.sh [--sample]
set -euo pipefail
cd "$(dirname "$0")/.."

SAMPLE_FLAG="${1:-}"

docker compose up -d mongo
docker compose run --rm --no-deps spark python -m src.etl.porto_etl $SAMPLE_FLAG
docker compose run --rm --no-deps spark python -m src.ml.train_travel_time
docker compose run --rm spark python -m src.serving.load_mongo
echo "Curated data: data/curated/   Model: outputs/models/   Evidence: evidence/ml/"
