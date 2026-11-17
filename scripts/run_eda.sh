#!/usr/bin/env bash
# Step 0: dataset feasibility study (NYC TLC, rejected) and profiling (Porto, selected).
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose run --rm --no-deps spark python -m src.eda.nyc_tlc_profile
docker compose run --rm --no-deps spark python -m src.eda.porto_profile
echo "Evidence written to evidence/eda/{nyc_tlc,porto}/"
