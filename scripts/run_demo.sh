#!/usr/bin/env bash
# Brings up MongoDB + the FastAPI serving layer and opens the demo UI.
# Run scripts/run_pipeline.sh first (at least once) so the model and MongoDB data exist.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose up -d mongo api
echo "Waiting for the API to become healthy..."
for _ in $(seq 1 30); do
  if curl -sf http://localhost:8000/health >/dev/null 2>&1; then
    echo "API is up: http://localhost:8000/health"
    break
  fi
  sleep 1
done
echo "Demo UI: http://localhost:8000/"
if command -v open >/dev/null 2>&1; then open "http://localhost:8000/"; fi
