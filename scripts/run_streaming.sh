#!/usr/bin/env bash
# Step 6: Kafka replay -> Spark Structured Streaming -> MongoDB zone_speed_live (speed layer).
# The producer and the consumer run as SEPARATE containers at the same time: the consumer starts
# first and processes fixes while the producer replays them (1 s wall = 1 min event time).
# Usage: ./scripts/run_streaming.sh [--sample]
set -euo pipefail
cd "$(dirname "$0")/.."

PRODUCER_ARGS=""
if [ "${1:-}" = "--sample" ]; then PRODUCER_ARGS="--sample --start-local 2013-10-01T00:00 --hours 720"; fi
KT="docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"

docker compose --profile streaming up -d mongo kafka
echo "[streaming] waiting for Kafka..."
for i in $(seq 1 60); do
  $KT --list >/dev/null 2>&1 && break
  sleep 2
done

# The topic is infrastructure: (re)create it before either side starts.
$KT --delete --topic porto.gps.pings >/dev/null 2>&1 || true
for i in $(seq 1 15); do
  $KT --create --topic porto.gps.pings --partitions 3 --replication-factor 1 >/dev/null 2>&1 && break
  sleep 2   # topic deletion is asynchronous
done

docker rm -f bda501-stream-consumer >/dev/null 2>&1 || true
docker compose run -d --no-deps --name bda501-stream-consumer spark \
  python -m src.streaming.live_zone_speed --reset --trigger "5 seconds" --idle-stop-seconds 45 >/dev/null
echo "[streaming] consumer started; replaying with the producer..."
sleep 20
docker compose run --rm --no-deps spark python -m src.streaming.producer $PRODUCER_ARGS

echo "[streaming] producer done; waiting for the consumer to drain the topic and stop..."
docker wait bda501-stream-consumer >/dev/null 2>&1 || true
docker logs bda501-stream-consumer > evidence/streaming/consumer.log 2>&1 || true
docker rm bda501-stream-consumer >/dev/null 2>&1 || true
$KT --describe --topic porto.gps.pings | tee evidence/streaming/topic_describe.txt
cat evidence/streaming/summary.md
