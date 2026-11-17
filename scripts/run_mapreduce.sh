#!/usr/bin/env bash
# Step 1: MapReduce zone x hour speed on Hadoop 3.5.0 (HDFS + YARN, Hadoop Streaming),
# then an independent Spark SQL re-computation with the same semantics as a cross-check.
# Usage: ./scripts/run_mapreduce.sh [--full]      (default input: data/sample/porto_sample.csv)
set -euo pipefail
cd "$(dirname "$0")/.."

NAME=sample
INPUT_LOCAL=data/sample/porto_sample.csv
if [ "${1:-}" = "--full" ]; then NAME=full; INPUT_LOCAL=data/raw/porto/train.csv; fi
EVID=evidence/mapreduce
OUT_LOCAL=data/curated/mapreduce/zone_hour_speed_${NAME}.tsv
mkdir -p "$EVID" data/curated/mapreduce

HADOOP_SERVICES="namenode datanode resourcemanager nodemanager historyserver hadoop-client"
docker compose --profile hadoop up -d $HADOOP_SERVICES
HC="docker compose --profile hadoop exec -T hadoop-client"

echo "[mapreduce] waiting for HDFS (leaving safe mode)..."
for i in $(seq 1 90); do $HC hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF" && break; sleep 2; done
# Poll the ResourceManager REST API from the host: the `yarn` CLI retries an unreachable RM
# for up to 15 minutes by default and would hang this loop.
echo "[mapreduce] waiting for a RUNNING YARN NodeManager..."
for i in $(seq 1 90); do
  curl -sf -m 3 http://localhost:8088/ws/v1/cluster/nodes 2>/dev/null | grep -q '"state":"RUNNING"' && break
  sleep 2
done

$HC hdfs dfs -mkdir -p /tmp /mr-history/tmp /mr-history/done /porto/input /porto/output
$HC hdfs dfs -chmod 1777 /tmp /mr-history/tmp /mr-history/done
echo "[mapreduce] HDFS put $INPUT_LOCAL -> /porto/input/$NAME.csv"
$HC hdfs dfs -put -f "/workspace/$INPUT_LOCAL" "/porto/input/$NAME.csv"
$HC hdfs dfs -rm -r -f "/porto/output/zone_hour_speed_$NAME" >/dev/null

START=$(date +%s)
$HC hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar \
  -D mapreduce.job.name="porto-zone-hour-speed-$NAME" \
  -D mapreduce.job.reduces=2 \
  -files /workspace/src/mapreduce/core.py,/workspace/src/mapreduce/mapper.py,/workspace/src/mapreduce/reducer.py \
  -mapper "python3 mapper.py" \
  -combiner "python3 reducer.py --combiner" \
  -reducer "python3 reducer.py" \
  -input "/porto/input/$NAME.csv" \
  -output "/porto/output/zone_hour_speed_$NAME" 2>&1 | tee "$EVID/job_$NAME.log"
END=$(date +%s)
echo "{\"input\": \"$INPUT_LOCAL\", \"wall_clock_s\": $((END - START))}" > "$EVID/job_${NAME}_timing.json"

$HC hdfs dfs -ls "/porto/output/zone_hour_speed_$NAME" | tee "$EVID/hdfs_output_$NAME.txt"
rm -f "$OUT_LOCAL"
$HC hdfs dfs -getmerge "/porto/output/zone_hour_speed_$NAME" "/workspace/$OUT_LOCAL"
echo "[mapreduce] $(wc -l < "$OUT_LOCAL") output rows -> $OUT_LOCAL"

# Free the Docker VM memory for Spark before the cross-check (HDFS data stays in its volume).
docker compose --profile hadoop stop $HADOOP_SERVICES

docker compose run --rm --no-deps spark python -m src.mapreduce.crosscheck \
  --input "$INPUT_LOCAL" --mr-output "$OUT_LOCAL" --job-log "$EVID/job_$NAME.log" --name "$NAME"
