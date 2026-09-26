#!/usr/bin/env bash
# Reproducible local run for the retail pipeline.
#
# Two things this script exists to enforce, both of which bit us:
#   1. the venv must NOT live under ~/Documents (iCloud marks files `dataless`
#      and importing pandas then hangs forever on a file read)
#   2. PYSPARK_PYTHON must be pinned, or the Spark worker resolves the system
#      python (3.14) and dies with PYTHON_VERSION_MISMATCH against the 3.12 driver
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="${VENV:-$HOME/.venvs/retail-pipeline}"
PY="$VENV/bin/python"

cd "$REPO_DIR"

if [ ! -x "$PY" ]; then
  echo "venv not found at $VENV"
  echo "  python3.12 -m venv $VENV && $VENV/bin/pip install -r requirements.txt"
  exit 1
fi

set -a; [ -f .env ] && . ./.env; set +a

export PYSPARK_PYTHON="$PY"
export PYSPARK_DRIVER_PYTHON="$PY"
export ES_HOST="http://localhost:${ES_PORT:-9200}"
export KAFKA_BOOTSTRAP="localhost:${KAFKA_PORT:-9092}"
export PG_PORT="${POSTGRES_PORT:-5432}"
export MONGO_PORT="${MONGO_PORT:-27017}"
export MONGO_DB="${MONGO_DB:-retail_analytics}"
export MONGO_COLLECTION="${MONGO_COLLECTION:-order_facts}"

case "${1:-help}" in
  up)
    docker compose up -d
    docker compose ps
    ;;
  topics)
    for t in orders_stream orders_stream_dlt; do
      docker exec rp-kafka kafka-topics --bootstrap-server localhost:9092 \
        --create --topic "$t" --partitions 3 --replication-factor 1 2>&1 | tail -1
    done
    ;;
  batch)
    "$PY" -u src/run_pipeline.py
    ;;
  stream)
    exec "$PY" -u src/spark_pipeline.py
    ;;
  produce)
    "$PY" -u -c "import sys; sys.path.insert(0,'src'); import producer; producer.run()"
    ;;
  verify)
    "$PY" - <<'EOF'
import os
from pymongo import MongoClient

client = MongoClient(f"mongodb://localhost:{os.environ['MONGO_PORT']}")
db = client[os.environ["MONGO_DB"]]
coll = db[os.environ["MONGO_COLLECTION"]]

n = coll.count_documents({})
ids = coll.distinct("order_id")
ops = [(o, coll.count_documents({"op": o})) for o in coll.distinct("op")]

print(f"documents        = {n}")
print(f"distinct order_id= {len(ids)}")
print(f"duplicates       = {n - len(ids)}")
print(f"ops              = {ops}")
print(f"enrichment_nulls = {coll.count_documents({'name': None})}")

assert n == len(ids), "FAIL: duplicate order rows in the sink"
print("OK: sink is idempotent (one document per order_id)")
EOF
    ;;
  *)
    echo "usage: $0 {up|topics|batch|stream|produce|verify}"
    echo
    echo "  up       start postgres/kafka/mongo/elasticsearch/kibana"
    echo "  topics   create orders_stream + orders_stream_dlt (3 partitions each)"
    echo "  batch    extract customer dimension -> Parquet lake"
    echo "  stream   run the Spark Structured Streaming consumer (blocking)"
    echo "  produce  emit 200 CDC events into Kafka"
    echo "  verify   assert the Mongo sink has no duplicate order rows"
    ;;
esac
