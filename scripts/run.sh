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
  test)
    "$PY" tests/test_validation.py
    ;;
  airflow)
    shift || true
    docker compose --profile airflow up -d
    sleep 20
    docker exec rp-airflow-scheduler airflow dags unpause retail_customer_dimension
    docker exec rp-airflow-scheduler airflow dags list-import-errors
    docker exec rp-airflow-scheduler airflow dags trigger retail_customer_dimension "$@"
    echo
    echo "DAG state:"
    docker exec rp-airflow-scheduler airflow dags list-runs -d retail_customer_dimension | head -6
    echo
    echo "Task states:"
    RUN=$(docker exec rp-airflow-scheduler airflow dags list-runs -d retail_customer_dimension \
          --output json 2>/dev/null | "$PY" -c "
import sys, json
runs = json.load(sys.stdin)
runs.sort(key=lambda r: r['start_date'], reverse=True)
print(runs[0]['run_id'])")
    docker exec rp-airflow-scheduler airflow tasks states-for-dag-run retail_customer_dimension "$RUN"
    ;;
  test-airflow)
    "$PY" tests/test_validation.py
    echo
    echo "DAG import errors:"
    docker exec rp-airflow-scheduler airflow dags list-import-errors
    echo "Registered DAGs:"
    docker exec rp-airflow-scheduler airflow dags list 2>/dev/null | grep -E "dag_id|retail_" || true
    ;;
  *)
    echo "usage: $0 {up|topics|batch|stream|produce|verify|test|airflow|test-airflow}"
    echo
    echo "  up           start postgres/kafka/mongo/elasticsearch/kibana"
    echo "  topics       create orders_stream + orders_stream_dlt (3 partitions each)"
    echo "  batch        extract, validate and publish the customer dimension"
    echo "  stream       run the Spark Structured Streaming consumer (blocking)"
    echo "  produce      emit 200 CDC events into Kafka"
    echo "  verify       assert the Mongo sink has no duplicate order rows"
    echo "  test         run the data quality gate unit tests"
    echo "  airflow      start Airflow and trigger the batch DAG (opt-in profile)"
    echo "  test-airflow check the DAG parses with no import errors"
    ;;
esac
