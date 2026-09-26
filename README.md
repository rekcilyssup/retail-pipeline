# Retail Order Intelligence Pipeline

A production-style hybrid data engineering pipeline implementing Lambda-style stream processing and reference data harmonization. Built with **Apache Spark (Structured Streaming)**, **Apache Kafka**, **PostgreSQL**, **Parquet**, **MongoDB**, **Elasticsearch**, **Kibana**, and **Apache Airflow**.

---

## Architecture Overview

```
 ┌────────────────────────┐
 │   PostgreSQL (OLTP)    │
 └───────────┬────────────┘
             │  Batch JDBC Extraction
             ▼
 ┌────────────────────────┐
 │   Parquet Data Lake    │ ◄───────────────────────────┐
 └───────────┬────────────┘                             │
             │ Reference Dimension                      │
             ▼ (Broadcast Join)                         │
 ┌────────────────────────┐      ┌────────────────────┐ │
 │ Spark Structured Stream├─────►│  MongoDB (NoSQL)   │ │
 └───────────▲────────────┘      │ (Enriched Orders)  │ │
             │ Micro-Batch       └────────────────────┘ │
 ┌───────────┴────────────┐      ┌────────────────────┐ │
 │   Kafka Event Broker   │      │ Dead-Letter Queue  │ │
 │ (CDC Order Event Bus)  │      │  (DLQ JSON Sink)   │ │
 └────────────────────────┘      └─────────▲──────────┘ │
                                           │            │
                                  Malformed Records     │
                                                        │
 ┌──────────────────────────────────────────────────────┴──┐
 │      Observability: Elasticsearch & Kibana (ELK)        │
 └─────────────────────────────────────────────────────────┘
```

The pipeline handles three data flows:
1. **Batch Extraction & Data Lake Staging**: Customer dimension tables are extracted from PostgreSQL and persisted as column-oriented Parquet files in the local lake storage.
2. **Real-time Event Streaming & Harmonization**: High-velocity order events (with Change Data Capture `op` semantics) stream through Kafka into Spark Structured Streaming.
3. **Validation & Dead-Letter Queue**: Incoming records are validated against schema constraints and a lateness window. Valid records are harmonized with the customer dimension via a broadcast join and upserted into MongoDB; rejected records are published to the `orders_stream_dlt` topic without stopping the stream.
4. **End-to-End Observability**: Structured logs from all components are indexed in real-time into Elasticsearch and visualized via Kibana, and every micro-batch emits counters that reconcile against the events it consumed.

---

## Core Engineering Features

- **Stream-Static Data Harmonization**: Joins high-velocity Kafka event streams with Parquet reference data using Spark broadcast joins, eliminating network shuffle overhead. The dimension is re-read on a TTL so attribute changes actually reach the stream.
- **CDC-Aware Ingestion with Real Semantics**: Events carry an `op` field (`I`/`U`/`D`) the way Debezium does, but updates and deletes reference an order that was *actually inserted* by the producer, and every event carries a monotonically increasing `lsn`.
- **Versioned, Idempotent Sinks**: Within a batch, events are collapsed to the newest `lsn` per `order_id`; against the sink, any event whose `lsn` is not strictly greater than the persisted `lsn` is discarded. Documents are keyed on `_id = order_id`, so a replayed micro-batch cannot double-count. Deletes are applied as real deletes and are idempotent by nature.
- **Lossless Dead-Letter Queue**: Every rejected record reaches the `orders_stream_dlt` topic, including payloads that fail JSON parsing entirely. `is_valid` is built with `when/otherwise` so a NULL predicate cannot escape and silently drop a row from every branch.
- **Micro-Batch Processing with Checkpointing**: `checkpointLocation` gives at-least-once recovery. A write failure is never swallowed - the query fails so the batch is replayed, which is the only safe behaviour once a checkpoint exists.
- **Backpressure Control**: `maxOffsetsPerTrigger` bounds how much is read per micro-batch, and consumer lag is logged per partition with a warning threshold.
- **Reconciling Observability**: Per-batch counters (invalid / late / superseded / stale / upserted / deleted) sum back to the events processed, with an explicit `reconciles=` flag. Structured logs ship to Elasticsearch for Kibana, and degrade to local rotating files if the cluster is unreachable.
- **Boundary Type Safety**: The batch job normalises pandas' nanosecond timestamps to microseconds before writing Parquet, because Spark 3.5 rejects unflagged `INT64 (TIMESTAMP(NANOS))`. The dimension is projected to the attributes the stream needs, since BSON cannot represent Spark's `TimestampNTZ`.
- **Polyglot Storage**: Columnar Parquet for batch analytics, document storage in MongoDB for low-latency operational reads.
- **Workflow Orchestration**: `scripts/run.sh` sequences the stages with retries and exit-code checks.

> **Not yet wired up:** `airflow_dag.py` is included as an Airflow 2.x reference DAG but is
> **not** started by `docker-compose.yml` and has not been executed end to end. Treat it as
> a design sketch, not a working component.

---

## Technology Stack

| Layer | Technologies |
| :--- | :--- |
| **Ingestion & Messaging** | Apache Kafka, Confluent Schema/Zookeeper |
| **Stream & Batch Processing** | Apache Spark 3.5 (PySpark), Spark SQL, Structured Streaming |
| **Storage & Lakes** | Parquet, PostgreSQL (OLTP), MongoDB 7.0 (NoSQL) |
| **Observability & Logging** | Elasticsearch 8.11, Kibana, Python RotatingFileHandler |
| **Orchestration** | Apache Airflow, Python Orchestration Runner |
| **Infrastructure** | Docker, Docker Compose |

---

## Repository Structure

```
├── airflow_dag.py             # Airflow 2.x reference DAG (not started by compose)
├── docker-compose.yml         # Multi-service stack (Postgres, Kafka, Mongo, ES, Kibana)
├── requirements.txt           # Python dependencies (PySpark, Kafka, PyArrow, SQLAlchemy, etc.)
├── .env.example               # Host port overrides, for running beside other projects
├── scripts/
│   └── run.sh                 # Single entrypoint: up / topics / batch / stream / produce / verify
├── data/
│   ├── init_postgres.sql      # Seed script for OLTP database
│   ├── lake/                  # Parquet data lake destination (customer dimension)
│   ├── checkpoints/           # Spark Structured Streaming offsets + WAL
│   └── dead_letter/           # Unused: dead-letter records go to the orders_stream_dlt topic
├── logs/                      # Rotating application logs
└── src/
    ├── batch_ingest.py        # JDBC batch extraction to Parquet lake, with Spark-safe dtypes
    ├── producer.py            # Kafka CDC event simulator (I/U/D, lsn, chaos injection)
    ├── spark_pipeline.py      # Spark Structured Streaming engine, CDC apply, LSN guard, DLT
    ├── run_pipeline.py        # Batch coordinator with retries and output validation
    └── utils/
        └── es_logger.py       # Distributed log forwarder for Elasticsearch
```

---

## Prerequisites

- **Docker & Docker Compose** (Docker Desktop or OrbStack)
- **Python 3.12 exactly.** The default `python3` on many machines is now 3.13/3.14, and the
  pinned `pandas==2.1.4` has no wheel for those and will not compile from source.
- **Java 11 or 17** (required by PySpark 3.5)

### Two environment traps worth knowing about

1. **Do not put the virtualenv inside a cloud-synced folder.** If your repo lives under
   `~/Documents` and iCloud Drive is enabled, files that are not yet materialised on disk
   get the macOS `dataless` flag. Importing a library then blocks *forever* on a file read
   with no error message. Keep the venv outside the synced tree:

   ```bash
   python3.12 -m venv ~/.venvs/retail-pipeline
   ~/.venvs/retail-pipeline/bin/pip install -r requirements.txt
   ```

2. **Pin `PYSPARK_PYTHON`.** Otherwise the Spark *worker* resolves the system `python3`
   and dies with `PYTHON_VERSION_MISMATCH` (worker 3.14 vs driver 3.12). `scripts/run.sh`
   sets this for you.

### Port conflicts

If you already run Postgres, MongoDB or Elasticsearch natively, or another Docker project,
the host process wins the port and your container looks healthy while never receiving a
connection. Copy `.env.example` to `.env` and move the ports. Every Python entrypoint reads
its port from the environment.

## Quick Start

Everything is wrapped by `scripts/run.sh`, which sets the interpreter and env consistently:

```bash
./scripts/run.sh up       # postgres, kafka, mongo, elasticsearch, kibana
./scripts/run.sh topics   # orders_stream + orders_stream_dlt, 3 partitions each
./scripts/run.sh batch    # extract customer dimension -> Parquet lake
./scripts/run.sh stream   # Spark Structured Streaming consumer (blocking, run in its own terminal)
./scripts/run.sh produce  # emit 200 CDC events into Kafka (own terminal)
./scripts/run.sh verify   # assert the sink has exactly one document per order_id
```

<details>
<summary>Equivalent manual commands</summary>

```bash
docker compose up -d
python3.12 -m venv ~/.venvs/retail-pipeline && ~/.venvs/retail-pipeline/bin/pip install -r requirements.txt
export PYSPARK_PYTHON=~/.venvs/retail-pipeline/bin/python PYSPARK_DRIVER_PYTHON=$PYSPARK_PYTHON
~/.venvs/retail-pipeline/bin/python src/run_pipeline.py
~/.venvs/retail-pipeline/bin/python src/spark_pipeline.py
~/.venvs/retail-pipeline/bin/python src/producer.py
```

</details>

Services (host ports, overridable via `.env`):
- **PostgreSQL**: `localhost:5432`
- **Kafka Broker**: `localhost:9092`
- **MongoDB**: `localhost:27017`
- **Elasticsearch**: `localhost:9200`
- **Kibana UI**: `http://localhost:5601`

## Verifying Results & Observability

### 1. Enriched orders in MongoDB

```bash
docker exec -it rp-mongo mongosh retail_analytics --eval "db.order_facts.find().limit(5).pretty()"
./scripts/run.sh verify     # documents vs distinct order_id -> must be equal
```

### 2. Dead-letter records

Rejected records are published to the **`orders_stream_dlt` Kafka topic**, not to a local
folder. A topic is partitioned, replayable and monitorable, and it does not depend on
node-local disk.

```bash
docker exec -it rp-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic orders_stream_dlt --from-beginning
```

Two reject reasons are tagged: `schema_or_parse_failure` and `late_event`. Payloads that
fail JSON parsing entirely are still present - the raw text is carried alongside, so triage
never requires a Kafka replay of the source topic.

### 3. Per-batch reconciliation

Every micro-batch logs a line whose counters sum back to the number of events processed:

```
[batch 0] processed=200 accepted=186 invalid=13 late=1 superseded=48 stale=131
          upserted=0 deleted=0 op_mix={'D': 7} unhandled_ops={} reconciles=True
```

If `reconciles=False`, records are unaccounted for and the pipeline is losing data.

---

## Enterprise / Cloud Deployment Architecture

In a production cloud deployment:
- **OLTP Database**: Amazon RDS / Azure SQL
- **Message Broker**: Amazon Managed Streaming for Apache Kafka (MSK) / Azure Event Hubs
- **Data Lake Storage**: Amazon S3 / Azure Data Lake Storage (ADLS Gen2)
- **Distributed Compute**: AWS EMR / Databricks / Spark on Kubernetes (EKS/AKS)
- **Orchestration**: Managed Workflows for Apache Airflow (MWAA) / Azure Data Factory
- **Operational Store**: Amazon DocumentDB / MongoDB Atlas
