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

The pipeline handles two distinct data flows:
1. **Batch Extraction & Data Lake Staging**: Customer dimension tables are extracted from PostgreSQL and persisted as column-oriented Parquet files in the local lake storage.
2. **Real-time Event Streaming & Harmonization**: High-velocity order events (with Change Data Capture `op` semantics) stream through Kafka into Spark Structured Streaming.
3. **Validation & Dead-Letter Queue (DLQ)**: Incoming records are validated against schema constraints. Valid records are harmonized with the customer dimension via a broadcast join and persisted into MongoDB; malformed events are routed to a dead-letter sink without crashing the stream.
4. **End-to-End Observability**: Structured logs from all components are indexed in real-time into Elasticsearch and visualized via Kibana.

---

## Core Engineering Features

- **Stream-Static Data Harmonization**: Joins high-velocity Kafka event streams with Parquet reference data using Spark broadcast joins, eliminating network shuffle overhead.
- **CDC-Aware Ingestion**: Event payloads simulate Change Data Capture (CDC) operations (`Insert`, `Update`, `Delete`), allowing downstream consumers to handle upserts and state transitions.
- **Fault-Tolerant Exception Handling**: Implements a Dead-Letter Queue (DLQ) pattern where malformed payloads (invalid types, missing keys) are routed to isolated cold storage for triage.
- **Micro-Batch Processing with Checkpointing**: Leverages Spark Structured Streaming's writeStream checkpointing to guarantee at-least-once processing semantics and failure recovery.
- **Polyglot Storage & NoSQL Modeling**: Pairs columnar lake storage (Parquet) for batch analytics with document storage (MongoDB) for low-latency operational access.
- **Centralized Log Telemetry**: Custom logging integration with Elasticsearch and local rotating logs with graceful degradation if the monitoring cluster is unavailable.
- **Workflow Orchestration**: Orchestrated via automated step validation scripts and an Apache Airflow DAG with automated retries and failure alerts.

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
├── airflow_dag.py             # Airflow DAG definition for scheduled batch ingestion
├── docker-compose.yml         # Multi-service stack (Postgres, Kafka, Mongo, ES, Kibana)
├── requirements.txt           # Python dependencies (PySpark, Kafka, PyArrow, etc.)
├── data/
│   ├── init_postgres.sql      # Seed script for OLTP database
│   ├── lake/                  # Parquet data lake destination
│   └── dead_letter/           # Dead-letter quarantine storage
├── logs/                      # Rotating application logs
└── src/
    ├── batch_ingest.py        # JDBC batch extraction to Parquet lake
    ├── producer.py            # Kafka event generator with CDC payload schemas
    ├── spark_pipeline.py      # Spark Structured Streaming engine & broadcast join
    ├── run_pipeline.py        # Pipeline coordinator and output validator
    └── utils/
        └── es_logger.py       # Distributed log forwarder for Elasticsearch
```

---

## Prerequisites

- **Docker & Docker Compose** (Docker Desktop or OrbStack)
- **Python 3.10 – 3.12** (Python 3.12 recommended)
- **Java 11 or 17** (required by PySpark)

---

## Quick Start

### 1. Environment Setup

```bash
# Create and activate virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Launch Infrastructure Services

Start the database, message broker, NoSQL store, and monitoring stack in the background:

```bash
docker compose up -d

# Verify all containers are up and healthy
docker compose ps
```

Services initialized:
- **PostgreSQL**: `localhost:5432`
- **Kafka Broker**: `localhost:9092`
- **MongoDB**: `localhost:27017`
- **Elasticsearch**: `localhost:9200`
- **Kibana UI**: `http://localhost:5601`

---

## Running the Pipeline

### Stage 1: Batch Extraction & Lake Staging

Run the batch orchestrator to pull the customer dimension from PostgreSQL into the Parquet lake:

```bash
python src/run_pipeline.py
```

### Stage 2: Start the Spark Streaming Consumer

Launch the Spark Structured Streaming engine (runs continuously to process micro-batches):

```bash
python src/spark_pipeline.py
```

### Stage 3: Produce Real-time Order Events

In a separate terminal, trigger the event producer to stream order events to Kafka:

```bash
python src/producer.py
```

---

## Verifying Results & Observability

### 1. Enriched Orders in MongoDB
Query the MongoDB operational document store:
```bash
docker exec -it rp-mongo mongosh retail_analytics --eval "db.order_facts.find().limit(5).pretty()"
```

### 2. Dead-Letter Quarantine
Inspect records filtered out by the schema validation layer:
```bash
cat data/dead_letter/*.json | head -n 5
```

### 3. Kibana Live Telemetry
1. Open `http://localhost:5601` in your browser.
2. Navigate to **Stack Management** > **Data Views**.
3. Create a Data View matching index pattern `pipeline-logs-*` (Timestamp field: `timestamp`).
4. Navigate to **Discover** to inspect real-time log streams, batch execution metrics, and error rates.

---

## Enterprise / Cloud Deployment Architecture

In a production cloud deployment:
- **OLTP Database**: Amazon RDS / Azure SQL
- **Message Broker**: Amazon Managed Streaming for Apache Kafka (MSK) / Azure Event Hubs
- **Data Lake Storage**: Amazon S3 / Azure Data Lake Storage (ADLS Gen2)
- **Distributed Compute**: AWS EMR / Databricks / Spark on Kubernetes (EKS/AKS)
- **Orchestration**: Managed Workflows for Apache Airflow (MWAA) / Azure Data Factory
- **Operational Store**: Amazon DocumentDB / MongoDB Atlas
