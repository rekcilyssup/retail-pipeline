# Retail Order Intelligence Pipeline — Deep Dive

> **Read this before you revise.** Every number and every claim below comes from an
> executed run of this repository, not from intent. Where the pipeline has a real
> limitation, it is named rather than hidden.
>
> Verified with: Python 3.12, PySpark 3.5.1, Kafka 7.5 (3 partitions), MongoDB 7,
> Elasticsearch 8.11, Airflow 2.9.3.

---

## Table of Contents
1. [The problem this pipeline solves](#1-the-problem-this-pipeline-solves)
2. [Architecture: the seven layers](#2-architecture-the-seven-layers)
3. [Core concepts, de-jargonised](#3-core-concepts-de-jargonised)
4. [The three failure modes that shaped the design](#4-the-three-failure-modes-that-shaped-the-design)
5. [Verified run: what actually happened](#5-verified-run-what-actually-happened)
6. [File-by-file walkthrough](#6-file-by-file-walkthrough)
7. [The 12 questions you will be asked](#7-the-12-questions-you-will-be-asked)
8. [Known limitations — say these before they find them](#8-known-limitations--say-these-before-they-find-them)
9. [Command reference](#9-command-reference)

---

# 1. The problem this pipeline solves

A checkout service writes a row per order into PostgreSQL. The analytics team then wants
to answer *"revenue per customer segment per city for the last 15 minutes"*.

They cannot run that query on the operational database. A `JOIN` plus `GROUP BY` over
100k orders/minute locks tables, the checkout button starts spinning, and the storefront
falls over. **The analytics workload kills the transactional workload.**

The fix is to separate them:

```
OLTP (Postgres)  ──extract──►  storage  ──►  transform  ──►  query engine
   writes                        (Parquet)     (Spark)         (Mongo)
   never queried
     for analytics
```

Everything in this repository follows from that one sentence.

---

# 2. Architecture: the seven layers

Every layer below maps to real files. Learn this diagram; it is the single most
reusable thing in the project.

```
 1. SOURCE           PostgreSQL `customers` table            data/init_postgres.sql
                          │
                          │ 2. INGESTION (scheduled JDBC pull)
                          ▼
                    candidate Parquet snapshot       data/lake/_candidate/
                          │
                          │ 3. QUALITY GATE (13 checks, fail-fast)   src/validation.py
                          ▼
                    published Parquet snapshot      data/lake/customers.parquet
                          │                                   ▲
                          │                             (atomic rename)
                          │
    ┌─────────────────────┴──────────────────────────────────┴─────────────┐
    │ 4. BUFFER       Kafka topic `orders_stream`, 3 partitions, keyed by   │
    │                 order_id so all events for one order keep their order  │
    │ 5. PROCESSING   Spark Structured Streaming, micro-batch:              │
    │                   parse → validate → collapse by lsn → discard stale   │
    │                   → apply CDC → broadcast join to dimension           │
    └───────────────────────────────┬───────────────────────────────────────┘
                                    │
                    ┌───────────────┴────────────────┐
                    ▼                                ▼
    6. SERVING  MongoDB `order_facts`      DEAD LETTER  Kafka `orders_stream_dlt`
                (one doc per order_id)     (rejected records, by reason)

 7. ORCHESTRATION   Airflow DAG: extract → validate → publish   airflow_dag.py
    + MONITORING    per-batch reconciling counters → Elasticsearch → Kibana
```

**The one-line version:** layers 2–4 get the data to land reliably, layers 5–6 make it
correct, layer 7 is what makes the system operable and reviewable.

### Medallion, in this project

| Zone | What it is | Here |
|---|---|---|
| **Bronze** (raw) | Landed as-is, no assumptions, replayable | Kafka topic, and the `_candidate` Parquet snapshot |
| **Silver** (validated) | Typed, deduped, conformed, one row per entity | `data/lake/customers.parquet` after the 13-check gate |
| **Gold** (serving) | Consumption-shaped, keyed for reads | MongoDB `order_facts` |

The reason to keep Bronze: when the transform is wrong, you replay forward from raw. You
never go back to the source system.

---

# 3. Core concepts, de-jargonised

### OLTP vs OLAP
OLTP optimises many small transactions: fast single-row inserts, row locks, normalised
schema. OLAP optimises few large scans: columnar layout so a `SUM` over one column reads
one column's worth of bytes. Postgres is the former; Parquet and MongoDB are the latter.

### Batch vs streaming vs CDC
- **Batch** — process a window of data at once. Cheap, simple, and can recompute history.
- **Streaming** — process each record as it arrives. Low latency, no full recompute.
- **CDC** — capture the *changes* to a database by reading its write-ahead log, instead
  of polling. Polling `WHERE updated_at > now() - 5 min` hammers the source and misses
  intermediate states; CDC sees every insert, update and delete exactly once, in order.

This project runs **batch** for the dimension (slow-changing reference data) and
**streaming with CDC semantics** for the order events (fast-changing facts). That split
is the normal production answer, not a compromise.

### Kafka
A partitioned, replicated, append-only log. Producers append to a topic; consumers track
their own offset per partition and read independently. Three properties matter here:

1. **Retention is the buffer.** A slow consumer does not lose data; the log holds it.
2. **The partition key determines ordering.** Only per-partition order is guaranteed,
   never global. This project keys every event by `order_id` so all events for one order
   land in one partition and stay in relative order.
3. **Offsets make replay free.** Rewinding a consumer group to offset 0 replays
   everything. This project relies on that to prove idempotency.

### Spark Structured Streaming
An unbounded stream treated as a continuously-appended table. Spark takes whatever
arrived in the last trigger interval and runs your transformations as a **micro-batch**.
The cost of micro-batching versus true record-at-a-time is latency; the benefit is that
the same DataFrame API, the same optimizer and the same sink connectors work for batch
and streaming.

### Shuffle vs broadcast join
Joining two distributed tables normally requires a **shuffle**: Spark hashes the join key
and redistributes rows across the network so matching keys land on the same executor.
That network movement is the single largest cost in a Spark pipeline.

A **broadcast join** copies one small side to every executor's memory once, so the join
happens locally with no shuffle. Here the customer dimension is 10 rows / 4 KB, so
broadcasting is obviously right. Spark normally decides this automatically, but it is
worth being able to say when the *small* side stops being small: at roughly 10–100 MB
depending on executor memory and cluster size, broadcast stops being free and a shuffle
join wins again. **Interview answer: always give the size threshold, not just the
mechanism.**

### Parquet
Columnar, compressed, with per-column statistics in the footer. Three payoffs:
column pruning (read only the columns you query), predicate pushdown (skip row groups
whose min/max cannot match), and far fewer disk reads than row formats like CSV or JSON.
Snappy or Zstd compression typically reaches 5–10x on this kind of data.

### Dead-letter queue
A quarantine for records the pipeline refuses to process. It exists so that **one bad
record cannot stop the stream**. A DLQ is only useful if it is *complete* — which is
subtly hard, and was the biggest bug in this project's history (see §4.1).

### MongoDB
A document store. The enriched order is one self-contained BSON document, so a support
dashboard can read an order by `_id` in a single lookup, with no joins. JSON-like
flexibility also means the schema can evolve without migrations. The cost: you give up
joins and cross-document transactions.

### Elasticsearch + Kibana
A distributed search and analytics engine plus its dashboard. Here it indexes structured
logs so you can query "show me every micro-batch where the dead-letter count spiked"
instead of SSH-ing into containers and grepping. The handler is deliberately **fail-soft**:
if the cluster is unreachable, logging falls back to local rotating files, because a
pipeline must never die because its monitoring died.

### Airflow
Workflows as a **DAG** — a directed acyclic graph of tasks with dependencies. The reason
it beats cron: cron fires on a clock whether or not the previous job finished. Here
`validate` cannot start until `extract` succeeds, and `publish` cannot start until
`validate` passes. A slow upstream stage delays the run instead of silently reading
incomplete data.

---

# 4. The three failure modes that shaped the design

This is the part worth understanding. Each of these was a real bug found by running the
pipeline, not a hypothetical.

### 4.1 The silent data loss (SQL three-valued logic)

`from_json` returns an **all-NULL struct** when a payload cannot be parsed. The old
validation built its predicate with bare boolean operators:

```python
col("customer_id").rlike("^[0-9]+$") & col("amount").rlike(...)
```

In SQL, `NULL AND NULL` is `NULL`, not `FALSE`. And `filter()` keeps a row only when the
predicate is `TRUE`. So a NULL predicate was excluded from the good branch **and** from
the dead-letter branch. The record reached neither. Measured on 11 records:

```
total=11  good=5  dead_letter=2  ACCOUNTED=7  LOST=4
```

The four lost records were exactly the unparseable ones. The dead-letter queue was
structurally incapable of catching the thing it existed to catch.

**The fix** is to make a NULL verdict impossible rather than to handle NULLs later:

```python
when(col("order_id").isNull() | col("amount").isNull() | ..., lit(False))
  .otherwise(<predicate>)
```

Now every row gets an explicit `TRUE` or `FALSE`. Verified: `LOST=0`, and all four
corrupt payloads appear in the dead-letter topic with their raw text preserved.

**The transferable lesson:** in any data pipeline, decide what a NULL verdict *means*
before you filter. "Not valid" and "not yet known" are different states and usually need
different destinations.

### 4.2 Non-idempotent writes (at-least-once is not a delivery guarantee)

Spark's `checkpointLocation` gives **at-least-once** processing. The failure mode is
precise: the batch writes to MongoDB, then the process dies *before* the checkpoint
commits. On restart Spark replays that batch. With a blind append, the order now exists
twice and revenue is overstated.

The tempting fix is a bare `try/except` that logs and continues. That is **worse**: the
checkpoint still records the batch as complete, so those records are gone permanently
and the only trace is one log line. A pipeline that quietly discards data is more
dangerous than one that stops.

The fix has two halves, and **both are required**:

1. **Never swallow the failure.** Let the query fail so the batch is replayed.
2. **Make the sink idempotent**, so replaying is harmless:
   - events carry a monotonic `lsn`
   - within a micro-batch, collapse to the newest `lsn` per `order_id` using a
     `row_number()` window
   - against the sink, discard any event whose `lsn` is not **strictly greater** than the
     persisted `lsn` — this kills both replays and out-of-order arrivals
   - write with `_id = order_id`, so a replay replaces rather than inserts
   - `op = D` becomes a real delete, which is idempotent by nature

Verified by replaying the same 200 events with the checkpoint deleted:

```
[batch 0] processed=200 accepted=183 invalid=13 late=4 superseded=42 stale=133
          upserted=0 deleted=0 op_mix={'D': 8} unhandled_ops={} reconciles=True
Mongo after replay: 133 documents, 0 duplicates
```

`upserted=0`. Every event was rejected as either superseded-in-batch or stale. The eight
replayed deletes returned 0 rows because those orders were already gone.
`13 + 4 + 42 + 133 + 0 + 8 = 200`.

**The transferable lesson:** "exactly-once" is not something you get from a checkpoint.
You get at-least-once plus an idempotent sink, which together give *effectively-once*.
Anyone who claims true exactly-once without naming the sink's idempotency key has not
thought it through.

### 4.3 Type contracts at tool boundaries

Two failures here were pure type mismatches between tools that all "speak Parquet":

```
AnalysisException: Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))
Cannot cast 2023-03-22T00:00 into a BsonValue. TimestampNTZType has no matching BsonValue
```

- pandas 2.x defaults to `datetime64[ns]`. pyarrow writes that as a bare INT64 with an
  *unflagged* nanosecond timestamp, and Spark refuses it because it cannot infer the
  resolution. Fix: coerce to `datetime64[us]` in the batch job.
- BSON has no `TimestampNTZ` type at all. Fix: project the dimension to the four
  attributes the stream actually needs, and stringify any timestamp column.

**The transferable lesson:** the producer of a dataset owns its contract. The consumer's
type support is an upper bound on what you may send it. A "format we both agree on" is
not a contract — an explicit, validated schema is.

---

# 5. Verified run: what actually happened

Clean slate: Mongo dropped, checkpoint removed, both topics recreated, 200 events
produced.

| Measurement | Result |
|---|---|
| events produced / processed | 200 / **200** |
| invalid (10 schema-invalid + 3 unparseable) | **13** → dead-letter topic |
| late events (older than the 1h window) | **4** → dead-letter topic |
| dead-letter topic total | **17** = 13 + 4 |
| superseded within a micro-batch | 18 |
| Mongo documents | **133** = producer's own reported "133 live orders" |
| distinct `order_id` | 133 |
| **duplicate order rows** | **0** |
| `_id == order_id` for every document | **True** |
| `op` distribution | `I: 109, U: 24` — no `D` survived, deletes applied |
| enrichment nulls on `name` | **0** (broadcast join resolved all 133) |
| `json_str` / `is_valid` in the sink | absent (scratch columns never published) |
| Kibana / Elasticsearch | HTTP 200 / logs indexed |
| Data quality gate, clean source | 13 checks passed |
| Data Quality gate, corrupted source | **2 failures, publication blocked, snapshot preserved** |

Reconciliation identity, logged per micro-batch:

```
processed = invalid + late + superseded + stale + upserted + deleted
```

`reconciles=True` is asserted in every batch log line. If your counters do not sum back
to your input, you do not yet understand your own pipeline.

The single most useful line for an interview is the reconciliation, because it proves
you are not guessing about correctness.

---

# 6. File-by-file walkthrough

### `docker-compose.yml`
Six services: Postgres 15, Confluent Kafka 7.5 + ZooKeeper, MongoDB 7,
Elasticsearch 8.11, Kibana. Host ports are parameterised via `.env` so the stack can run
beside other projects. Healthchecks plus `depends_on: service_healthy` mean the pipeline
starts against a ready database, not a booting one. Airflow sits behind an opt-in
`--profile airflow` because its image is large and it is not required for the stream.

### `data/init_postgres.sql`
Creates and seeds `customers` — 10 rows, an Indian retail dimension with `customer_id`,
`name`, `city`, `segment`, `signup_date`, `updated_at`. This is the reference data that
gets broadcast into the stream.

### `data/init_airflow.sql`
Creates the `airflow` metadata database and role, separate from `retail_src`. Sharing a
database between the orchestrator and the workload is how you accidentally let a
migration drop a business table.

### `src/validation.py` — the quality gate
Thirteen checks in five families. Returns a report so **all** failures surface at once
instead of dying on the first one.

| Family | Checks |
|---|---|
| Completeness | `row_count_minimum`, `required_columns_present`, `null_rate`, `no_empty_strings` |
| Uniqueness | `customer_id_unique`, `customer_id_numeric`, `customer_id_positive` |
| Validity | `segment_domain`, `signup_date_parseable`, `signup_date_not_future` (warning) |
| Consistency | `row_count_reconciles_with_source`, `no_rows_lost_against_source_watermark` |
| Freshness | `snapshot_freshness` |

Thresholds are environment-configurable (`DQ_MIN_ROWS`, `DQ_MAX_NULL_RATE`,
`DQ_MAX_AGE_HOURS`). The consistency checks compare against the source's own row count
and `MAX(updated_at)` — the only way to notice dropped or duplicated rows is to compare
against the source.

`tests/test_validation.py` asserts every check fires on data crafted to break it, plus a
clean frame that must pass, plus a case that warns without blocking. **A gate that never
fails is decoration.**

### `src/batch_ingest.py`
Reads `customers` via SQLAlchemy, then coerces dtypes to the subset Spark can read
(`normalise_for_spark`). Writes to a **candidate** path, deliberately not the published
one, so a failed validation cannot overwrite the snapshot the streaming job is reading.
Also exposes `fetch_source_metadata()` returning the source row count and high-water
mark, which is what makes reconciliation possible.

### `src/run_pipeline.py` — the coordinator
Three composable stages, so the CLI and the Airflow DAG run the *same* implementation:

- `extract_stage()` → candidate snapshot
- `validate_stage()` → quality gate; on failure **quarantines** the candidate to
  `data/rejected/<timestamp>.parquet` and raises
- `publish_stage()` → atomic promote via write-to-staging then `os.replace`, so a reader
  sees either the whole old snapshot or the whole new one, never a truncated file

Stages **raise** rather than return `False`. This is deliberate and was a real bug: an
Airflow `PythonOperator` treats any non-exception return value as success, so a gate that
returned `False` produced a green task while publishing nothing. Verified: with corrupted
source, `validate` goes to `failed` and `publish` to `upstream_failed`.

### `src/producer.py` — the CDC simulator
`OrderEventSimulator` holds `live_orders` and per-order history so that **updates and
deletes reference an order that was actually inserted**. The earlier version generated a
fresh `uuid4` per event, which made "update" meaningless.

Every event carries a monotonic `lsn`, and all events for one order are published with
`order_id` as the Kafka key so per-key ordering is real. Chaos is explicit and
probabilistic: 2% unparseable bytes, 2% late/out-of-order (re-emits an *older* version of
an already-advanced order), 2% legitimate negative-amount refund, 5% schema-invalid, the
rest inserts/updates/deletes. Refunds matter: they are the case a naive
`^[0-9]+$` validation throws away as garbage.

### `src/spark_pipeline.py` — the streaming engine
`foreachBatch` handler, in order:

1. **Parse** — `from_json` against a string-typed schema, keeping the raw `json_str`
2. **Validate** — `when/otherwise` so no NULL verdict escapes; required fields, format
   checks, `op` in I/U/D
3. **Lateness** — `unix_timestamp()` (note: `current_timestamp()` is frozen at query start
   in Spark) against a configurable allowed-lateness window
4. **Collapse** — `row_number()` over `partitionBy("order_id").orderBy(lsn desc)` keeps
   the newest version per order
5. **Guard** — left-join against the `lsn` already persisted; keep only strictly greater
6. **Apply CDC** — `D` → `delete_many`, `I`/`U` → upsert on `_id = order_id`
7. **Harmonise** — broadcast join to the TTL-refreshed dimension, projecting only the
   attributes the sink can represent
8. **Reconcile** — one `groupBy` aggregation produces the op mix and the log line

`CustomerDimensionCache` re-reads the Parquet snapshot on a TTL *or* when the file mtime
changes, because broadcasting the dimension once at startup means attribute changes never
reach a long-running stream. A missing dimension raises instead of returning an empty
frame, which would silently enrich every order with NULLs forever.

Source options: `maxOffsetsPerTrigger` (the real backpressure control — a slow consumer
should read less per batch, not accumulate an unbounded one), `kafka.group.id`,
`failOnDataLoss=false`.

### `src/utils/es_logger.py`
A `logging.Handler` that indexes each record into `pipeline-logs-<date>` with timestamp,
level, logger, message, module, hostname, and the full traceback when present. Built
fail-soft: 1-second timeout, 0 retries, exceptions swallowed. Local
`RotatingFileHandler` (2 MB × 3 backups) is always attached. Console + file + ES.

### `airflow_dag.py`
Three `PythonOperator` tasks with explicit `extract >> validate >> publish` dependencies,
Airflow 2.x `schedule=` (not the removed `schedule_interval=`), `catchup=False`,
`max_active_runs=1`, and exponential backoff on retries. `validate` sets **`retries=0`**:
a quality breach is deterministic, so retrying re-reads the same bad rows and only delays
the alert. The source tree is resolved from `RETAIL_PIPELINE_SRC` rather than a hardcoded
container path.

---

# 7. The 12 questions you will be asked

### 1. Walk me through the architecture.
Seven layers: Postgres source, scheduled JDBC ingestion, Parquet candidate plus a
13-check quality gate, Kafka as the buffer keyed by `order_id`, Spark Structured
Streaming micro-batches, MongoDB as the serving store, a Kafka dead-letter topic for
rejects, and Airflow plus Elasticsearch for orchestration and observability. Batch for
the slow-changing dimension, streaming for the fast-changing order events.

### 2. Why a broadcast join? When would that break?
The dimension is ~4 KB, so Spark copies it to every executor and the join runs locally
with no network shuffle, which is the single biggest cost in a Spark pipeline. It breaks
when the small side stops being small — roughly beyond 10–100 MB depending on executor
memory — at which point the broadcast stops fitting and a shuffle join wins. I would also
consider a partitioned dimension table instead, since broadcast does not scale with
executor count.

### 3. How do you prevent duplicate orders if Spark replays a micro-batch?
Two layers. Events carry a monotonic `lsn`; a micro-batch is collapsed to the newest
`lsn` per `order_id`, and anything not strictly greater than the `lsn` already in the
sink is discarded. Then the sink itself is idempotent, because documents are written with
`_id = order_id` so a replay replaces rather than inserts. Deletes are real deletes, so
they are idempotent by nature. I verified it by replaying all 200 events with the
checkpoint deleted: `upserted=0`, document count unchanged, zero duplicates.

### 4. Then what does "exactly-once" mean here?
At-least-once from the checkpoint, plus an idempotent sink, giving effectively-once.
Checkpointing alone cannot give true exactly-once, because the window between the sink
write and the checkpoint commit is unavoidable. The only real exactly-once is a
transactional sink, which for MongoDB would mean running inside a transaction the
connector does not support.

### 5. What happens to an order whose customer changes segment?
The stream does not know until the batch job republishes the dimension. The cache
re-reads on a 300-second TTL or when the file mtime changes, so the change propagates
within that window. The current row is not retroactively updated, so historical
enrichment reflects the attribute at read time, not at event time. If that matters for
reporting, the fix is slowly-changing-dimension type 2 with an effective-from date rather
than overwriting in place.

### 6. A CDC event arrives late. How do you handle it?
Three layers. A lateness window routes anything older than the threshold to the
dead-letter topic with reason `late_event` rather than silently applying it. Within the
window, the `lsn` guard rejects it if a newer version is already persisted, so an old
update cannot overwrite a new one. And the Kafka key means all events for one order stay
in relative order within their partition. Beyond the window, recovery is a replay of the
source topic from an earlier offset.

### 7. How do you know the pipeline is healthy?
Every micro-batch logs a line whose counters sum back to the events processed, with an
asserted `reconciles=` flag: `processed = invalid + late + superseded + stale + upserted
+ deleted`. If those do not sum, records are unaccounted for. Alongside that: consumer
lag per partition with a warning threshold, dead-letter counts by reason, snapshot
freshness, and all of it shipped to Elasticsearch for Kibana. The rule is that a pipeline
publishing wrong numbers quietly is worse than one that fails loudly.

### 8. The pipeline worked yesterday and is now slow. How do you debug?
Start from metrics, not code. Compare run duration against its own recent history to
confirm the regression and when it started. Then work down the layers: source latency
and row-count change; consumer lag and whether `maxOffsetsPerTrigger` is now the
bottleneck; data skew, since one hot partition key makes one task do all the work;
partition pruning being lost after a stats refresh; GC pressure and executor memory;
small files in the Parquet snapshot; and finally downstream throttling or a slow sink. The
per-batch counters localise it fast, because a change in the `superseded` or `stale`
ratio tells you whether the *data* changed rather than the infrastructure.

### 9. Why Spark Structured Streaming over Flink or Kafka Streams?
One API for batch and streaming with the same optimizer and sink connectors, which
matters here because the same broadcast-join and validation logic has to run in both the
batch and the stream. Its cost is micro-batch latency. If I needed sub-second latency
with complex event-time windows and exactly-once state, I would reach for Flink; for
simple high-throughput routing inside Kafka, Kafka Streams.

### 10. What does `op` do in your pipeline, and how do you test it?
`I` and `U` upsert on `_id = order_id`; `D` issues a real delete. The test is a full
reconciliation against the producer's own final state, plus a replay: on a clean run the
document count equals the producer's reported live-order count, and on replay
`upserted=0` with zero duplicates. That is a stronger claim than "I think it works".

### 11. How would you scale this to 100k events/second?
Partition the topic so throughput scales, and key by `order_id` so partitions stay
balanced — a hot key is the skew risk to watch. Raise partitions and executor count,
tune `maxOffsetsPerTrigger` so micro-batches stay within memory. Move the LSN state
store off a per-batch driver read toward something designed for it, or shard the state by
partition so each task only reads its own keys. Replace the local DLQ with a real
dead-letter topic at scale. And the checkpoint/offset volume becomes the thing that needs
its own storage tier.

### 12. Why MongoDB and not back into PostgreSQL?
Writing a high-volume stream into the operational database creates lock contention with
checkout traffic — the exact failure this pipeline exists to prevent. The document model
suits the enriched order, which is self-contained, and gives single-`_id` lookups for
operational dashboards. I would keep the analytical copy in a warehouse or on Parquet and
treat Mongo as the operational read model, not the system of record.

---

# 8. Known limitations — say these before they find them

Naming a limitation is a strength in an interview. Each of these is a real constraint.

1. **The LSN state store is a driver-side read per micro-batch.** It collects distinct
   order IDs and queries Mongo for their versions. That is fine at this scale and wrong
   at 100k events/second. The fix is a partitioned state store — Redis, HBase, or a
   sharded Delta/Iceberg table — so each task reads only its own keys.

2. **Deletes bypass Spark.** `delete_many` is a driver-side side effect, so it is not
   part of the checkpointed plan. Deletes are idempotent so replay is safe, but a partial
   failure could leave a delete applied while the batch replays. Acceptable here, and it
   should be stated.

3. **Dimension enrichment is not historised.** See question 5 — the value read is the
   value now, not the value at event time. Slowly-changing dimensions would fix it.

4. **Validation is regex and rule based, not schema-registry based.** A real platform
   would use a schema registry with compatibility rules so a breaking producer change
   fails CI rather than being caught in the DLQ at 3am.

5. **Single-node everything.** One broker, one Mongo, one-node Elasticsearch, local
   Parquet. Real deployment maps to MSK/S3/EMR or Event Hubs/ADLS/Databricks, but
   replication, partitioning strategy and leader failover are unexercised here.

6. **No backfill tooling.** Recovering a historical window means deleting the checkpoint
   and replaying from an offset, which works but is manual.

7. **The Airflow DAG schedules only the batch stage.** The streaming job runs
   continuously outside the orchestrator. In production it would be a managed long-running
   job with its own deployment and alerting.

8. **The producer is a simulator, not Debezium.** It reproduces CDC semantics
   faithfully — stable keys, I/U/D, monotonic LSN, out-of-order events — but it is not a
   real connector reading a write-ahead log.

---

# 9. Command reference

```bash
./scripts/run.sh up            # postgres, kafka, mongo, elasticsearch, kibana
./scripts/run.sh topics        # orders_stream + orders_stream_dlt, 3 partitions each
./scripts/run.sh batch         # extract -> validate -> publish (with the quality gate)
./scripts/run.sh stream        # Spark Structured Streaming consumer (blocking)
./scripts/run.sh produce       # 200 CDC events into Kafka
./scripts/run.sh verify        # assert one document per order_id
./scripts/run.sh test          # data quality gate unit tests
./scripts/run.sh airflow       # start Airflow and trigger the batch DAG
./scripts/run.sh test-airflow  # validation tests + DAG import check
```

Inspect results:

```bash
# enriched orders
docker exec -it rp-mongo mongosh retail_analytics \
  --eval "db.order_facts.find().limit(3).pretty()"

# dead-letter records
docker exec -it rp-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic orders_stream_dlt --from-beginning

# rejected batch snapshots kept for triage
ls data/rejected/

# streaming logs
tail -f logs/pipeline.log
```

Two environment traps that will bite you, both handled by `scripts/run.sh`:

- **Do not put the venv under `~/Documents`.** If iCloud Drive manages that folder,
  unmaterialised files get the macOS `dataless` flag and `import pandas` blocks forever
  on a file read with no error message. Keep it outside the synced tree.
- **Pin `PYSPARK_PYTHON`.** Otherwise the worker resolves the system `python3` and dies
  with `PYTHON_VERSION_MISMATCH` against the driver.
