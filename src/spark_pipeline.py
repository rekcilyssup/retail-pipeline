"""
Spark Structured Streaming Engine:
  - Consumes order events from Apache Kafka (simulated CDC: op = I / U / D)
  - Validates schema defensively and routes every malformed record to a
    dead-letter topic -- including payloads that fail JSON parsing entirely
  - De-duplicates to the newest version per order within a micro-batch, then
    discards stale/out-of-order events by comparing the event LSN against the
    LSN already persisted in the sink
  - Applies CDC semantics: I/U become idempotent upserts keyed on order_id,
    D becomes a delete, so replaying a micro-batch cannot double-count revenue
  - Harmonizes the stream with the customer dimension via a broadcast join
    against a TTL-refreshed Parquet snapshot
  - Writes enriched records to the MongoDB operational document store
  - Emits telemetry (batch counts, consumer lag) to Elasticsearch/Kibana

Delivery semantics: at-least-once. Idempotency is provided by the sink, not by
the checkpoint, so a crash between the sink write and the checkpoint commit
replays the batch harmlessly.
"""
import os
import sys
import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, to_timestamp, when, lit, coalesce, row_number,
    current_timestamp, unix_timestamp,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType,
)

sys.path.append(os.path.dirname(__file__))
from utils.es_logger import get_logger

logger = get_logger("spark_pipeline")

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders_stream")
KAFKA_DLT_TOPIC = os.getenv("KAFKA_DLT_TOPIC", "orders_stream_dlt")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "retail-order-stream")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DB = os.getenv("MONGO_DB", "retail_analytics")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "order_facts")
MONGO_URI = os.getenv("MONGO_URI", f"mongodb://localhost:{MONGO_PORT}/{MONGO_DB}.{MONGO_COLLECTION}")
DEAD_LETTER_PATH = os.path.join(PROJECT_ROOT, "data", "dead_letter")
CUSTOMER_LAKE_PATH = os.path.join(PROJECT_ROOT, "data", "lake", "customers.parquet")
CHECKPOINT_PATH = os.path.join(PROJECT_ROOT, "data", "checkpoints", "orders_stream")

MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "20000"))
DIM_REFRESH_SECONDS = int(os.getenv("DIM_REFRESH_SECONDS", "300"))
ALLOWED_LATENESS_SECONDS = int(os.getenv("ALLOWED_LATENESS_SECONDS", "3600"))
LAG_REPORT_EVERY_BATCHES = int(os.getenv("LAG_REPORT_EVERY_BATCHES", "5"))

CUSTOMER_DIM_SCHEMA = "customer_id INT, name STRING, city STRING, segment STRING"
CUSTOMER_DIM_ENRICH_COLUMNS = ("customer_id", "name", "city", "segment")

event_schema = StructType([
    StructField("op", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", StringType()),
    StructField("product_category", StringType()),
    StructField("event_time", StringType()),
    StructField("lsn", LongType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RetailOrderIntelligencePipeline")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        )
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .getOrCreate()
    )


class CustomerDimensionCache:
    """
    TTL cache for the batch-extracted customer dimension.

    The dimension is broadcast to every executor for the join, so it must be
    re-read periodically or the stream silently enriches against stale
    attributes forever.
    """

    def __init__(self, spark: SparkSession, path: str, ttl_seconds: int):
        self.spark = spark
        self.path = path
        self.ttl = ttl_seconds
        self._df = None
        self._loaded_at = 0.0
        self._mtime = None

    def get(self) -> DataFrame:
        if not os.path.exists(self.path):
            raise FileNotFoundError(
                f"Customer dimension not found at {self.path}. "
                "Run 'python src/run_pipeline.py' before starting the stream."
            )
        mtime = os.path.getmtime(self.path)
        expired = (time.time() - self._loaded_at) > self.ttl
        changed = self._mtime is not None and mtime != self._mtime
        if self._df is None or expired or changed:
            raw = self.spark.read.parquet(self.path)
            self._df = self._project_enrichment_columns(raw)
            self._loaded_at = time.time()
            self._mtime = mtime
            logger.info(
                f"Customer dimension reloaded from {self.path} "
                f"({self._df.count()} rows, columns={self._df.columns}, mtime={mtime})"
            )
        return self._df

    @staticmethod
    def _project_enrichment_columns(raw: DataFrame) -> DataFrame:
        """
        Keep only the attributes the stream needs, and stringify timestamps.

        Two reasons, both about contracts between tools:
          - column pruning: shipping signup_date/updated_at into the sink is
            pure cost, they are never read downstream
          - the MongoDB Spark connector cannot map a Spark TimestampNTZ to a
            BSON value ("TimestampNTZType has no matching BsonValue"), so any
            Parquet timestamp column would abort the whole write
        """
        from pyspark.sql.types import TimestampNTZType

        available = set(raw.columns)
        keep = [c for c in CUSTOMER_DIM_ENRICH_COLUMNS if c in available]
        if "customer_id" not in keep:
            raise ValueError(
                f"Customer dimension is missing the join key 'customer_id'; found {raw.columns}"
            )
        projected = raw.select(keep)
        for field in projected.schema.fields:
            if isinstance(field.dataType, TimestampNTZType):
                projected = projected.withColumn(field.name, col(field.name).cast("string"))
        return projected


def current_lsns(order_ids) -> dict:
    """
    Read the LSN already persisted for each order_id.

    This is the state store that makes the pipeline idempotent and
    order-insensitive: an event whose LSN is not greater than the stored LSN is
    a replay or an out-of-order arrival and must not be applied.
    """
    if not order_ids:
        return {}
    from pymongo import MongoClient

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    try:
        coll = client.get_default_database()["order_facts"]
        cursor = coll.find({"_id": {"$in": list(order_ids)}}, {"lsn": 1})
        return {doc["_id"]: doc.get("lsn", 0) for doc in cursor}
    finally:
        client.close()


def apply_deletes(order_ids) -> int:
    """Deletes are naturally idempotent, so replaying them is safe."""
    if not order_ids:
        return 0
    from pymongo import MongoClient

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    try:
        coll = client.get_default_database()["order_facts"]
        return coll.delete_many({"_id": {"$in": list(order_ids)}}).deleted_count
    finally:
        client.close()


def split_valid_and_invalid(parsed: DataFrame) -> tuple:
    """
    Split a micro-batch into (valid, invalid).

    is_valid is built with when/otherwise so a NULL predicate can never escape:
    in SQL a NULL predicate is not TRUE, and filter() drops those rows from every
    downstream branch, which is how malformed records get lost.
    """
    validated = parsed.withColumn(
        "is_valid",
        when(
            col("op").isNull() | col("order_id").isNull()
            | col("customer_id").isNull() | col("amount").isNull()
            | col("lsn").isNull() | col("product_category").isNull(),
            lit(False),
        ).otherwise(
            col("op").isin("I", "U", "D")
            & col("customer_id").rlike(r"^[0-9]+$")
            & col("amount").rlike(r"^-?[0-9]+(\.[0-9]+)?$")
            & col("product_category").rlike(r"^[A-Za-z][A-Za-z _-]*$")
        ),
    )

    good = (
        validated.filter(col("is_valid"))
        .withColumn("customer_id", col("customer_id").cast(IntegerType()))
        .withColumn("amount", col("amount").cast(DoubleType()))
        .withColumn("ingested_at", current_timestamp())
    )

    # The raw payload travels with the rejected row so triage never needs a
    # Kafka replay, and so payloads that failed JSON parsing are still visible.
    bad = (
        validated.filter(~coalesce(col("is_valid"), lit(False)))
        .withColumn("rejected_at", current_timestamp())
        .withColumn("reject_reason", lit("schema_or_parse_failure"))
    )
    return good, bad


def reject_late(good: DataFrame) -> tuple:
    """Route events older than the allowed lateness window to the dead-letter topic."""
    # unix_timestamp() with no argument is evaluated per row; current_timestamp()
    # would be frozen at query start for the lifetime of the stream.
    age = unix_timestamp() - col("event_time").cast(LongType())
    late = good.filter(age > ALLOWED_LATENESS_SECONDS)
    fresh = good.filter(age <= ALLOWED_LATENESS_SECONDS)
    return fresh, late


def latest_per_order(good: DataFrame) -> DataFrame:
    """Collapse a micro-batch to one row per order_id: the highest LSN wins."""
    window = Window.partitionBy("order_id").orderBy(col("lsn").desc())
    return good.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")


def drop_stale(latest: DataFrame, spark: SparkSession) -> DataFrame:
    """Discard events that are older than what the sink already holds."""
    ids = [r["order_id"] for r in latest.select("order_id").distinct().collect()]
    stored = current_lsns(ids)
    logger.info(f"LSN guard: comparing {len(ids)} orders against {len(stored)} existing documents")
    current_df = spark.createDataFrame(
        [(k, int(v)) for k, v in stored.items()], schema="order_id string, stored_lsn long"
    )
    return (
        latest.join(current_df, on="order_id", how="left")
        .filter(col("stored_lsn").isNull() | (col("lsn") > col("stored_lsn")))
        .drop("stored_lsn")
    )


def write_dlq(bad: DataFrame, reason: str) -> None:
    """Publish rejected records to a dedicated dead-letter topic.

    A topic rather than a folder: it is partitioned, replayable, monitorable
    and does not depend on node-local disk.
    """
    if bad.isEmpty():
        return
    payload = bad.withColumn("reject_reason", lit(reason))
    if "order_id" in payload.columns:
        payload = payload.withColumn("key", coalesce(col("order_id"), lit("")).cast("string"))
    else:
        payload = payload.withColumn("key", lit("").cast("string"))

    struct_fields = [c for c in payload.columns if c not in ("key", "value")]
    payload = payload.withColumn("value", to_json(struct(*[col(c) for c in struct_fields])).cast("binary"))
    (
        payload.select("key", "value")
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", KAFKA_DLT_TOPIC)
        .option("kafka.producer.acks", "all")
        .save()
    )
    logger.warning(f"routed {bad.count()} record(s) to dead-letter topic '{KAFKA_DLT_TOPIC}' ({reason})")


def report_consumer_lag() -> None:
    """Log how far behind the stream is. This is the backpressure signal."""
    try:
        from kafka import KafkaConsumer, TopicPartition

        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=KAFKA_GROUP_ID,
            enable_auto_commit=False,
        )
        try:
            partitions = consumer.partitions_for_topic(KAFKA_TOPIC) or set()
            tps = [TopicPartition(KAFKA_TOPIC, p) for p in sorted(partitions)]
            if not tps:
                return
            end = consumer.end_offsets(tps)
            committed = consumer.committed(tps)
            lag = {}
            for tp in tps:
                pos = committed.get(tp)
                if pos is None:
                    lag[tp.partition] = end[tp]
                else:
                    lag[tp.partition] = end[tp] - pos
            total = sum(lag.values())
            logger.info(f"consumer lag topic='{KAFKA_TOPIC}' total={total} by_partition={lag}")
            if total > MAX_OFFSETS_PER_TRIGGER * 10:
                logger.warning(
                    f"consumer lag {total} exceeds 10x maxOffsetsPerTrigger "
                    f"({MAX_OFFSETS_PER_TRIGGER}) -- processing cannot keep up with ingest"
                )
        finally:
            consumer.close()
    except Exception as e:
        logger.warning(f"lag report failed: {e}")


def process_batch(batch_df: DataFrame, batch_id: int, spark: SparkSession, dim_cache: CustomerDimensionCache, batch_counter: list):
    """foreachBatch handler: validate, dedup, guard, apply CDC, write sinks."""
    total = batch_df.count()
    if total == 0:
        return

    parsed = (
        batch_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .select("json_str", from_json(col("json_str"), event_schema).alias("data"))
        .select("json_str", "data.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
    )

    good, bad = split_valid_and_invalid(parsed)
    write_dlq(bad, "schema_or_parse_failure")

    fresh, late = reject_late(good)
    write_dlq(late.select("json_str", "order_id", "op", "event_time"), "late_event")

    invalid_total = bad.count()
    late_total = late.count()
    accepted_total = fresh.count()
    total = batch_df.count()

    if fresh.isEmpty():
        logger.info(
            f"[batch {batch_id}] processed={total} accepted=0 invalid={invalid_total} "
            f"late={late_total} superseded=0 stale=0 upserted=0 deleted=0"
        )
        return

    latest = latest_per_order(fresh)
    superseded_total = accepted_total - latest.count()
    effective = drop_stale(latest, spark)
    stale_total = latest.count() - effective.count()

    deletes = effective.filter(col("op") == "D")
    upserts = effective.filter(col("op") != "D")

    # One aggregation instead of a chain of count() actions, so the accounting
    # can never drift between the number logged and the number acted on.
    op_counts = {
        row["op"]: row["cnt"]
        for row in effective.groupBy("op").count().withColumnRenamed("count", "cnt").collect()
    }
    upsert_count = sum(n for op, n in op_counts.items() if op != "D")
    delete_count = op_counts.get("D", 0)
    unknown_ops = {str(op): n for op, n in op_counts.items() if op not in ("I", "U", "D")}

    deleted = apply_deletes([r["order_id"] for r in deletes.select("order_id").collect()])

    if not upserts.isEmpty():
        customer_dim = dim_cache.get()
        enriched = upserts.join(customer_dim, on="customer_id", how="left")
        # Serve only the contract the consumer needs, and key the document on
        # order_id so the sink itself is idempotent. Relying on the LSN guard
        # alone is not enough: it is application logic and can be bypassed.
        served = enriched.select(
            col("order_id").alias("_id"),
            "order_id", "op", "customer_id", "amount", "product_category",
            "event_time", "lsn", "ingested_at", "name", "city", "segment",
        )
        (
            served.write.format("mongodb")
            .mode("append")
            .option("idFieldList", "_id")
            .save()
        )

    batch_counter[0] += 1
    if batch_counter[0] % LAG_REPORT_EVERY_BATCHES == 0:
        report_consumer_lag()

    accounted = invalid_total + late_total + superseded_total + stale_total + upsert_count + delete_count
    logger.info(
        f"[batch {batch_id}] processed={total} accepted={accepted_total} invalid={invalid_total} "
        f"late={late_total} superseded={superseded_total} stale={stale_total} "
        f"upserted={upsert_count} deleted={deleted} "
        f"op_mix={op_counts} unhandled_ops={unknown_ops} "
        f"reconciles={accounted == total}"
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    dim_cache = CustomerDimensionCache(spark, CUSTOMER_LAKE_PATH, DIM_REFRESH_SECONDS)
    dim_cache.get()
    batch_counter = [0]

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("kafka.group.id", KAFKA_GROUP_ID)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    logger.info(
        f"Starting structured streaming query topic='{KAFKA_TOPIC}' "
        f"group='{KAFKA_GROUP_ID}' maxOffsetsPerTrigger={MAX_OFFSETS_PER_TRIGGER} "
        f"checkpoint={CHECKPOINT_PATH}"
    )

    query = (
        raw_stream.writeStream
        .foreachBatch(
            lambda df, bid: process_batch(df, bid, spark, dim_cache, batch_counter)
        )
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
