"""
Spark Structured Streaming Engine:
  - Consumes high-velocity order events from Apache Kafka
  - Validates schema and routes malformed events to a Dead-Letter Queue (DLQ)
  - Harmonizes real-time facts with customer dimension reference data via broadcast join
  - Writes enriched records to MongoDB operational document store
  - Emits telemetry metrics to Elasticsearch/Kibana
"""
import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_timestamp, when, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

sys.path.append(os.path.dirname(__file__))
from utils.es_logger import get_logger

logger = get_logger("spark_pipeline")

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders_stream")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/retail_analytics.order_facts")
DEAD_LETTER_PATH = os.path.join(PROJECT_ROOT, "data", "dead_letter")
CUSTOMER_LAKE_PATH = os.path.join(PROJECT_ROOT, "data", "lake", "customers.parquet")
CHECKPOINT_PATH = os.path.join(PROJECT_ROOT, "data", "checkpoints", "orders_stream")

event_schema = StructType([
    StructField("op", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),   # read as string first; validate before casting
    StructField("amount", StringType()),
    StructField("product_category", StringType()),
    StructField("event_time", StringType()),
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



def load_customer_dim(spark: SparkSession) -> DataFrame:
    """Batch reference data harmonized into the streaming job via a broadcast join."""
    if not os.path.exists(CUSTOMER_LAKE_PATH):
        logger.warning("Customer dimension not found -- run batch_ingest.py first")
        return spark.createDataFrame([], schema="customer_id INT, name STRING, city STRING, segment STRING")
    return spark.read.parquet(CUSTOMER_LAKE_PATH)


def process_batch(batch_df: DataFrame, batch_id: int, customer_dim: DataFrame):
    """foreachBatch handler: validate, harmonize, split good/bad records, write sinks."""
    try:
        total = batch_df.count()
        if total == 0:
            return

        validated = batch_df.withColumn(
            "is_valid",
            col("customer_id").rlike("^[0-9]+$") & col("amount").rlike(r"^[0-9]+\.?[0-9]*$"),
        )

        good = validated.filter(col("is_valid")).withColumn(
            "customer_id", col("customer_id").cast(IntegerType())
        ).withColumn("amount", col("amount").cast(DoubleType()))

        bad = validated.filter(~col("is_valid"))

        bad_count = bad.count()
        good_count = good.count()

        # Harmonize: enrich streaming orders with batch customer attributes
        enriched = good.join(customer_dim, on="customer_id", how="left")

        if good_count > 0:
            (enriched.write.format("mongodb").mode("append").save())

        if bad_count > 0:
            (bad.write.mode("append").json(DEAD_LETTER_PATH))
            logger.warning(f"[batch {batch_id}] {bad_count} malformed records routed to dead-letter")

        logger.info(f"[batch {batch_id}] processed={total} valid={good_count} invalid={bad_count}")

    except Exception as e:
        logger.error(f"[batch {batch_id}] processing failed: {e}", exc_info=True)
        # Record error and proceed to maintain stream liveness;
        # in production, route failure context to alert monitor and DLQ.


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    customer_dim = load_customer_dim(spark).cache()

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw_stream.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), event_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
    )

    logger.info("Starting structured streaming query against Kafka topic 'orders_stream'")

    query = (
        parsed.writeStream
        .foreachBatch(lambda df, bid: process_batch(df, bid, customer_dim))
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
