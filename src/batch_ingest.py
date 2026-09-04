"""
Batch ingestion module: extracts customer dimension data from the PostgreSQL
OLTP database and stores it as columnar Parquet files in the data lake storage
for downstream Spark broadcast join processing.
"""
import os
import sys
import psycopg2
import pandas as pd

sys.path.append(os.path.dirname(__file__))
from utils.es_logger import get_logger

logger = get_logger("batch_ingest")

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PG_CONN = dict(host="localhost", port=5432, dbname="retail_src", user="retail", password="retail")
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data", "lake", "customers.parquet")


def extract_customers() -> pd.DataFrame:
    logger.info("Connecting to source Postgres to extract customers table")
    try:
        with psycopg2.connect(**PG_CONN) as conn:
            df = pd.read_sql("SELECT * FROM customers", conn)
        logger.info(f"Extracted {len(df)} rows from customers")
        return df
    except Exception as e:
        logger.error(f"Batch extraction failed: {e}", exc_info=True)
        raise


def load_to_lake(df: pd.DataFrame):
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    try:
        df.to_parquet(OUTPUT_PATH, index=False)
        logger.info(f"Wrote {len(df)} rows to {OUTPUT_PATH}")
    except Exception as e:
        logger.error(f"Failed writing to data lake: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    df = extract_customers()
    load_to_lake(df)
    logger.info("Batch ingestion complete")
