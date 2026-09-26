"""
Batch ingestion module: extracts customer dimension data from the PostgreSQL
OLTP database and stores it as columnar Parquet files in the data lake storage
for downstream Spark broadcast join processing.
"""
import os
import sys
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

sys.path.append(os.path.dirname(__file__))
from utils.es_logger import get_logger

logger = get_logger("batch_ingest")

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PG_CONN = dict(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    dbname=os.getenv("PG_DB", "retail_src"),
    user=os.getenv("PG_USER", "retail"),
    password=os.getenv("PG_PASSWORD", "retail"),
    connect_timeout=10,
)
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data", "lake", "customers.parquet")


def normalise_for_spark(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce dtypes to the subset Spark can read.

    pandas 2.x defaults to datetime64[ns], which pyarrow writes as a bare INT64
    with an unflagged nanosecond timestamp. Spark refuses that
    ("Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))") because it cannot
    infer the resolution. Microsecond timestamps are written with a proper
    converted type and are portable.
    """
    import datetime as dt

    for column in df.columns:
        series = df[column]
        if pd.api.types.is_datetime64_any_dtype(series):
            df[column] = series.astype("datetime64[us]")
        elif series.dtype == object and len(series) and isinstance(series.dropna().iloc[0], dt.date):
            df[column] = pd.to_datetime(series).astype("datetime64[us]")
    return df


def extract_customers() -> pd.DataFrame:
    logger.info(f"Connecting to source Postgres at {PG_CONN['host']}:{PG_CONN['port']}/{PG_CONN['dbname']}")
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=PG_CONN["user"],
        password=PG_CONN["password"],
        host=PG_CONN["host"],
        port=PG_CONN["port"],
        database=PG_CONN["dbname"],
    )
    engine = create_engine(url, connect_args={"connect_timeout": PG_CONN["connect_timeout"]})
    try:
        with engine.connect() as conn:
            df = pd.read_sql("SELECT * FROM customers", conn)
    finally:
        engine.dispose()

    logger.info(f"Extracted {len(df)} rows from customers")
    df = normalise_for_spark(df)
    logger.info(f"Normalised dtypes for Spark: {dict(df.dtypes.astype(str))}")
    return df


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
