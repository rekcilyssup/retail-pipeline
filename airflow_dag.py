"""
Apache Airflow DAG for scheduled batch ingestion and lake staging.
Orchestrates customer dimension table extraction, validation, and task dependencies with automated retries.
"""
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/project/src")  # adjust to your mount path
from batch_ingest import extract_customers, load_to_lake

default_args = {
    "owner": "you",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="retail_batch_ingest",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    def _extract_and_load():
        df = extract_customers()
        load_to_lake(df)

    ingest_task = PythonOperator(
        task_id="extract_and_load_customers",
        python_callable=_extract_and_load,
    )
