"""
Apache Airflow DAG for the batch customer-dimension stage.

Why a DAG rather than cron: a cron job fires on a clock regardless of whether
its predecessor finished. Here `validate` cannot start until `extract` has
succeeded, and `publish` cannot start until `validate` has passed, so a slow or
failed upstream stage delays the run instead of reading incomplete data.

Start it with:
    docker compose --profile airflow up -d
    ./scripts/run.sh airflow
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# The project is bind-mounted into the scheduler container, so resolve the
# source tree from the environment rather than hardcoding a container path.
PROJECT_SRC = os.environ.get("RETAIL_PIPELINE_SRC", "/opt/airflow/project/src")
sys.path.insert(0, PROJECT_SRC)

from run_pipeline import extract_stage, validate_stage, publish_stage  # noqa: E402

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="retail_customer_dimension",
    default_args=default_args,
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "batch", "data-quality"],
    doc_md=__doc__,
) as dag:
    extract = PythonOperator(
        task_id="extract_customer_dimension",
        python_callable=extract_stage,
        doc_md="Pull the customer dimension from Postgres into a candidate Parquet snapshot.",
    )

    validate = PythonOperator(
        task_id="validate_customer_dimension",
        python_callable=validate_stage,
        # A breach of the data quality gate is deterministic, not transient:
        # retrying re-reads the same bad rows and only delays the alert. Only
        # the I/O stages above get retries.
        retries=0,
        doc_md=(
            "Data quality gate: completeness, uniqueness, validity, reconciliation "
            "against the source row count, and freshness. Quarantines the candidate "
            "and fails the run on any error-level breach."
        ),
    )

    publish = PythonOperator(
        task_id="publish_customer_dimension",
        python_callable=publish_stage,
        doc_md="Atomically promote the validated candidate to the path the stream reads.",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    extract >> validate >> publish
