"""
Pipeline coordinator: sequences batch extraction, data quality validation and
publication, with retries and a fail-fast quality gate.

The quality gate is the point of this file. A run that extracts rows but
fails validation never publishes, so downstream consumers keep reading the
last known-good snapshot instead of silently switching to corrupt data.
"""
import os
import subprocess
import sys
import time
from datetime import datetime

import pandas as pd

sys.path.append(os.path.dirname(__file__))
from utils.es_logger import get_logger
from validation import validate_customer_dimension

logger = get_logger("orchestrator")

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
LAKE_DIR = os.path.join(PROJECT_ROOT, "data", "lake")
CANDIDATE_PATH = os.path.join(LAKE_DIR, "_candidate", "customers.parquet")
PUBLISHED_PATH = os.path.join(LAKE_DIR, "customers.parquet")
BATCH_SCRIPT_PATH = os.path.join(SRC_DIR, "batch_ingest.py")


def run_step(name: str, cmd: list, retries: int = 2):
    for attempt in range(1, retries + 1):
        logger.info(f"Running step '{name}' (attempt {attempt}/{retries}): {' '.join(cmd)}")
        result = subprocess.run(cmd)
        if result.returncode == 0:
            logger.info(f"Step '{name}' succeeded")
            return True
        logger.error(f"Step '{name}' failed with exit code {result.returncode}")
        time.sleep(2)
    logger.error(f"Step '{name}' failed after {retries} attempts -- aborting pipeline")
    return False


def publish_snapshot(df: pd.DataFrame) -> str:
    """
    Promote the validated candidate to the published path, atomically.

    Spark rejects a half-written Parquet file, and a file-exists check cannot
    distinguish a complete snapshot from a truncated one, so stage the write
    and rename into place. os.replace is atomic within a filesystem, so a
    reader either sees the whole old snapshot or the whole new one.
    """
    os.makedirs(LAKE_DIR, exist_ok=True)
    staging = PUBLISHED_PATH + ".staging"
    try:
        df.to_parquet(staging, index=False)
        os.replace(staging, PUBLISHED_PATH)
    finally:
        if os.path.exists(staging):
            os.remove(staging)
    return PUBLISHED_PATH


def quarantine_candidate(reason: str) -> str:
    """Keep the rejected snapshot for triage instead of silently deleting it."""
    quarantine_dir = os.path.join(PROJECT_ROOT, "data", "rejected")
    os.makedirs(quarantine_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    target = os.path.join(quarantine_dir, f"customers_{stamp}.parquet")
    if os.path.exists(CANDIDATE_PATH):
        os.replace(CANDIDATE_PATH, target)
    logger.error(f"Candidate quarantined at {target} ({reason})")
    return target


class StageFailure(RuntimeError):
    """
    Raised by a stage that did not meet its contract.

    Stages raise rather than return False because an Airflow PythonOperator
    treats any non-exception return value as success. A quality gate that
    returns False would be reported as a green task while the run quietly
    published nothing.
    """


def extract_stage() -> None:
    """Run the JDBC extraction, producing a candidate snapshot."""
    if not run_step("batch_ingest", [sys.executable, BATCH_SCRIPT_PATH]):
        raise StageFailure("extraction failed after retries")


def validate_stage() -> None:
    """
    Apply the data quality gate to the candidate.

    On failure the candidate is quarantined for triage, the previously
    published snapshot is left untouched, and this raises so the orchestrator
    records a failed run and skips publication.
    """
    sys.path.insert(0, SRC_DIR)
    from batch_ingest import fetch_source_metadata

    if not os.path.exists(CANDIDATE_PATH):
        raise StageFailure(f"no candidate snapshot at {CANDIDATE_PATH}")

    candidate = pd.read_parquet(CANDIDATE_PATH)
    source = fetch_source_metadata()
    logger.info(f"Reconciling candidate against source: {source}")

    report = validate_customer_dimension(
        candidate,
        expected_rows=source["row_count"],
        source_max_updated_at=source["max_updated_at"],
    )
    report.emit()

    if not report.ok:
        failed = ", ".join(r.check for r in report.failures)
        quarantine_candidate(failed)
        raise StageFailure(
            f"quality gate blocked publication ({failed}); the previously published "
            "snapshot is unchanged and remains in use"
        )

    if report.warnings:
        logger.warning(
            f"candidate passes with {len(report.warnings)} warning(s): "
            f"{[r.check for r in report.warnings]}"
        )


def publish_stage() -> None:
    """Promote the validated candidate to the published path, atomically."""
    if not os.path.exists(CANDIDATE_PATH):
        raise StageFailure(f"nothing to publish: no candidate at {CANDIDATE_PATH}")
    candidate = pd.read_parquet(CANDIDATE_PATH)
    path = publish_snapshot(candidate)
    os.remove(CANDIDATE_PATH)
    logger.info(
        f"Published validated snapshot: {path} "
        f"({os.path.getsize(path)} bytes, {len(candidate)} rows)"
    )


def run_batch_stage():
    """
    Extract, validate, then publish, for use outside an orchestrator.

    Split into three stages so the Airflow DAG can schedule and observe them
    individually while this module stays the single implementation both
    callers share.
    """
    try:
        extract_stage()
        validate_stage()
        publish_stage()
    except StageFailure as e:
        logger.error(f"Batch stage aborted: {e}")
        return False
    return True


if __name__ == "__main__":
    if not run_batch_stage():
        sys.exit(1)

    logger.info("Batch stage complete. Launch the streaming job with: ./scripts/run.sh stream")
