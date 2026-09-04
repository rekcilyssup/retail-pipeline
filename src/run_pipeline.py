"""
Pipeline Coordinator: sequences batch extraction and data quality validation
with automated retries and exit status verification.
"""
import subprocess
import sys
import time
import os

sys.path.append(os.path.dirname(__file__))
from utils.es_logger import get_logger

logger = get_logger("orchestrator")


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


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CUSTOMER_LAKE_PATH = os.path.join(PROJECT_ROOT, "data", "lake", "customers.parquet")
BATCH_SCRIPT_PATH = os.path.join(PROJECT_ROOT, "src", "batch_ingest.py")


def validate_batch_output():
    if not os.path.exists(CUSTOMER_LAKE_PATH):
        logger.error(f"Validation failed: customer dimension parquet not found at {CUSTOMER_LAKE_PATH}")
        return False
    logger.info(f"Validation passed: customer dimension present ({os.path.getsize(CUSTOMER_LAKE_PATH)} bytes)")
    return True


if __name__ == "__main__":
    if not run_step("batch_ingest", [sys.executable, BATCH_SCRIPT_PATH]):
        sys.exit(1)

    if not validate_batch_output():
        sys.exit(1)

    logger.info("Batch stage complete. Launch the streaming job with: python src/spark_pipeline.py")
