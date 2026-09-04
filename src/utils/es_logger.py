"""
Central logging and telemetry utility.

Provides structured logging across all pipeline stages to:
  1. Standard output for developer console feedback
  2. Local rotating log files in logs/pipeline.log
  3. Elasticsearch index `pipeline-logs-*` for real-time visualization in Kibana

Includes graceful degradation to file/console logging if Elasticsearch is unreachable.
"""
import os
import logging
import datetime
import socket
import traceback
from logging.handlers import RotatingFileHandler

try:
    from elasticsearch import Elasticsearch
except ImportError:
    Elasticsearch = None

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX_PREFIX = "pipeline-logs"


class ElasticsearchHandler(logging.Handler):
    """A logging.Handler that ships each record to Elasticsearch as a JSON doc."""

    def __init__(self, es_host: str = ES_HOST):
        super().__init__()
        self.es = None
        if Elasticsearch is not None:
            try:
                self.es = Elasticsearch(es_host, request_timeout=1, max_retries=0)
            except Exception:
                self.es = None  # fail soft -- monitoring must never break the pipeline


    def emit(self, record: logging.LogRecord):
        if self.es is None:
            return
        try:
            index_name = f"{ES_INDEX_PREFIX}-{datetime.date.today().isoformat()}"
            doc = {
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "module": record.module,
                "host": socket.gethostname(),
            }
            if record.exc_info:
                doc["exception"] = "".join(traceback.format_exception(*record.exc_info))
            self.es.index(index=index_name, document=doc)
        except Exception:
            # Never let a logging/monitoring failure crash the actual pipeline stage
            pass


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    # Ensure logs directory exists relative to project root
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    log_dir = os.path.join(project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "pipeline.log")

    file_handler = RotatingFileHandler(log_file, maxBytes=2_000_000, backupCount=3)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    es_handler = ElasticsearchHandler()
    es_handler.setFormatter(fmt)
    logger.addHandler(es_handler)

    return logger

