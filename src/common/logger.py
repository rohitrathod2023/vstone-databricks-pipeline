"""
Custom logger for the VStone traffic-simulator pipeline.

Every notebook / module in this repo gets its logger the same way:

    from common.logger import get_logger
    log = get_logger(__name__, catalog="vstone_traffic_dev", job_name="Data Chunking")

    log.info("starting chunk split")
    log.error("chunk 4 (XML) failed schema validation", exc_info=True)

Two things happen on every call:
  1. A standard Python logger writes to stdout — this is what shows up in the
     Databricks job run UI automatically, no extra setup needed.
  2. (Optional, on by default) the same event is appended as a row to
     `<catalog>.audit.pipeline_logs` — a Delta table — so the "Audit &
     Observability Layer" in the architecture diagram is a real, queryable
     thing you can demo on Day 4 / Day 10, not just a box on a picture.

Delta writes are best-effort: if the audit table/schema doesn't exist yet
(e.g. very first run before Bronze setup) or Spark isn't available (e.g.
running a unit test locally), logging still works — it just skips the Delta
write and says so once at DEBUG level, rather than crashing your job over a
logging call.
"""
from __future__ import annotations

import logging
import sys
import uuid
from datetime import datetime, timezone
from typing import Optional

_RUN_ID = str(uuid.uuid4())  # one run id per process/job run, shared by every logger

_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"


class _DeltaAuditHandler(logging.Handler):
    """Best-effort handler that appends log records to <catalog>.audit.pipeline_logs.

    Designed to never raise: any failure to write is logged once at DEBUG via
    the parent logger and then silently disabled for the rest of the run.
    """

    def __init__(self, catalog: str, job_name: str, spark=None):
        super().__init__()
        self.catalog = catalog
        self.job_name = job_name
        self.run_id = _RUN_ID
        self._spark = spark
        self._disabled = False
        self._table = f"`{catalog}`.audit.pipeline_logs"

    def _get_spark(self):
        if self._spark is not None:
            return self._spark
        try:
            from pyspark.sql import SparkSession  # noqa: WPS433 (intentional local import)
            return SparkSession.getActiveSession()
        except Exception:  # pragma: no cover - environment without pyspark
            return None

    def emit(self, record: logging.LogRecord) -> None:
        if self._disabled:
            return
        spark = self._get_spark()
        if spark is None:
            self._disabled = True
            logging.getLogger(__name__).debug(
                "Delta audit logging disabled: no active Spark session (ok in local/unit-test runs)."
            )
            return
        try:
            row = [(
                datetime.now(timezone.utc),
                self.run_id,
                self.job_name,
                record.name,
                record.levelname,
                self.format(record),
            )]
            df = spark.createDataFrame(
                row,
                schema="event_ts timestamp, run_id string, job_name string, module string, level string, message string",
            )
            (
                df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(self._table)
            )
        except Exception as exc:  # noqa: BLE001 - logging must never break the pipeline
            self._disabled = True
            logging.getLogger(__name__).debug(
                "Delta audit logging disabled after failed write to %s: %s", self._table, exc
            )


def get_logger(
    name: str,
    catalog: Optional[str] = None,
    job_name: str = "vstone-pipeline",
    level: int = logging.INFO,
    write_to_delta: bool = True,
    spark=None,
) -> logging.Logger:
    """Return a configured logger. Safe to call repeatedly (won't duplicate handlers)."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(logging.Formatter(_FORMAT, _DATEFMT))
        logger.addHandler(stream_handler)

    if write_to_delta and catalog:
        already_has_delta_handler = any(isinstance(h, _DeltaAuditHandler) for h in logger.handlers)
        if not already_has_delta_handler:
            delta_handler = _DeltaAuditHandler(catalog=catalog, job_name=job_name, spark=spark)
            delta_handler.setFormatter(logging.Formatter("%(message)s"))
            delta_handler.setLevel(level)
            logger.addHandler(delta_handler)

    logger.propagate = False
    return logger


def current_run_id() -> str:
    """The shared run id used for every _DeltaAuditHandler in this process — use this to
    tag output tables/audit columns so a single job run is traceable end to end."""
    return _RUN_ID
