"""
Audit columns, applied the same way to every table in every layer
(Bronze/Silver/Gold) per the capstone's mandatory requirement.

    from utils.audit import add_audit_columns
    df = add_audit_columns(df, source_format="csv", source_file="chunk1.csv")

Adds:
    load_dt        — UTC timestamp this row was written (not event time)
    source_format   — csv / json / xml / delta ...
    source_file     — original file/path this row came from
    run_id          — ties every row back to the pipeline run that wrote it
                      (see utils.logger.current_run_id) — makes MERGE INTO /
                      late-arriving-data debugging traceable on Day 7
"""
from __future__ import annotations

from utils.logger import current_run_id


def add_audit_columns(df, source_format: str, source_file: str):
    """Works on a PySpark DataFrame. Kept dependency-light (no pyspark import
    at module load time) so this file can be unit-tested without a Spark session
    by passing in any object that implements .withColumn(...)."""
    from pyspark.sql import functions as F  # local import: only needed when actually called

    return (
        df.withColumn("load_dt", F.current_timestamp())
        .withColumn("source_format", F.lit(source_format))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("run_id", F.lit(current_run_id()))
    )
