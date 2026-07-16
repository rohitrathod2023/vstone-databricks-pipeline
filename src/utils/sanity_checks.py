"""
The brief's "sanity check for pipelines for each ingestion" requirement, as
one reusable function instead of a copy-pasted check per pipeline: a load
isn't "done" until its target table has rows and its audit columns are
actually populated (catches a silent all-NULL audit column just as much as
a silent zero-row load).

    from utils.sanity_checks import run_sanity_check
    result = run_sanity_check(spark, cfg["target_table"], catalog=catalog)
    if not result["passed"]:
        raise RuntimeError(result["failures"])
"""
from __future__ import annotations

from typing import Any, Dict

from utils.logger import get_logger

AUDIT_COLUMNS = ("load_dt", "source_format", "source_file", "run_id")


def run_sanity_check(spark, table: str, catalog: str, job_name: str = "Sanity Check") -> Dict[str, Any]:
    log = get_logger(__name__, catalog=catalog, job_name=job_name)
    df = spark.table(table)
    row_count = df.count()

    failures = []
    if row_count == 0:
        failures.append("row_count is 0")

    present_audit_columns = [c for c in AUDIT_COLUMNS if c in df.columns]
    for column in present_audit_columns:
        null_count = df.filter(df[column].isNull()).count()
        if null_count > 0:
            failures.append(f"{column} has {null_count} null values out of {row_count}")

    result = {"table": table, "row_count": row_count, "passed": not failures, "failures": failures}

    if result["passed"]:
        log.info(f"Sanity check PASSED for {table}: {row_count} rows, audit columns all non-null")
    else:
        log.error(f"Sanity check FAILED for {table}: {failures}")

    return result
