"""
Day 1 — chronological 40/30/20/10 chunker for cars.csv.

Reads raw_cars from Volumes, sorts by `date` (row order in the source file is
NOT guaranteed chronological, so we sort explicitly rather than trust it),
splits into 4 chronological slices, and writes each slice out in its target
format (chunk1/2 stay CSV, chunk3 becomes JSON, chunk4 becomes XML) to
/Volumes/<catalog>/raw/raw_volume/chunks/ — ready for Bronze ingestion on Day 2/3.

Idempotent / safe to re-run (the brief requires this explicitly):
  - Every write uses mode="overwrite" on a fixed, deterministic path per chunk,
    so re-running the job re-derives the same 4 chunks rather than appending
    duplicates or accumulating partial output.
  - The split itself is deterministic: row count comes from a full ORDER BY date,
    monotonically increasing id via row_number(), so the same input always
    produces the same 4 slices regardless of how many times this runs.

Called from src/notebooks/01_data_chunking.py, which is the thin Databricks
Job entrypoint — all real logic lives here so it's unit-testable without a
running job (see tests/unit/test_chunking.py).
"""
from __future__ import annotations

from common.audit import add_audit_columns
from common.config_loader import get_source_config
from common.io_readers import read_source, write_source
from common.logger import get_logger

# (pct_of_total, target_source_key) in chronological order — earliest slice first.
# Percentages must sum to 100; enforced by _validate_split() below.
CHUNK_PLAN = [
    (40, "chunk1_csv"),
    (30, "chunk2_csv"),
    (20, "chunk3_json"),
    (10, "chunk4_xml"),
]


def _validate_split():
    total = sum(pct for pct, _ in CHUNK_PLAN)
    if total != 100:
        raise ValueError(f"CHUNK_PLAN percentages must sum to 100, got {total}")


def run(spark, env: str = "dev") -> dict:
    """Runs the full chunking pipeline for the given environment. Returns a dict of
    {chunk_key: row_count} for logging/testing/verification."""
    _validate_split()
    catalog = get_source_config("chunk1_csv", env=env)["target_table"].split(".")[0]
    log = get_logger(__name__, catalog=catalog, job_name="Data Chunking")

    log.info("Reading raw_cars from Volumes")
    raw_cfg = get_source_config("raw_cars", env=env)
    cars_df = read_source(spark, raw_cfg)

    total_rows = cars_df.count()
    log.info(f"raw_cars loaded: {total_rows} rows")

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    ordered = cars_df.orderBy(F.col("date").asc())
    windowed = ordered.withColumn("_row_num", F.row_number().over(Window.orderBy(F.col("date").asc())))

    results = {}
    cumulative_pct = 0
    for pct, chunk_key in CHUNK_PLAN:
        start_pct = cumulative_pct
        end_pct = cumulative_pct + pct
        cumulative_pct = end_pct

        start_row = int(total_rows * start_pct / 100) + 1
        end_row = int(total_rows * end_pct / 100) if end_pct < 100 else total_rows

        chunk_df = (
            windowed.filter((F.col("_row_num") >= start_row) & (F.col("_row_num") <= end_row))
            .drop("_row_num")
        )
        row_count = chunk_df.count()

        chunk_cfg = get_source_config(chunk_key, env=env)
        source_file = f"cars.csv[rows {start_row}-{end_row}]"
        audited_df = add_audit_columns(chunk_df, source_format=chunk_cfg["format"], source_file=source_file)

        log.info(f"Writing {chunk_key}: {row_count} rows ({pct}% of total) -> {chunk_cfg['path']}")
        write_source(audited_df, chunk_cfg, mode="overwrite")

        results[chunk_key] = row_count

    log.info(f"Chunking complete: {results}")
    return results


if __name__ == "__main__":
    # Allows a quick local smoke-test in a Databricks notebook cell:
    #   from pipelines.ingestion.chunking import run
    #   run(spark, env="dev")
    raise SystemExit(
        "This module is meant to be imported and called with an active Spark session "
        "(see src/notebooks/01_data_chunking.py), not run as a standalone script."
    )
