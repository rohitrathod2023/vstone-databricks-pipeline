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
  - Skip-based on top of that (force=False by default): a re-run with nothing
    changed doesn't re-sort/re-window all 24.6M rows of cars.csv just to
    rewrite the same output — it checks a completion marker and returns the
    existing row counts instead. Mirrors COPY INTO's own default-skip/
    force-reprocess behavior (see pipelines/bronze/copy_into.py) and the
    download notebook's existing force parameter, so the same idempotency
    philosophy holds across the whole Day 1-3 pipeline.

Called from src/notebooks/01_data_chunking.py, which is the thin Databricks
Job entrypoint — all real logic lives here so it's unit-testable without a
running job (see tests/unit/test_chunking.py).
"""
from __future__ import annotations

import os

from utils.audit import add_audit_columns
from utils.config_loader import get_source_config, get_source_schema
from utils.io_readers import read_source, write_source
from utils.logger import current_run_id, get_logger

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


_COMPLETION_MARKER_NAME = "_chunking_complete"


def _completion_marker_path(env: str) -> str:
    """All 4 chunks share one parent directory -- derive it from chunk1_csv's
    config rather than hardcoding "chunks/" a second time here."""
    chunks_dir = get_source_config("chunk1_csv", env=env)["path"].rsplit("/", 1)[0]
    return f"{chunks_dir}/{_COMPLETION_MARKER_NAME}"


def chunking_already_complete(env: str = "dev") -> bool:
    """True only if a prior run finished writing all 4 chunks. Checks the
    completion marker specifically (written last, only after every chunk
    succeeds) rather than "do the chunk files exist" -- a run that crashed
    partway through writing would leave a partial file that looks "done" to
    a bare existence check, silently leaving bad data in place."""
    return os.path.exists(_completion_marker_path(env))


def mark_chunking_complete(env: str) -> None:
    with open(_completion_marker_path(env), "w") as f:
        f.write(f"run_id={current_run_id()}\n")


def run(spark, env: str = "dev", force: bool = False) -> dict:
    """Runs the full chunking pipeline for the given environment. Returns a dict of
    {chunk_key: row_count} for logging/testing/verification.

    force=False (default) skips the actual split/write if chunking_already_complete()
    is True, returning the existing chunks' row counts instead -- pass force=True
    to reprocess cars.csv regardless of what's already there."""
    _validate_split()
    catalog = get_source_config("chunk1_csv", env=env)["target_table"].split(".")[0]
    log = get_logger(__name__, catalog=catalog, job_name="Data Chunking")

    if not force and chunking_already_complete(env):
        log.info("Chunks already present and complete — skipping (pass force=True to redo)")
        results = {
            chunk_key: read_source(
                spark, get_source_config(chunk_key, env=env), schema=get_source_schema(chunk_key)
            ).count()
            for _, chunk_key in CHUNK_PLAN
        }
        log.info(f"Existing chunk row counts: {results}")
        return results

    log.info("Reading raw_cars from Volumes")
    raw_cfg = get_source_config("raw_cars", env=env)
    cars_df = read_source(spark, raw_cfg, schema=get_source_schema("raw_cars"))

    total_rows = cars_df.count()
    log.info(f"raw_cars loaded: {total_rows} rows")

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    # date is now StringType (Bronze reads are explicitly all-string, see
    # config/schemas.py) rather than inferred TimestampType -- this still
    # sorts correctly because the source's ISO8601 format
    # ("2023-06-02T12:36:03.093Z") is lexicographically sortable, not
    # because of any special handling here.
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
    mark_chunking_complete(env)
    return results


if __name__ == "__main__":
    # Allows a quick local smoke-test in a Databricks notebook cell:
    #   from pipelines.ingestion.chunking import run
    #   run(spark, env="dev")
    raise SystemExit(
        "This module is meant to be imported and called with an active Spark session "
        "(see src/notebooks/01_data_chunking.py), not run as a standalone script."
    )
