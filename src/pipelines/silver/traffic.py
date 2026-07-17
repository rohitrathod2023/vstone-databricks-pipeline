"""Silver traffic: UNION ALL of the 4 Bronze traffic_counts_* tables --
same rows, same grain, split across 4 ingestion techniques as a technical
exercise, not 4 different datasets being combined side-by-side. No
fact-to-dimension join happens here; that's Gold's job.
"""
from __future__ import annotations

from functools import reduce
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.silver.header_standardization import apply_header_standardization
from pipelines.silver.quarantine import build_rejection_reason_expr

# Order matches the 4 Bronze source tables this module's caller must supply
# bronze_dfs in: traffic_counts_copyinto, traffic_counts_dlt,
# traffic_counts_autoloader, traffic_counts_pyspark.
SOURCE_TECHNIQUES = ["copyinto", "dlt", "autoloader", "pyspark"]

TRAFFIC_QUARANTINE_RULES = {
    "negative_count": (
        "enter < 0 OR exit < 0",
        "negative_count: enter or exit is negative",
    ),
    "unparseable_date": (
        "date IS NULL",
        "unparseable_date: date could not be parsed",
    ),
}


def assert_schemas_match(bronze_dfs: List[DataFrame]) -> None:
    """Assert every Bronze traffic DataFrame has identical column names/order.

    Args:
        bronze_dfs: The 4 Bronze traffic_counts_* DataFrames, in
            SOURCE_TECHNIQUES order.

    Raises:
        ValueError: If any DataFrame's columns differ from the first one --
            e.g. the PySpark/XML path's sanitize_column_name() producing a
            different name than the other 3 techniques. Real check, not an
            assumption -- confirmed equal today, but a future Bronze change
            could break it silently without this.
    """
    first_columns = bronze_dfs[0].columns
    for technique, df in zip(SOURCE_TECHNIQUES, bronze_dfs):
        if df.columns != first_columns:
            raise ValueError(
                f"Bronze traffic schema mismatch: '{technique}' has columns {df.columns}, "
                f"expected {first_columns} (from '{SOURCE_TECHNIQUES[0]}')"
            )


def build_checked_traffic(bronze_dfs: List[DataFrame]) -> DataFrame:
    """Union, type-cast, dedup, and compute rejection_reason for the 4 Bronze traffic tables.

    Args:
        bronze_dfs: The 4 Bronze traffic_counts_* DataFrames (all columns
            STRING), in SOURCE_TECHNIQUES order (copyinto, dlt, autoloader,
            pyspark).

    Returns:
        DataFrame with strictly-typed columns (id/location/enter/exit: int,
        date: timestamp, source_technique: string) plus a rejection_reason
        column. silver_traffic and silver_traffic_rejected are both derived
        by filtering this same DataFrame (see quarantine.valid_rows/
        rejected_rows).

    Notes:
        dropDuplicates on the id+location+date natural key is defensive
        insurance against future drift (a rerun chunking job, a changed
        chunk split, a Bronze backfill), not a reaction to a found problem --
        Phase 1 profiling confirmed zero duplicate groups across the full
        24.7M-row union today, so this is expected to remove zero rows.
        Likewise, both quarantine rules are real invariants (a count is
        never negative; a date either parses or it doesn't), not tied to
        the currently-observed 0-35 enter/exit range -- expected to reject
        zero rows on current data, same as silver_streets_rejected.
    """
    assert_schemas_match(bronze_dfs)

    typed_dfs = []
    for df, technique in zip(bronze_dfs, SOURCE_TECHNIQUES):
        standardized = apply_header_standardization(df)
        typed = standardized.select(
            F.col("id").cast("int"),
            F.col("location").cast("int"),
            F.col("enter").cast("int"),
            F.col("exit").cast("int"),
            # try_to_timestamp returns NULL on an unparseable string instead
            # of raising under ANSI mode -- required for the unparseable_date
            # quarantine rule below to ever fire instead of crashing the
            # pipeline outright. date is ISO8601 (Spark's default parse
            # format), unambiguous unlike telegram.csv's date column.
            F.try_to_timestamp(F.col("date")).alias("date"),
            F.lit(technique).alias("source_technique"),
        )
        typed_dfs.append(typed)

    unioned = reduce(DataFrame.unionByName, typed_dfs)
    deduped = unioned.dropDuplicates(["id", "location", "date"])
    return deduped.withColumn("rejection_reason", build_rejection_reason_expr(TRAFFIC_QUARANTINE_RULES))
