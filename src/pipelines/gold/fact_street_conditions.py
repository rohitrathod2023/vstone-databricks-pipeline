"""Fact_Street_Conditions: silver_environment's accepted rows joined to
Dim_Street and Dim_Date. No aggregation -- one row per accepted
silver_environment row.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.audit import add_audit_columns


def build_fact_street_conditions(
    environment_df: DataFrame, dim_street_df: DataFrame, dim_date_df: DataFrame
) -> DataFrame:
    """Join silver_environment's accepted rows to Dim_Street and Dim_Date.

    Args:
        environment_df: silver_environment -- the ACCEPTED rows only (not
            silver_environment_rejected).
        dim_street_df: Dim_Street's full version history (street_id,
            street_key, __START_AT, __END_AT, ...) -- not filtered to
            is_current, since a fact from an earlier date must resolve to
            whichever version was active back then.
        dim_date_df: Dim_Date (full_date, date_key, ...).

    Returns:
        DataFrame with street_key, date_key, noise, pollution, light,
        raining, plus the 4 standard audit columns.

    Notes:
        street_key resolution is a RANGE join on Dim_Street's __START_AT/
        __END_AT, not a plain equi-join on street_id alone -- an equi-join
        would fan out once a street has more than one SCD2 version (every
        fact row would match every version of that street, multiplying row
        count). The range condition (__START_AT <= fact date < __END_AT,
        or __END_AT IS NULL for the current version) picks exactly the one
        version that was active as of the fact's own date.

        Backfill clamp: AUTO CDC FROM SNAPSHOT always stamps __START_AT
        with wall-clock processing time, not any business-effective date in
        the source data -- confirmed live that this crashed street_key
        resolution to 100% NULL on the real dataset, since Dim_Street's
        __START_AT is "today" while every silver_environment fact date is
        from 2023-2024, years earlier. Every fact predates every version's
        __START_AT, so the plain range condition above never matches
        anything. The fix: a fact older than a street's EARLIEST known
        version clamps to that earliest version instead of resolving to
        NULL -- standard Kimball backfill convention, treating the earliest
        known version as the best available information for anything before
        SCD2 tracking began. This can't fan out: only one Dim_Street row per
        street_id is ever flagged as the earliest version, and it's
        mutually exclusive with the normal range condition (a row can't
        simultaneously be before its own __START_AT and within
        [__START_AT, __END_AT)). On this build, Dim_Street has exactly one
        version per street, so every real fact takes the clamp path; once a
        second version exists, only genuinely-pre-tracking facts would.

        LEFT joins throughout, same reasoning as Fact_Traffic_Counts: an
        inner join would silently drop any row that fails to resolve,
        making a row-count mismatch undetectable.
    """
    earliest_version_window = Window.partitionBy("street_id")
    dim_street_with_earliest = dim_street_df.withColumn(
        "_earliest_start_at", F.min("__START_AT").over(earliest_version_window)
    ).withColumn("_is_earliest_version", F.col("__START_AT") == F.col("_earliest_start_at"))

    joined = (
        environment_df.alias("e")
        .join(
            dim_street_with_earliest.alias("s"),
            (F.col("e.street_id") == F.col("s.street_id"))
            & (
                (
                    (F.col("e.date") >= F.col("s.__START_AT"))
                    & (F.col("s.__END_AT").isNull() | (F.col("e.date") < F.col("s.__END_AT")))
                )
                | ((F.col("e.date") < F.col("s._earliest_start_at")) & F.col("s._is_earliest_version"))
            ),
            "left",
        )
        .join(
            dim_date_df.select("full_date", "date_key").alias("d"),
            F.to_date(F.col("e.date")) == F.col("d.full_date"),
            "left",
        )
        .select(
            F.col("s.street_key").alias("street_key"),
            F.col("d.date_key").alias("date_key"),
            F.col("e.noise").alias("noise"),
            F.col("e.pollution").alias("pollution"),
            F.col("e.light").alias("light"),
            F.col("e.raining").alias("raining"),
        )
    )
    return add_audit_columns(joined, source_format="delta", source_file="silver_environment")
