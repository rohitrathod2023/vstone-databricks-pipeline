"""Dim_Date: a generated calendar dimension, one row per day. No source file
-- the date range is derived from the real observed min/max across
silver_traffic/environment/telegram, not hardcoded.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from utils.audit import add_audit_columns


def compute_date_range(
    traffic_df: DataFrame, environment_df: DataFrame, telegram_df: DataFrame, buffer_days: int = 3
) -> tuple[str, str]:
    """Compute the real min/max date across every date-bearing Silver table, buffered.

    Args:
        traffic_df: silver_traffic DataFrame (has a `date` column).
        environment_df: silver_environment DataFrame (has a `date` column).
        telegram_df: silver_telegram DataFrame (has an `event_timestamp` column).
        buffer_days: Days to pad on each end of the observed range, to guard
            against edge rows just outside what's currently loaded.

    Returns:
        (min_date, max_date) as "yyyy-MM-dd" strings, ready for Dim_Date's
        generation range.

    Notes:
        Takes DataFrames rather than table names/a bare spark.sql query, so
        the caller (the DLT notebook) resolves the fully-qualified Silver
        table references -- a bare `FROM silver_traffic` wouldn't resolve
        correctly here, since the Gold pipeline's default catalog/schema
        context is Gold's, not Silver's. silver_traffic and
        silver_environment both name their timestamp column `date`;
        silver_telegram names it `event_timestamp` -- see each table's
        schema in config/silver_schemas.py. Real observed range at build
        time: 2023-06-02 to 2024-03-10, identical across all three tables.
    """
    combined = (
        traffic_df.select(F.col("date").cast("date").alias("d"))
        .unionByName(environment_df.select(F.col("date").cast("date").alias("d")))
        .unionByName(telegram_df.select(F.col("event_timestamp").cast("date").alias("d")))
    )
    bounds = combined.agg(F.min("d").alias("min_d"), F.max("d").alias("max_d")).collect()[0]
    row = traffic_df.sparkSession.createDataFrame([(bounds["min_d"], bounds["max_d"])], ["min_d", "max_d"])
    buffered = row.select(
        F.date_sub("min_d", buffer_days).alias("min_d"),
        F.date_add("max_d", buffer_days).alias("max_d"),
    ).collect()[0]
    return str(buffered["min_d"]), str(buffered["max_d"])


def build_dim_date(spark: SparkSession, min_date: str, max_date: str) -> DataFrame:
    """Generate one row per calendar day between min_date and max_date, inclusive.

    Args:
        spark: Active SparkSession.
        min_date: First date in the range, "yyyy-MM-dd".
        max_date: Last date in the range, "yyyy-MM-dd", inclusive.

    Returns:
        DataFrame with one row per day: date_key (YYYYMMDD surrogate key),
        full_date, year, month, month_name, day_of_month, day_of_week,
        day_name, quarter, is_weekend, plus the 4 standard audit columns.

    Notes:
        day_of_week uses Spark's dayofweek() convention: 1=Sunday through
        7=Saturday (not ISO-8601's 1=Monday) -- documented here so
        downstream consumers don't have to guess. is_weekend follows the
        same convention (Sunday=1 or Saturday=7).
    """
    bounds = spark.createDataFrame([(min_date, max_date)], ["min_date", "max_date"])
    dates = bounds.select(
        F.explode(
            F.sequence(F.to_date("min_date"), F.to_date("max_date"), F.expr("interval 1 day"))
        ).alias("full_date")
    )
    enriched = dates.select(
        F.date_format(F.col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),
        F.col("full_date"),
        F.year("full_date").alias("year"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.dayofweek("full_date").alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.quarter("full_date").alias("quarter"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
    )
    return add_audit_columns(enriched, source_format="generated", source_file="date_dimension_generator")
