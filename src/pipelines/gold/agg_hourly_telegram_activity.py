"""agg_hourly_telegram_activity: hourly telegram message-volume rollup --
answers "when do most incident reports come in," now meaningful now that
fact_city_observations.time_key is a real per-row HHMMSS extraction instead
of the hardcoded 0 it used to be for traffic/environmental rows (telegram's
time_key was always correct).
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.audit import add_audit_columns


def build_agg_hourly_telegram_activity(
    fact_city_observations_df: DataFrame,
    dim_date_df: DataFrame,
    dim_time_df: DataFrame,
) -> DataFrame:
    """Aggregate fact_city_observations' telegram rows to one row per
    (date_key, hour).

    Args:
        fact_city_observations_df: fact_city_observations (observation_type,
            date_key, time_key, message_count, ...).
        dim_date_df: Date dimension, for calendar attributes.
        dim_time_df: Time dimension, for time_key -> hour lookup.

    Returns:
        DataFrame with date_key, full_date, year, month, day_name,
        is_weekend, hour, total_messages, observation_count, plus the 4
        standard audit columns.

    Notes:
        No avg/max/min message_length here -- message_length was dropped
        from fact_city_observations entirely (no aggregation of message
        length has business value; recompute from silver_telegram.message
        with F.length() directly if ever needed). An earlier draft of this
        table referenced it and would fail with UNRESOLVED_COLUMN against
        the current schema.

        Filtered to observation_type == 'telegram' first (matches the other
        3 agg_* tables' convention), not message_count.isNotNull().

        Grain is (date_key, hour), not (date_key, time_key) -- `hour` isn't
        dim_time's PRIMARY KEY, so this table declares no FOREIGN KEY to
        dim_time (see table_schemas.py's AGG_HOURLY_TELEGRAM_ACTIVITY_SCHEMA
        for why).
    """
    telegram_facts = fact_city_observations_df.filter(F.col("observation_type") == "telegram")

    with_hour = telegram_facts.join(
        dim_time_df.select("time_key", "hour"),
        "time_key",
        "inner",
    )

    hourly_agg = with_hour.groupBy("date_key", "hour").agg(
        F.sum("message_count").alias("total_messages"),
        F.count(F.lit(1)).alias("observation_count"),
    )

    with_date = hourly_agg.join(
        dim_date_df.select("date_key", "full_date", "year", "month", "day_name", "is_weekend"),
        "date_key",
        "left",
    )

    result = with_date.select(
        "date_key",
        "full_date",
        "year",
        "month",
        "day_name",
        "is_weekend",
        "hour",
        "total_messages",
        "observation_count",
    )

    return add_audit_columns(result, source_format="generated", source_file="agg_hourly_telegram_activity_aggregation")
