"""agg_daily_location_traffic: daily traffic rollup per location -- adds the
date dimension gold_location_summary's all-time totals didn't have, so
traffic trends over time become queryable per intersection.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.audit import add_audit_columns


def build_agg_daily_location_traffic(
    fact_city_observations_df: DataFrame,
    dim_location_df: DataFrame,
    dim_date_df: DataFrame,
) -> DataFrame:
    """Aggregate fact_city_observations' traffic rows to one row per
    (location_key, date_key).

    Args:
        fact_city_observations_df: fact_city_observations (observation_type,
            location_key, date_key, enter, exit, ...).
        dim_location_df: Location dimension, for coordinates.
        dim_date_df: Date dimension, for calendar attributes.

    Returns:
        DataFrame with location_key, location, latitude, longitude,
        date_key, full_date, year, month, day_name, is_weekend, total_enter,
        total_exit, net_traffic, avg_enter, avg_exit, max_enter, max_exit,
        observation_count, plus the 4 standard audit columns.

    Notes:
        Filtered to observation_type == 'traffic' first, then left-joined to
        dim_location -- same precedent gold_location_summary already set.
        Traffic rows for location=7 (quarantined at Silver for bad
        coordinates, excluded from dim_location) carry a NULL location_key
        and surface here as their own NULL-keyed group per date, rather than
        being silently dropped -- see docs/gold_data_model.md's "Known,
        expected orphan" section.
    """
    traffic_facts = fact_city_observations_df.filter(F.col("observation_type") == "traffic")

    daily_agg = traffic_facts.groupBy("location_key", "date_key").agg(
        F.sum("enter").alias("total_enter"),
        F.sum("exit").alias("total_exit"),
        F.avg("enter").alias("avg_enter"),
        F.avg("exit").alias("avg_exit"),
        F.max("enter").alias("max_enter"),
        F.max("exit").alias("max_exit"),
        F.count(F.lit(1)).alias("observation_count"),
    )

    with_net = daily_agg.withColumn("net_traffic", F.col("total_enter") - F.col("total_exit"))

    with_location = with_net.join(
        dim_location_df.select("location_key", "location", "latitude", "longitude"),
        "location_key",
        "left",
    )

    with_date = with_location.join(
        dim_date_df.select("date_key", "full_date", "year", "month", "day_name", "is_weekend"),
        "date_key",
        "left",
    )

    result = with_date.select(
        "location_key",
        "location",
        "latitude",
        "longitude",
        "date_key",
        "full_date",
        "year",
        "month",
        "day_name",
        "is_weekend",
        "total_enter",
        "total_exit",
        "net_traffic",
        "avg_enter",
        "avg_exit",
        "max_enter",
        "max_exit",
        "observation_count",
    )

    return add_audit_columns(result, source_format="generated", source_file="agg_daily_location_traffic_aggregation")
