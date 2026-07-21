"""gold_location_summary: one row per location, rolling up
fact_city_observations' traffic rows to answer the brief's "busiest
intersections" question -- total entered/exited/volume per location, over
the whole dataset. Simple GROUP BY, no ranking/window functions.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.audit import add_audit_columns


def build_gold_location_summary(fact_city_observations_df: DataFrame) -> DataFrame:
    """Aggregate fact_city_observations' traffic rows to one row per location_key.

    Args:
        fact_city_observations_df: fact_city_observations (observation_type,
            location_key, enter, exit, ...).

    Returns:
        DataFrame with location_key, total_vehicles_entered,
        total_vehicles_exited, total_traffic_volume, total_traffic_readings,
        plus the 4 standard audit columns.

    Notes:
        Filtered to observation_type == 'traffic' first -- environmental and
        telegram rows always carry a NULL location_key and would otherwise
        collapse into a meaningless extra group alongside location=7's real
        orphaned traffic readings (see docs/gold_data_model.md for why
        location=7 legitimately has no location_key).
    """
    traffic_only = fact_city_observations_df.filter(F.col("observation_type") == "traffic")

    grouped = traffic_only.groupBy("location_key").agg(
        F.sum("enter").alias("total_vehicles_entered"),
        F.sum("exit").alias("total_vehicles_exited"),
        F.count(F.lit(1)).alias("total_traffic_readings"),
    )

    with_volume = grouped.withColumn(
        "total_traffic_volume", F.col("total_vehicles_entered") + F.col("total_vehicles_exited")
    )

    return add_audit_columns(with_volume, source_format="generated", source_file="gold_location_summary_aggregation")
