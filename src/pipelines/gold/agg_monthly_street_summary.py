"""agg_monthly_street_summary: monthly environmental rollup per street --
long-term/seasonal trend view, the grain the brief's "monthly trend"
deliverable (Day 6.B) asks for.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.audit import add_audit_columns


def build_agg_monthly_street_summary(
    fact_city_observations_df: DataFrame,
    dim_street_df: DataFrame,
    dim_date_df: DataFrame,
) -> DataFrame:
    """Aggregate fact_city_observations' environmental rows to one row per
    (street_key, year, month).

    Args:
        fact_city_observations_df: fact_city_observations (observation_type,
            street_key, date_key, noise, pollution, light, raining, ...).
        dim_street_df: Street dimension, for street name/id/danger rating.
        dim_date_df: Date dimension, for year/month.

    Returns:
        DataFrame with street_key, street_id, street, dangerous, year,
        month, avg/max noise, avg/max pollution, avg/max light,
        days_with_rain, observation_count, observation_days, plus the 4
        standard audit columns.

    Notes:
        days_with_rain counts distinct date_keys with at least one
        raining > 0 reading that month, via countDistinct(F.when(...)) --
        NOT F.sum(F.when(raining > 0, 1)), which counts individual sensor
        readings, not days. At street+month grain there can be thousands of
        readings per day, so a raw reading count would wildly overstate
        "days" (e.g. one rainy day with 500 readings would read as "500 days
        of rain"). `raining`'s -1 "not raining" sentinel already falls
        through the `> 0` check into the FALSE branch correctly, so no
        separate sentinel handling is needed here (unlike
        agg_daily_street_conditions' rain_intensity_sum, which sums the
        actual values and would be corrupted by -1 without an explicit
        filter).
    """
    env_facts = fact_city_observations_df.filter(F.col("observation_type") == "environmental")

    with_date = env_facts.join(
        dim_date_df.select("date_key", "year", "month"),
        "date_key",
        "inner",
    )

    rainy_date_key = F.when(F.col("raining") > 0, F.col("date_key"))

    monthly_agg = with_date.groupBy("street_key", "year", "month").agg(
        F.avg("noise").alias("avg_noise"),
        F.avg("pollution").alias("avg_pollution"),
        F.avg("light").alias("avg_light"),
        F.max("noise").alias("max_noise"),
        F.max("pollution").alias("max_pollution"),
        F.max("light").alias("max_light"),
        F.countDistinct(rainy_date_key).alias("days_with_rain"),
        F.count(F.lit(1)).alias("observation_count"),
        F.countDistinct("date_key").alias("observation_days"),
    )

    with_street = monthly_agg.join(
        dim_street_df.filter(F.col("is_current")).select("street_key", "street_id", "street", "dangerous"),
        "street_key",
        "left",
    )

    result = with_street.select(
        "street_key",
        "street_id",
        "street",
        "dangerous",
        "year",
        "month",
        "avg_noise",
        "avg_pollution",
        "avg_light",
        "max_noise",
        "max_pollution",
        "max_light",
        "days_with_rain",
        "observation_count",
        "observation_days",
    )

    return add_audit_columns(result, source_format="generated", source_file="agg_monthly_street_summary_aggregation")
