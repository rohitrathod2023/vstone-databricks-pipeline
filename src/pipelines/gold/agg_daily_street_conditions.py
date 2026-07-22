"""agg_daily_street_conditions: daily environmental rollup per street --
noise/pollution/light/rain trends broken out by street, complementing
gold_dlt_tables' fact_city_observations at a finer grain than a city-wide
daily average would give (see docs/gold_data_model.md's Business aggregates
section for why this and the other 3 agg_* tables exist as separate tables
rather than one, and how each differs from what came before).
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.audit import add_audit_columns


def build_agg_daily_street_conditions(
    fact_city_observations_df: DataFrame,
    dim_street_df: DataFrame,
    dim_date_df: DataFrame,
) -> DataFrame:
    """Aggregate fact_city_observations' environmental rows to one row per
    (street_key, date_key).

    Args:
        fact_city_observations_df: fact_city_observations (street_key,
            date_key, noise, pollution, light, raining, ...) -- ALTERNATIVE
            schema, no observation_type column (see
            docs/fact_table_without_discriminator_alternative.md).
        dim_street_df: Street dimension, for street name/id lookup.
        dim_date_df: Date dimension, for calendar attributes.

    Returns:
        DataFrame with street_key, street_id, street, date_key, full_date,
        year, month, day_name, is_weekend, avg/max/min noise, avg/max/min
        pollution, avg/max/min light, rain_intensity_sum, observation_count,
        plus the 4 standard audit columns.

    Notes:
        Filtered to noise.isNotNull() to identify environmental rows --
        there's no observation_type discriminator on this branch. Confirmed
        safe against real data: a live query against silver_environment
        found 0 nulls across noise/pollution/light/raining in all 87.8M
        rows, so any one of the 4 measures reliably identifies an
        environmental row today. This is an explicit trade-off versus the
        dev-branch design (an explicit observation_type column), not a free
        win -- see the alternative-design doc for the full comparison.

        rain_intensity_sum sums `raining` only where it's >= 0, replacing
        negative values with 0 first. `raining`'s source range is -1 to
        99.99, where -1 is a sentinel meaning "not raining" (confirmed:
        ~49% of raw values), not a real reading -- summing the column
        directly would let every "not raining" row silently subtract from
        the total. Named rain_intensity_sum, not hours_raining: `raining` is
        itself a percentage-style reading (sum of cars / street length), not
        a duration, so a sum across readings isn't literally "hours."

        Joins dim_street filtered to is_current == True only (not a
        point-in-time SCD2 resolution per date) -- same simplification
        fact_city_observations.py's own environmental branch already makes
        when resolving street_key.
    """
    env_facts = fact_city_observations_df.filter(F.col("noise").isNotNull())

    rain_when_valid = F.when(F.col("raining") >= 0, F.col("raining")).otherwise(F.lit(0.0))

    daily_agg = env_facts.groupBy("street_key", "date_key").agg(
        F.avg("noise").alias("avg_noise"),
        F.max("noise").alias("max_noise"),
        F.min("noise").alias("min_noise"),
        F.avg("pollution").alias("avg_pollution"),
        F.max("pollution").alias("max_pollution"),
        F.min("pollution").alias("min_pollution"),
        F.avg("light").alias("avg_light"),
        F.max("light").alias("max_light"),
        F.min("light").alias("min_light"),
        F.sum(rain_when_valid).alias("rain_intensity_sum"),
        F.count(F.lit(1)).alias("observation_count"),
    )

    with_street = daily_agg.join(
        dim_street_df.filter(F.col("is_current")).select("street_key", "street_id", "street"),
        "street_key",
        "left",
    )

    with_date = with_street.join(
        dim_date_df.select("date_key", "full_date", "year", "month", "day_name", "is_weekend"),
        "date_key",
        "left",
    )

    result = with_date.select(
        "street_key",
        "street_id",
        "street",
        "date_key",
        "full_date",
        "year",
        "month",
        "day_name",
        "is_weekend",
        "avg_noise",
        "max_noise",
        "min_noise",
        "avg_pollution",
        "max_pollution",
        "min_pollution",
        "avg_light",
        "max_light",
        "min_light",
        "rain_intensity_sum",
        "observation_count",
    )

    return add_audit_columns(result, source_format="generated", source_file="agg_daily_street_conditions_aggregation")
