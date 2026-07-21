"""gold_daily_summary: one row per date, rolling up fact_city_observations'
three observation_type branches into the daily traffic/environment/telegram
numbers the brief actually asks about (busiest days, pollution/noise trends,
citizen report volume). A single GROUP BY over one table -- the unified fact
design means no cross-table join is needed to get all three domains onto one
row per day.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.audit import add_audit_columns


def build_gold_daily_summary(fact_city_observations_df: DataFrame) -> DataFrame:
    """Aggregate fact_city_observations to one row per date_key.

    Args:
        fact_city_observations_df: fact_city_observations (observation_type,
            date_key, enter, exit, noise, pollution, light, raining,
            message_count, ...).

    Returns:
        DataFrame with date_key, total_vehicles_entered,
        total_vehicles_exited, net_traffic_flow, avg_noise, avg_pollution,
        avg_light, avg_raining, telegram_message_count, total_observations,
        plus the 4 standard audit columns.

    Notes:
        Each measure is computed with a conditional SUM/AVG scoped to its
        own observation_type via F.when(...) -- traffic measures only
        average over 'traffic' rows, environmental measures only over
        'environmental' rows, etc. -- rather than filtering into 3 separate
        DataFrames and joining them back together, since a single GROUP BY
        already produces exactly one row per date_key with no join needed.

        All count-ish measures (entered/exited/net/telegram_message_count/
        total_observations) are BIGINT, matching what F.sum()/F.count()
        naturally return -- not cast down to INT, to avoid the
        LongType-vs-declared-IntegerType mismatch that broke dim_technique
        earlier this build (DELTA_MERGE_INCOMPATIBLE_DATATYPE).
    """
    obs = fact_city_observations_df

    grouped = obs.groupBy("date_key").agg(
        F.sum(F.when(obs.observation_type == "traffic", obs.enter)).alias("total_vehicles_entered"),
        F.sum(F.when(obs.observation_type == "traffic", obs.exit)).alias("total_vehicles_exited"),
        F.avg(F.when(obs.observation_type == "environmental", obs.noise)).alias("avg_noise"),
        F.avg(F.when(obs.observation_type == "environmental", obs.pollution)).alias("avg_pollution"),
        F.avg(F.when(obs.observation_type == "environmental", obs.light)).alias("avg_light"),
        F.avg(F.when(obs.observation_type == "environmental", obs.raining)).alias("avg_raining"),
        F.sum(F.when(obs.observation_type == "telegram", obs.message_count)).alias("telegram_message_count"),
        F.count(F.lit(1)).alias("total_observations"),
    )

    with_net_flow = grouped.withColumn(
        "net_traffic_flow", F.col("total_vehicles_entered") - F.col("total_vehicles_exited")
    )

    return add_audit_columns(with_net_flow, source_format="generated", source_file="gold_daily_summary_aggregation")
