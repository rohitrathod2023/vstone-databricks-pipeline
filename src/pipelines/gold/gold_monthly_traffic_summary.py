"""gold_monthly_traffic_summary: one row per location per calendar month,
aggregated from Fact_Traffic_Counts. Addresses the brief's "top-10 busiest
intersections" and "monthly trend" examples -- busiest_rank_in_month makes
the top-10 filter trivial (WHERE busiest_rank_in_month <= 10).
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.audit import add_audit_columns


def build_gold_monthly_traffic_summary(fact_traffic_counts_df: DataFrame, dim_date_df: DataFrame) -> DataFrame:
    """Aggregate Fact_Traffic_Counts to one row per location per calendar month.

    Args:
        fact_traffic_counts_df: Fact_Traffic_Counts (location_key, date_key,
            enter, exit, ...).
        dim_date_df: Dim_Date (date_key, year, month, ...).

    Returns:
        DataFrame with location_key, year, month, total_enter, total_exit,
        total_traffic_volume, avg_daily_traffic_volume,
        busiest_rank_in_month, plus the 4 standard audit columns.

    Notes:
        avg_daily_traffic_volume divides by the count of DISTINCT dates
        actually present in that location+month group, not a hardcoded
        calendar-day count -- a month at the edge of the real data range
        may not have full coverage.

        busiest_rank_in_month is a dense rank of total_traffic_volume
        within each (year, month) partition, descending -- location=7's
        traffic rows (which never resolve a location_key, see
        Fact_Traffic_Counts) are excluded from this aggregate by the join
        below, same as any other unresolvable row.
    """
    joined = fact_traffic_counts_df.alias("f").join(
        dim_date_df.select("date_key", "year", "month").alias("d"),
        F.col("f.date_key") == F.col("d.date_key"),
        "inner",
    )
    grouped = joined.groupBy(F.col("f.location_key").alias("location_key"), "d.year", "d.month").agg(
        F.sum("f.enter").alias("total_enter"),
        F.sum("f.exit").alias("total_exit"),
        F.countDistinct("f.date_key").alias("_distinct_days"),
    )
    with_totals = (
        grouped.withColumn("total_traffic_volume", F.col("total_enter") + F.col("total_exit"))
        .withColumn("avg_daily_traffic_volume", F.col("total_traffic_volume") / F.col("_distinct_days"))
        .drop("_distinct_days")
    )

    month_window = Window.partitionBy("year", "month").orderBy(F.col("total_traffic_volume").desc())
    ranked = with_totals.withColumn("busiest_rank_in_month", F.rank().over(month_window))

    return add_audit_columns(ranked, source_format="generated", source_file="gold_monthly_traffic_summary_aggregation")
