"""Build fact_daily_summary: daily aggregated metrics across traffic,
environment, telegram, and street safety.

Uses INNER JOIN strategy on dates - only includes dates present in ALL three
silver sources (traffic, environment, telegram) to ensure NOT NULL constraints
are honest (real data, not imputed zeros).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_fact_daily_summary(
    spark: SparkSession,
    traffic_df: DataFrame,
    environment_df: DataFrame,
    telegram_df: DataFrame,
    dim_street_df: DataFrame,
) -> DataFrame:
    """Build fact_daily_summary using INNER JOIN across all three sources.
    
    Args:
        spark: Spark session for dangerous_streets_count subquery
        traffic_df: silver_traffic table
        environment_df: silver_environment table
        telegram_df: silver_telegram table
        dim_street_df: dim_street table (for dangerous_streets_count)
    
    Returns:
        DataFrame with daily aggregated metrics (283 rows for INNER JOIN dates)
    """
    # Step 1: Find common dates across all three sources (INNER JOIN logic)
    traffic_dates = traffic_df.select(F.to_date("date").alias("common_date")).distinct()
    env_dates = environment_df.select(F.to_date("date").alias("common_date")).distinct()
    telegram_dates = telegram_df.select(F.to_date("event_timestamp").alias("common_date")).distinct()
    
    common_dates = traffic_dates.intersect(env_dates).intersect(telegram_dates)
    
    # Step 2: Aggregate traffic by date
    traffic_agg = (
        traffic_df
        .withColumn("date_val", F.to_date("date"))
        .groupBy("date_val")
        .agg(
            F.sum("enter").alias("total_entered"),
            F.sum("exit").alias("total_exited"),
        )
    )
    
    # Step 3: Aggregate environment by date
    environment_agg = (
        environment_df
        .withColumn("date_val", F.to_date("date"))
        .groupBy("date_val")
        .agg(
            F.avg("noise").alias("avg_noise"),
            F.avg("pollution").alias("avg_pollution"),
            F.avg("light").alias("avg_light"),
            F.avg("raining").alias("avg_raining"),
            F.max("pollution").alias("max_pollution"),
        )
    )
    
    # Step 4: Aggregate telegram by date
    telegram_agg = (
        telegram_df
        .withColumn("date_val", F.to_date("event_timestamp"))
        .groupBy("date_val")
        .agg(F.count("*").alias("message_count"))
    )
    
    # Step 5: Compute dangerous_streets_count (constant across all dates)
    dangerous_count = (
        dim_street_df
        .filter((F.col("is_current") == True) & (F.col("dangerous") >= 0.5))
        .count()
    )
    
    # Step 6: Join all aggregates on common dates
    result = (
        common_dates
        .join(traffic_agg, common_dates.common_date == traffic_agg.date_val, "inner")
        .join(environment_agg, common_dates.common_date == environment_agg.date_val, "inner")
        .join(telegram_agg, common_dates.common_date == telegram_agg.date_val, "inner")
        .select(
            # date_key as YYYYMMDD integer
            F.date_format(common_dates.common_date, "yyyyMMdd").cast("int").alias("date_key"),
            
            # Traffic measures (COALESCE not needed - INNER JOIN ensures data exists)
            F.col("total_entered").alias("total_vehicles_entered"),
            F.col("total_exited").alias("total_vehicles_exited"),
            (F.col("total_entered") - F.col("total_exited")).alias("net_traffic_flow"),
            
            # Environment measures
            F.col("avg_noise"),
            F.col("avg_pollution"),
            F.col("avg_light"),
            F.col("avg_raining"),
            F.col("max_pollution"),
            
            # Street safety (constant)
            F.lit(dangerous_count).cast("int").alias("dangerous_streets_count"),
            
            # Telegram
            F.col("message_count").alias("telegram_message_count"),
            
            # Audit metadata
            F.current_timestamp().alias("load_dt"),
            F.lit("silver_layer").alias("source_format"),
            F.lit("multi-source-aggregation").alias("source_file"),
            F.lit("dlt-pipeline-build").alias("run_id"),
        )
        .orderBy("date_key")
    )
    
    return result
