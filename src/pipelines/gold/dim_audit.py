"""dim_audit - Audit metadata dimension for data lineage.

Consolidates the audit columns (load_dt, source_format, source_file, run_id)
into a proper dimension table with an integer surrogate key, so
fact_city_observations carries a single audit_key FK instead of 4 raw columns.

Grows by a handful of rows each time a Bronze source is reloaded with a new
load_dt/run_id -- since load_dt/source_format/source_file/run_id are carried
through from Bronze unchanged (not regenerated per layer, see
docs/gold_data_model.md), today's real row count is small (one distinct
combination per Bronze file/run currently loaded into Silver), not a fixed
number.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_dim_audit(traffic_df: DataFrame, environment_df: DataFrame, telegram_df: DataFrame) -> DataFrame:
    """
    Build dim_audit dimension by extracting unique audit combinations from
    all silver layer tables.

    Generates surrogate keys using DENSE_RANK to ensure consistent mapping
    across all source tables.

    Args:
        traffic_df: Silver traffic table
        environment_df: Silver environment table
        telegram_df: Silver telegram table

    Returns:
        DataFrame with columns:
            - audit_key (INT): Surrogate key
            - load_dt (TIMESTAMP): When data was loaded
            - source_format (STRING): File format (csv, json, etc.)
            - source_file (STRING): Source file name
            - run_id (STRING): Pipeline run UUID
            - created_timestamp (TIMESTAMP): When audit record was created
    """
    # Extract audit columns from traffic
    traffic_audit = traffic_df.select("load_dt", "source_format", "source_file", "run_id").distinct()

    # Extract audit columns from environment
    environment_audit = environment_df.select("load_dt", "source_format", "source_file", "run_id").distinct()

    # Extract audit columns from telegram
    telegram_audit = telegram_df.select("load_dt", "source_format", "source_file", "run_id").distinct()

    # Union all audit records and get unique combinations
    all_audit = traffic_audit.unionByName(environment_audit).unionByName(telegram_audit).distinct()

    # Generate audit_key using DENSE_RANK for consistent ordering
    window_spec = Window.orderBy("load_dt", "source_format", "source_file", "run_id")

    dim_audit = all_audit.withColumn("audit_key", F.dense_rank().over(window_spec)).withColumn(
        "created_timestamp", F.current_timestamp()
    )

    return dim_audit.select(
        "audit_key",
        "load_dt",
        "source_format",
        "source_file",
        "run_id",
        "created_timestamp",
    )
