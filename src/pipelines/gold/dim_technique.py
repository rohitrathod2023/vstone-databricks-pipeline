"""dim_technique - Ingestion technique/method dimension.

A proper lookup dimension for ingestion technique, with an integer surrogate
key -- fact_city_observations resolves technique_key against this instead of
carrying a raw source_technique string.

Small static dimension (4 rows): autoloader, copyinto, dlt, pyspark -- the
real techniques used anywhere in this pipeline (see
src/pipelines/silver/traffic.py's SOURCE_TECHNIQUES and src/config/sources.yml's
technique: fields). No "telegram_csv" technique exists in the real pipeline --
bronze_telegram uses copy_into, same as bronze_streets/node_locations/streets_list.
"""

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_dim_technique(spark: SparkSession) -> DataFrame:
    """Build dim_technique dimension table with all ingestion techniques.

    Returns static 4-row dimension with technique metadata.
    No source data dependencies - hardcoded reference data.

    Args:
        spark: SparkSession

    Returns:
        DataFrame with columns:
            - technique_key (INT): Surrogate key
            - technique_name (STRING): Technical name -- matches
              silver_traffic.source_technique's real values exactly
              (SOURCE_TECHNIQUES in pipelines/silver/traffic.py), so fact
              builders can resolve technique_key via a plain equi-join.
            - technique_type (STRING): 'streaming' or 'batch'
            - description (STRING): Human-readable description
            - supports_streaming (BOOLEAN): Whether technique supports streaming
            - created_date (DATE): When technique was added
    """
    current_date = date.today()

    techniques = [
        (1, "autoloader", "streaming", "Auto Loader for incremental file ingestion", True, current_date),
        (2, "copyinto", "batch", "COPY INTO for batch file loading", False, current_date),
        (3, "dlt", "streaming", "Delta Live Tables streaming pipeline", True, current_date),
        (4, "pyspark", "batch", "PySpark batch processing", False, current_date),
    ]

    # spark.createDataFrame() infers technique_key as LongType from the
    # Python int literals above, but DIM_TECHNIQUE_SCHEMA declares it INT --
    # confirmed live (DELTA_MERGE_INCOMPATIBLE_DATATYPE) that Delta rejects
    # writing a LongType column into a declared IntegerType one. Explicit
    # cast, same as every other surrogate key in this project.
    return spark.createDataFrame(
        techniques,
        ["technique_key", "technique_name", "technique_type", "description", "supports_streaming", "created_date"],
    ).withColumn("technique_key", F.col("technique_key").cast("int"))
