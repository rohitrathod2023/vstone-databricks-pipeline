"""Silver streets: cleaned, strictly-typed, quarantined streets_list --
carries the `dangerous` score Gold's Dim_Street depends on. No fact-to-
dimension join happens here; that's Gold's job.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.silver.header_standardization import apply_header_standardization
from pipelines.silver.quarantine import build_rejection_reason_expr
from pipelines.silver.text_normalization import normalize_street_name_udf

STREETS_QUARANTINE_RULES = {
    "invalid_coordinates": (
        "latitude = 0 AND longitude = 0",
        "invalid_coordinates: latitude and longitude are both 0",
    ),
    "dangerous_out_of_range": (
        "dangerous < 0 OR dangerous > 1",
        "dangerous_out_of_range: dangerous score is outside [0, 1]",
    ),
}


def build_checked_streets(bronze_df: DataFrame) -> DataFrame:
    """Type-cast, standardize headers/values, and compute rejection_reason for streets_list.

    Args:
        bronze_df: Bronze streets_list DataFrame (all columns STRING).

    Returns:
        DataFrame with strictly-typed columns (street: string, long/
        street_id: int, latitude/longitude/dangerous: double), the 4 audit
        columns carried through from Bronze unchanged, plus a
        rejection_reason column. silver_streets and silver_streets_rejected
        are both derived by filtering this same DataFrame.

    Notes:
        Both quarantine rules are defensive insurance, not reactions to a
        found problem -- Phase 1 profiling confirmed all 36 rows are
        currently clean on both. `long` is street length in meters, not to
        be confused with `longitude`.

        load_dt/source_format/source_file/run_id are selected straight
        through from bronze_df, not regenerated -- see locations.py's
        build_checked_locations for why.
    """
    standardized = apply_header_standardization(bronze_df)
    typed = standardized.select(
        normalize_street_name_udf()(F.col("street")).alias("street"),
        F.col("long").cast("int"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.col("dangerous").cast("double"),
        F.col("street_id").cast("int"),
        F.col("load_dt"),
        F.col("source_format"),
        F.col("source_file"),
        F.col("run_id"),
    )
    return typed.withColumn("rejection_reason", build_rejection_reason_expr(STREETS_QUARANTINE_RULES))
