"""Silver locations: cleaned, strictly-typed, quarantined node_locations --
traffic domain's dimension staging. No fact-to-dimension join happens here;
that's Gold's job.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.silver.header_standardization import apply_header_standardization
from pipelines.silver.quarantine import build_rejection_reason_expr

LOCATIONS_QUARANTINE_RULES = {
    "invalid_coordinates": (
        "latitude = 0 AND longitude = 0",
        "invalid_coordinates: latitude and longitude are both 0",
    ),
}


def build_checked_locations(bronze_df: DataFrame) -> DataFrame:
    """Type-cast, standardize headers, and compute rejection_reason for node_locations.

    Args:
        bronze_df: Bronze node_locations DataFrame (all columns STRING).

    Returns:
        DataFrame with strictly-typed columns (location: int, latitude/
        longitude: double), the 4 audit columns carried through from Bronze
        unchanged, plus a rejection_reason column. silver_locations and
        silver_locations_rejected are both derived by filtering this same
        DataFrame (see quarantine.valid_rows/rejected_rows).

    Notes:
        The location=7 bad-coordinate row (latitude=longitude=0) confirmed
        in Phase 1 profiling is the concrete row this quarantines -- not a
        hypothetical case.

        load_dt/source_format/source_file/run_id are selected straight
        through from bronze_df, not regenerated -- they already carry the
        original Bronze ingestion's lineage (which raw file, which run),
        and re-stamping here would silently overwrite that with Silver's
        own processing time.
    """
    standardized = apply_header_standardization(bronze_df)
    typed = standardized.select(
        F.col("location").cast("int"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.col("load_dt"),
        F.col("source_format"),
        F.col("source_file"),
        F.col("run_id"),
    )
    return typed.withColumn("rejection_reason", build_rejection_reason_expr(LOCATIONS_QUARANTINE_RULES))
