"""Silver environment: cleaned, strictly-typed, deduped, quarantined
street_conditions (streets.csv). No fact-to-dimension join happens here;
that's Gold's job (against Dim_Street).
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.silver.header_standardization import apply_header_standardization
from pipelines.silver.quarantine import build_rejection_reason_expr

ENVIRONMENT_QUARANTINE_RULES = {
    "raining_out_of_range": (
        "raining > 100",
        "raining_out_of_range: raining is above 100",
    ),
    "negative_noise": (
        "noise < 0",
        "negative_noise: noise is negative",
    ),
    "negative_pollution": (
        "pollution < 0",
        "negative_pollution: pollution is negative",
    ),
    "negative_light": (
        "light < 0",
        "negative_light: light is negative",
    ),
}


def build_checked_environment(bronze_df: DataFrame) -> DataFrame:
    """Type-cast, dedup, standardize headers, and compute rejection_reason for street_conditions.

    Args:
        bronze_df: Bronze street_conditions DataFrame (all columns STRING).

    Returns:
        DataFrame with strictly-typed columns (street_id: int, date:
        timestamp, noise/pollution/light/raining: double) plus a
        rejection_reason column. silver_environment and
        silver_environment_rejected are both derived by filtering this same
        DataFrame (see quarantine.valid_rows/rejected_rows).

    Notes:
        dropDuplicates on the street_id+date natural key is defensive
        insurance against future drift, not a reaction to a found problem --
        a real `GROUP BY street_id, date HAVING COUNT(*) > 1` query against
        Bronze confirmed zero duplicate groups today, so this is expected
        to remove zero rows.

        raining_out_of_range only checks the upper bound (raining > 100),
        not raining < 0 -- an earlier version of this rule rejected negative
        values too and was built and run for real, which revealed the
        original "0-100 percentage" assumption was wrong: 98.6% of real rows
        have raining between -1 and 1 (avg 0.49), which is the normal
        "not currently raining" baseline and legitimately includes small
        negative noise, not a defect. That version rejected 49.3% of the
        table. Only raining > 100 (0.05% of rows, ~44,004) is a genuine
        anomaly. The other three rules (negative_noise/pollution/light) are
        insurance: a decibel/pollution/light reading can never legitimately
        be negative, even though no real violations were found. Deliberately
        no upper bound on noise/pollution/light -- unlike raining's real 100
        ceiling, they have no fixed, universally-true maximum, and
        hardcoding "today's observed max" as a rejection rule would repeat
        the exact mistake Phase 4 deliberately avoided for silver_traffic's
        location range.
    """
    standardized = apply_header_standardization(bronze_df)
    typed = standardized.select(
        F.col("street_id").cast("int"),
        F.try_to_timestamp(F.col("date")).alias("date"),
        F.col("noise").cast("double"),
        F.col("pollution").cast("double"),
        F.col("light").cast("double"),
        F.col("raining").cast("double"),
    )
    deduped = typed.dropDuplicates(["street_id", "date"])
    return deduped.withColumn("rejection_reason", build_rejection_reason_expr(ENVIRONMENT_QUARANTINE_RULES))
