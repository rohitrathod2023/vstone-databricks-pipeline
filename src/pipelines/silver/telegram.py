"""Silver telegram: cleaned, deduped telegram.csv with the separate
date+hour strings combined into one real event_timestamp. No fact-to-
dimension join happens here; telegram.csv has no key relationship to any
other table (per Phase 1 profiling).
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.silver.header_standardization import apply_header_standardization
from pipelines.silver.quarantine import build_rejection_reason_expr
from pipelines.silver.text_normalization import normalize_street_name_udf

TELEGRAM_QUARANTINE_RULES = {
    "unparseable_event_timestamp": (
        "event_timestamp IS NULL",
        "unparseable_event_timestamp: date/hour could not be combined into a valid timestamp",
    ),
}


def build_checked_telegram(bronze_df: DataFrame) -> DataFrame:
    """Normalize message text, combine date+hour into a timestamp, dedup, and compute rejection_reason.

    Args:
        bronze_df: Bronze telegram DataFrame (all columns STRING: message,
            date, hour).

    Returns:
        DataFrame with columns (message: string, event_timestamp: timestamp),
        the 4 audit columns carried through from Bronze unchanged, plus a
        rejection_reason column. silver_telegram and silver_telegram_rejected
        are both derived by filtering this same DataFrame (see
        quarantine.valid_rows/rejected_rows).

    Notes:
        message reuses the whitespace-normalization Pandas UDF originally
        built for streets_list.street (normalize_street_name_udf) -- real
        leading-whitespace values confirmed in Bronze (e.g. " The car does
        not respect..."), brought into scope for this phase per Rohit's
        July 16 decision.

        date (confirmed dd/MM/yyyy) and hour (confirmed HH:mm:ss) are
        combined via try_to_timestamp, not to_timestamp -- under Spark's
        default ANSI mode, to_timestamp raises a hard exception on
        unparseable input instead of returning NULL, which would crash the
        whole pipeline on a single bad row and defeat the
        unparseable_event_timestamp quarantine rule's purpose. Confirmed
        against real Bronze data: all 128,440 rows parse cleanly today (0
        NULLs), so this rule is expected to currently reject zero rows --
        insurance against a future bad row, not a reaction to a found
        defect.

        dropDuplicates on message+date+hour (the original Bronze columns,
        before they're combined/dropped) is the same insurance dedup
        pattern used everywhere else in Silver -- a real
        `GROUP BY message, date, hour HAVING COUNT(*) > 1` query against
        Bronze confirmed zero duplicate groups today.

        load_dt/source_format/source_file/run_id are selected straight
        through from bronze_df, not regenerated -- see locations.py's
        build_checked_locations for why.
    """
    deduped_bronze = bronze_df.dropDuplicates(["message", "date", "hour"])
    standardized = apply_header_standardization(deduped_bronze)
    typed = standardized.select(
        normalize_street_name_udf()(F.col("message")).alias("message"),
        F.try_to_timestamp(
            F.concat(F.col("date"), F.lit(" "), F.col("hour")), F.lit("dd/MM/yyyy HH:mm:ss")
        ).alias("event_timestamp"),
        F.col("load_dt"),
        F.col("source_format"),
        F.col("source_file"),
        F.col("run_id"),
    )
    return typed.withColumn("rejection_reason", build_rejection_reason_expr(TELEGRAM_QUARANTINE_RULES))
