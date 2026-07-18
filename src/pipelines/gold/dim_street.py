"""Dim_Street: SCD2 street dimension, built in two hops.

1. stg_dim_street_scd2 (streaming table, internal CDC plumbing -- not a
   documented "dimension" in the Gold data model, same framing as a Bronze
   table feeding Silver): AUTO CDC FROM SNAPSHOT writes directly into this
   table. No plain function wraps that call -- it's wired straight into the
   DLT notebook, same convention as every other AUTO CDC/expectation call
   in this project being genuinely declarative, not something to hide
   behind a testable wrapper.
2. Dim_Street (materialized view, the real public-facing dimension): reads
   stg_dim_street_scd2's full history (every SCD2 version, not just
   current -- Fact_Street_Conditions needs full history to resolve the
   correct version per fact date) and adds the surrogate key + is_current.

The only Gold table with true SCD2 -- Dim_Location and Dim_Date are static
(Phase 1 decision, since there's no shared key between them and no history
to track).
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Columns AUTO CDC FROM SNAPSHOT tracks for history -- a new SCD2 version is
# created whenever any of these change. latitude/longitude are tracked too
# for completeness even though they're not expected to ever actually change.
TRACKED_COLUMNS = ["dangerous", "street", "long", "latitude", "longitude"]


def build_dim_street(stg_dim_street_scd2_df: DataFrame) -> DataFrame:
    """Build the final Dim_Street materialized view from the raw AUTO CDC staging table.

    Args:
        stg_dim_street_scd2_df: Full history from stg_dim_street_scd2 (a
            plain batch read, not spark.readStream) -- every SCD2 version,
            not just the current one. Carries silver_streets' own audit
            columns through, since they aren't part of TRACKED_COLUMNS and
            AUTO CDC FROM SNAPSHOT passes untracked source columns through
            to its target unchanged.

    Returns:
        DataFrame with street_key (surrogate key), street_id, street, long,
        latitude, longitude, dangerous, __START_AT, __END_AT, is_current,
        plus the 4 audit columns carried through from silver_streets
        unchanged.

    Notes:
        street_key uses row_number() OVER (ORDER BY street_id, __START_AT),
        same generation pattern as Dim_Location -- this is the second
        surrogate-key approach tried for Dim_Street. The first (a single
        streaming table computing street_key directly on the AUTO CDC
        output) hit two real, confirmed failures: monotonically_increasing_id()
        overflowed IntegerType, and row_number() is flatly unsupported on a
        streaming DataFrame. The two-hop split fixes this at the root:
        Dim_Street is now a MATERIALIZED VIEW reading stg_dim_street_scd2
        via a plain batch read, and row_number()'s "unsupported on
        streaming DataFrames" restriction applies to spark.readStream
        queries specifically -- a batch read of a streaming table's stored
        data is just an ordinary DataFrame, so the window function works
        normally here.

        is_current is derived from __END_AT IS NULL -- Databricks' own
        convention for "this is the currently active SCD2 version."

        load_dt/source_format/source_file/run_id are selected straight
        through from stg_dim_street_scd2_df, not regenerated via
        add_audit_columns() -- see dim_location.py's build_dim_location for
        why. Each SCD2 version carries the audit columns from whichever
        silver_streets snapshot produced that version.
    """
    return stg_dim_street_scd2_df.select(
        F.row_number().over(Window.orderBy("street_id", "__START_AT")).cast("int").alias("street_key"),
        F.col("street_id"),
        F.col("street"),
        F.col("long"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("dangerous"),
        F.col("__START_AT"),
        F.col("__END_AT"),
        F.col("__END_AT").isNull().alias("is_current"),
        F.col("load_dt"),
        F.col("source_format"),
        F.col("source_file"),
        F.col("run_id"),
    )
