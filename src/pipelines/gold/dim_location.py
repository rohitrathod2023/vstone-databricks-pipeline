"""Dim_Location: static location dimension built from silver_locations (the
location=7 bad-coordinate row was already rejected at Silver -- this table
never re-filters it). No SCD2 -- Phase 1 confirmed Dim_Location's attributes
are effectively immutable.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_dim_location(silver_locations_df: DataFrame) -> DataFrame:
    """Add a stable surrogate key to silver_locations, carrying its audit columns through.

    Args:
        silver_locations_df: The accepted silver_locations DataFrame (13
            good rows -- silver_locations_rejected's 1 row is never
            re-filtered here since Silver already excluded it).

    Returns:
        DataFrame with location_key (surrogate key), location (natural
        key), latitude, longitude, plus the 4 audit columns carried through
        from silver_locations unchanged.

    Notes:
        location_key is row_number() ordered by the natural key (location),
        not monotonically_increasing_id() -- this materialized view is
        fully recomputed on every refresh, so the key must be deterministic
        across recomputes (fact tables look it up by value, not position),
        which an ID generator tied to partition/task ordering wouldn't
        guarantee.

        load_dt/source_format/source_file/run_id are selected straight
        through from silver_locations_df, not regenerated via
        add_audit_columns() -- silver_locations already carries the
        original Bronze ingestion's lineage (which raw file, which run),
        so Gold re-stamping its own would overwrite that with Gold's own
        processing time and lose the trail back to the source.
    """
    return silver_locations_df.select(
        F.row_number().over(Window.orderBy("location")).cast("int").alias("location_key"),
        F.col("location"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("load_dt"),
        F.col("source_format"),
        F.col("source_file"),
        F.col("run_id"),
    )
