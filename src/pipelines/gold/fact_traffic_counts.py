"""Fact_Traffic_Counts: silver_traffic joined to Dim_Location and Dim_Date,
resolving natural keys to surrogate keys. No aggregation -- one row per
original silver_traffic row (id + location + date).
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_fact_traffic_counts(traffic_df: DataFrame, dim_location_df: DataFrame, dim_date_df: DataFrame) -> DataFrame:
    """Join silver_traffic to Dim_Location and Dim_Date, resolving surrogate keys.

    Args:
        traffic_df: silver_traffic (id, location, enter, exit, date,
            source_technique, plus its own audit columns).
        dim_location_df: Dim_Location (location, location_key, ...).
        dim_date_df: Dim_Date (full_date, date_key, ...).

    Returns:
        DataFrame with location_key, date_key, id, enter, exit,
        source_technique, plus the 4 audit columns carried through from
        silver_traffic (the driving/fact-side table) unchanged.

    Notes:
        Uses LEFT joins, not inner -- an inner join would silently drop any
        row whose location/date fails to resolve, which would make a
        row-count mismatch impossible to detect (the count would just look
        smaller with no signal that something dropped). LEFT preserves
        unmatched rows as NULL location_key/date_key so a real
        no-dropped-rows test can actually catch a failed resolution. This
        join is a pure lookup (Dim_Location has no duplicate `location`
        values, Dim_Date has no duplicate `full_date` values), so it must
        not fan out -- row count in should equal row count out.

        Audit columns come from silver_traffic (t.*), not Dim_Location or
        Dim_Date -- those are pure lookups here, and a fact row's real
        lineage is the fact-side record it came from, not whichever
        dimension row it happened to resolve to. Selected straight through,
        not regenerated via add_audit_columns(), so the trail back to the
        original Bronze ingestion (which raw file, which technique, which
        run) survives all the way into Gold.
    """
    joined = (
        traffic_df.alias("t")
        .join(
            dim_location_df.select("location", "location_key").alias("loc"),
            F.col("t.location") == F.col("loc.location"),
            "left",
        )
        .join(
            dim_date_df.select("full_date", "date_key").alias("d"),
            F.to_date(F.col("t.date")) == F.col("d.full_date"),
            "left",
        )
        .select(
            F.col("loc.location_key").alias("location_key"),
            F.col("d.date_key").alias("date_key"),
            F.col("t.id").alias("id"),
            F.col("t.enter").alias("enter"),
            F.col("t.exit").alias("exit"),
            F.col("t.source_technique").alias("source_technique"),
            F.col("t.load_dt").alias("load_dt"),
            F.col("t.source_format").alias("source_format"),
            F.col("t.source_file").alias("source_file"),
            F.col("t.run_id").alias("run_id"),
        )
    )
    return joined
