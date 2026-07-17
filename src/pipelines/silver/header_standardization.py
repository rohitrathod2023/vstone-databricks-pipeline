"""Silver column-NAME standardization -- defensive insurance against a future
messy header, not a fix for an observed problem (see Reliability pillar note
below). Schema-only (withColumnRenamed); see text_normalization.py for the
actual Pandas UDF that normalizes column VALUES instead.
"""
from __future__ import annotations

import re

from pyspark.sql import DataFrame

# Non-alphanumeric, non-underscore runs (spaces, hyphens, punctuation, etc.)
# collapse to one underscore; underscores are excluded here so a *separate*
# pass can collapse repeats -- matches the agreed 5-step order instead of
# folding steps 3-4 into a single regex.
_NON_ALNUM_OR_UNDERSCORE_RUN = re.compile(r"[^a-z0-9_]+")
_REPEATED_UNDERSCORE_RUN = re.compile(r"_+")


def standardize_column_name(name: str) -> str:
    """Standardize a column name to lowercase snake_case.

    Args:
        name: Raw column name (any case, may contain spaces/punctuation).

    Returns:
        Lowercase name with runs of non-alphanumeric characters collapsed to
        a single underscore and no leading/trailing underscore.
    """
    lowered = name.lower().strip()
    replaced = _NON_ALNUM_OR_UNDERSCORE_RUN.sub("_", lowered)
    collapsed = _REPEATED_UNDERSCORE_RUN.sub("_", replaced)
    return collapsed.strip("_")


def apply_header_standardization(df: DataFrame) -> DataFrame:
    """Rename every column in a DataFrame via standardize_column_name.

    Args:
        df: Any DataFrame whose column names should be standardized.

    Returns:
        DataFrame with standardized column names; row values are unchanged.

    Notes:
        Every Silver-bound column name profiled so far is already clean --
        this defends against a future messy data drop rather than fixing a
        found issue (Databricks Well-Architected Reliability pillar). Same
        spirit as Bronze's sanitize_column_name() (pyspark_xml_ingest.py),
        generalized here rather than reused, since Bronze's version is
        scoped specifically to XML tag/attribute naming.
    """
    for original in df.columns:
        standardized = standardize_column_name(original)
        # Skip the rename entirely when already standardized, rather than
        # calling withColumnRenamed with identical old/new names.
        if standardized != original:
            df = df.withColumnRenamed(original, standardized)
    return df
