"""Pandas UDF for streets_list.street text-VALUE normalization -- distinct
from header_standardization.py's column-NAME renaming. Not wired into
silver_streets yet; that happens once the Silver tables are built.
"""
from __future__ import annotations

import re
from typing import Optional

import pandas as pd
from pyspark.sql.functions import pandas_udf

_WHITESPACE_RUN = re.compile(r"\s+")


def normalize_street_name(value: Optional[str]) -> Optional[str]:
    """Trim and collapse whitespace in a street name, preserving casing.

    Args:
        value: Raw street name, or None.

    Returns:
        Whitespace-normalized street name, or None if the input was None.

    Notes:
        Casing is left untouched -- street names are proper nouns, and
        whitespace is the only messiness class found in profiling so far.
    """
    if value is None:
        return None
    return _WHITESPACE_RUN.sub(" ", value.strip())


def normalize_street_name_udf():
    """Build a pandas UDF wrapping normalize_street_name for .withColumn().

    Returns:
        A pandas_udf (string -> string) ready to apply to a Spark column.

    Notes:
        Built lazily rather than at import time -- pandas_udf's return-type
        parsing requires an active Spark session, which pytest's module
        collection doesn't guarantee yet.
    """

    def _normalize_street_name_series(series: pd.Series) -> pd.Series:
        def _clean(value: Optional[str]) -> Optional[str]:
            if value is None:
                return None
            return _WHITESPACE_RUN.sub(" ", value.strip())

        # Redefines normalize_street_name's body instead of calling it, so
        # cloudpickle serializes this nested closure BY VALUE. A reference to
        # a module-level function is serialized BY NAME instead, which needs
        # `pipelines.silver.text_normalization` importable on the executor --
        # confirmed this crashes with ModuleNotFoundError on serverless
        # Lakeflow compute, since sys.path inserts on the driver don't reach
        # the isolated Python worker processes serverless DLT runs UDFs in.
        return series.apply(_clean)

    return pandas_udf(_normalize_street_name_series, "string")
