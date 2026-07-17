"""Unit tests for pipelines.gold.dim_location -- static location dimension
built from silver_locations. No SCD2, no re-filtering (Silver already
rejected location=7's bad coordinates).

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_dim_location.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[2]").appName("vstone-dim-location-tests").getOrCreate()
    yield session
    session.stop()


def _silver_locations_df(spark):
    """13 rows, matching the real silver_locations shape (location=7's bad
    row already excluded, since Silver rejects it before this table ever
    sees it)."""
    rows = [(loc, 38.9 + loc * 0.01, -0.5 - loc * 0.01) for loc in [1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14]]
    return spark.createDataFrame(rows, ["location", "latitude", "longitude"])


def test_row_count_matches_silver_locations_exactly(spark):
    from pipelines.gold.dim_location import build_dim_location

    df = build_dim_location(_silver_locations_df(spark))

    assert df.count() == 13


def test_schema_matches_expected_shape(spark):
    from pipelines.gold.dim_location import build_dim_location

    df = build_dim_location(_silver_locations_df(spark))

    expected_columns = {
        "location_key", "location", "latitude", "longitude", "load_dt", "source_format", "source_file", "run_id"
    }
    assert set(df.columns) == expected_columns


def test_location_key_is_unique_and_int_typed(spark):
    from pyspark.sql.types import IntegerType

    from pipelines.gold.dim_location import build_dim_location

    df = build_dim_location(_silver_locations_df(spark))
    keys = [r["location_key"] for r in df.collect()]

    assert len(keys) == len(set(keys))
    assert dict(df.dtypes)["location_key"] == IntegerType().simpleString()


def test_location_key_is_stable_across_repeated_builds(spark):
    """A materialized view is fully recomputed every refresh -- the
    surrogate key must be deterministic (tied to the natural key's sort
    order), not tied to task/partition ordering, or fact tables' joins
    would silently break on the next refresh."""
    from pipelines.gold.dim_location import build_dim_location

    df1 = build_dim_location(_silver_locations_df(spark))
    df2 = build_dim_location(_silver_locations_df(spark))

    keys1 = {r["location"]: r["location_key"] for r in df1.collect()}
    keys2 = {r["location"]: r["location_key"] for r in df2.collect()}

    assert keys1 == keys2


def test_latitude_and_longitude_are_carried_through_unchanged(spark):
    from pipelines.gold.dim_location import build_dim_location

    df = build_dim_location(_silver_locations_df(spark))
    row = df.filter("location = 1").collect()[0]

    assert row["latitude"] == 38.91
    assert row["longitude"] == -0.51
