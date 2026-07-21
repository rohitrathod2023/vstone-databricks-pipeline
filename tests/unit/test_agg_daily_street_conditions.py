"""Unit tests for pipelines.gold.agg_daily_street_conditions -- daily
environmental rollup per street. Focused on the real bug found in review:
summing `raining` directly would let the -1 "not raining" sentinel silently
subtract from the total.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_agg_daily_street_conditions.py -v
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

    session = (
        SparkSession.builder.master("local[2]").appName("vstone-agg-daily-street-conditions-tests").getOrCreate()
    )
    yield session
    session.stop()


def _obs_df(spark, rows):
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("observation_type", StringType()),
            StructField("street_key", IntegerType()),
            StructField("date_key", IntegerType()),
            StructField("noise", DoubleType()),
            StructField("pollution", DoubleType()),
            StructField("light", DoubleType()),
            StructField("raining", DoubleType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


def _dim_street_df(spark):
    return spark.createDataFrame([(1, 10, "Main Street", True)], ["street_key", "street_id", "street", "is_current"])


def _dim_date_df(spark):
    return spark.createDataFrame(
        [(20240101, "2024-01-01", 2024, 1, "Monday", False)],
        ["date_key", "full_date", "year", "month", "day_name", "is_weekend"],
    )


def _build(spark, rows):
    from pipelines.gold.agg_daily_street_conditions import build_agg_daily_street_conditions

    return build_agg_daily_street_conditions(_obs_df(spark, rows), _dim_street_df(spark), _dim_date_df(spark))


def test_schema_matches_expected_shape(spark):
    result = _build(spark, [("environmental", 1, 20240101, 10.0, 5.0, 20.0, 0.3)])

    expected_columns = {
        "street_key", "street_id", "street", "date_key", "full_date", "year", "month", "day_name", "is_weekend",
        "avg_noise", "max_noise", "min_noise", "avg_pollution", "max_pollution", "min_pollution", "avg_light",
        "max_light", "min_light", "rain_intensity_sum", "observation_count",
        "load_dt", "source_format", "source_file", "run_id",
    }
    assert set(result.columns) == expected_columns


def test_rain_intensity_sum_excludes_the_negative_sentinel(spark):
    """Regression test for the real bug: raining's source range is -1 to
    99.99, where -1 is a sentinel meaning "not raining" (~49% of raw
    values), not a real reading. Summing the raw column directly would let
    -1 rows subtract from the total instead of contributing 0."""
    result = _build(
        spark,
        [
            ("environmental", 1, 20240101, 10.0, 5.0, 20.0, 5.0),
            ("environmental", 1, 20240101, 10.0, 5.0, 20.0, -1.0),
            ("environmental", 1, 20240101, 10.0, 5.0, 20.0, -1.0),
        ],
    )
    row = result.collect()[0]

    # Naive F.sum("raining") would give 5.0 + (-1.0) + (-1.0) = 3.0.
    assert row["rain_intensity_sum"] == 5.0
    assert row["observation_count"] == 3


def test_only_environmental_rows_are_aggregated(spark):
    result = _build(
        spark,
        [
            ("environmental", 1, 20240101, 10.0, 5.0, 20.0, 0.3),
            ("traffic", 1, 20240101, None, None, None, None),
            ("telegram", 1, 20240101, None, None, None, None),
        ],
    )

    assert result.count() == 1
    assert result.collect()[0]["observation_count"] == 1


def test_measures_are_grouped_per_street_and_date_not_mixed(spark):
    from pipelines.gold.agg_daily_street_conditions import build_agg_daily_street_conditions

    dim_street_df = spark.createDataFrame(
        [(1, 10, "Main Street", True), (2, 20, "Second Street", True)],
        ["street_key", "street_id", "street", "is_current"],
    )
    obs_df = _obs_df(
        spark,
        [
            ("environmental", 1, 20240101, 10.0, 5.0, 20.0, 0.3),
            ("environmental", 1, 20240101, 20.0, 7.0, 30.0, 0.5),
            ("environmental", 2, 20240101, 100.0, 50.0, 5.0, 0.0),
        ],
    )
    result = build_agg_daily_street_conditions(obs_df, dim_street_df, _dim_date_df(spark))
    rows = {r["street_key"]: r for r in result.collect()}

    assert rows[1]["avg_noise"] == 15.0
    assert rows[1]["observation_count"] == 2
    assert rows[2]["avg_noise"] == 100.0
    assert rows[2]["observation_count"] == 1
