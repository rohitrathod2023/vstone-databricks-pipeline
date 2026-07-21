"""Unit tests for pipelines.gold.agg_daily_location_traffic -- daily traffic
rollup per location.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_agg_daily_location_traffic.py -v
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
        SparkSession.builder.master("local[2]").appName("vstone-agg-daily-location-traffic-tests").getOrCreate()
    )
    yield session
    session.stop()


def _obs_df(spark, rows):
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("observation_type", StringType()),
            StructField("location_key", IntegerType()),
            StructField("date_key", IntegerType()),
            StructField("enter", IntegerType()),
            StructField("exit", IntegerType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


def _dim_location_df(spark):
    return spark.createDataFrame([(1, 1, 38.98, -0.53)], ["location_key", "location", "latitude", "longitude"])


def _dim_date_df(spark):
    return spark.createDataFrame(
        [(20240101, "2024-01-01", 2024, 1, "Monday", False)],
        ["date_key", "full_date", "year", "month", "day_name", "is_weekend"],
    )


def _build(spark, rows, dim_location_df=None):
    from pipelines.gold.agg_daily_location_traffic import build_agg_daily_location_traffic

    return build_agg_daily_location_traffic(
        _obs_df(spark, rows), dim_location_df or _dim_location_df(spark), _dim_date_df(spark)
    )


def test_schema_matches_expected_shape(spark):
    result = _build(spark, [("traffic", 1, 20240101, 5, 3)])

    expected_columns = {
        "location_key", "location", "latitude", "longitude", "date_key", "full_date", "year", "month", "day_name",
        "is_weekend", "total_enter", "total_exit", "net_traffic", "avg_enter", "avg_exit", "max_enter", "max_exit",
        "observation_count", "load_dt", "source_format", "source_file", "run_id",
    }
    assert set(result.columns) == expected_columns


def test_totals_and_net_traffic_are_summed_correctly(spark):
    result = _build(
        spark,
        [
            ("traffic", 1, 20240101, 5, 3),
            ("traffic", 1, 20240101, 7, 2),
        ],
    )
    row = result.collect()[0]

    assert row["total_enter"] == 12
    assert row["total_exit"] == 5
    assert row["net_traffic"] == 7
    assert row["observation_count"] == 2


def test_only_traffic_rows_are_aggregated(spark):
    result = _build(
        spark,
        [
            ("traffic", 1, 20240101, 5, 3),
            ("environmental", None, 20240101, None, None),
            ("telegram", None, 20240101, None, None),
        ],
    )

    assert result.count() == 1
    assert result.collect()[0]["total_enter"] == 5


def test_location_7_orphan_surfaces_as_a_null_keyed_group_not_dropped(spark):
    """location=7's coordinates were quarantined at Silver for bad data and
    excluded from dim_location -- its traffic readings still exist as a
    real domain and must surface here with a NULL location_key, matching
    the precedent already documented for gold_location_summary."""
    result = _build(spark, [("traffic", 7, 20240101, 5, 3)])
    row = result.collect()[0]

    assert row["location_key"] == 7
    assert row["location"] is None
    assert row["total_enter"] == 5
