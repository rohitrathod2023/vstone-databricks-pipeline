"""Unit tests for pipelines.gold.gold_location_summary -- the simple
per-location traffic rollup answering "busiest intersections."

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_gold_location_summary.py -v
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

    session = SparkSession.builder.master("local[2]").appName("vstone-gold-location-summary-tests").getOrCreate()
    yield session
    session.stop()


def _obs_df(spark, rows):
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("observation_type", StringType()),
            StructField("location_key", IntegerType()),
            StructField("enter", IntegerType()),
            StructField("exit", IntegerType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


def test_schema_matches_expected_shape(spark):
    from pipelines.gold.gold_location_summary import build_gold_location_summary

    obs_df = _obs_df(spark, [("traffic", 1, 5, 3)])
    result = build_gold_location_summary(obs_df)

    expected_columns = {
        "location_key", "total_vehicles_entered", "total_vehicles_exited", "total_traffic_volume",
        "total_traffic_readings", "load_dt", "source_format", "source_file", "run_id",
    }
    assert set(result.columns) == expected_columns


def test_totals_are_summed_per_location_not_mixed_across_locations(spark):
    from pipelines.gold.gold_location_summary import build_gold_location_summary

    obs_df = _obs_df(
        spark,
        [
            ("traffic", 1, 5, 3),
            ("traffic", 1, 7, 2),
            ("traffic", 2, 10, 8),
        ],
    )
    rows = {r["location_key"]: r for r in build_gold_location_summary(obs_df).collect()}

    assert rows[1]["total_vehicles_entered"] == 12
    assert rows[1]["total_vehicles_exited"] == 5
    assert rows[1]["total_traffic_volume"] == 17
    assert rows[1]["total_traffic_readings"] == 2
    assert rows[2]["total_vehicles_entered"] == 10
    assert rows[2]["total_traffic_readings"] == 1


def test_environmental_and_telegram_rows_are_excluded_not_grouped_as_null_location(spark):
    """environmental/telegram rows always have a NULL location_key --
    filtering to observation_type == 'traffic' first keeps them out of the
    aggregate entirely, rather than collapsing them into a location=None
    group alongside location=7's real orphaned traffic readings."""
    from pipelines.gold.gold_location_summary import build_gold_location_summary

    obs_df = _obs_df(
        spark,
        [
            ("traffic", 7, 5, 3),  # real orphan: location=7 has no dim_location match
            ("environmental", None, None, None),
            ("telegram", None, None, None),
        ],
    )
    result = build_gold_location_summary(obs_df)

    assert result.count() == 1
    row = result.collect()[0]
    assert row["location_key"] == 7
    assert row["total_vehicles_entered"] == 5


def test_a_location_with_zero_traffic_readings_never_appears(spark):
    from pipelines.gold.gold_location_summary import build_gold_location_summary

    obs_df = _obs_df(spark, [("telegram", None, None, None)])
    result = build_gold_location_summary(obs_df)

    assert result.count() == 0
