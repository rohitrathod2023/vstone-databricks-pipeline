"""Unit tests for pipelines.gold.gold_daily_summary -- the simple daily
rollup of fact_city_observations (traffic flow, environment averages,
telegram report volume).

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_gold_daily_summary.py -v
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

    session = SparkSession.builder.master("local[2]").appName("vstone-gold-daily-summary-tests").getOrCreate()
    yield session
    session.stop()


def _obs_df(spark, rows):
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("observation_type", StringType()),
            StructField("date_key", IntegerType()),
            StructField("enter", IntegerType()),
            StructField("exit", IntegerType()),
            StructField("noise", DoubleType()),
            StructField("pollution", DoubleType()),
            StructField("light", DoubleType()),
            StructField("raining", DoubleType()),
            StructField("message_count", IntegerType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


def test_schema_matches_expected_shape(spark):
    from pipelines.gold.gold_daily_summary import build_gold_daily_summary

    obs_df = _obs_df(spark, [("traffic", 20240101, 5, 3, None, None, None, None, None)])
    result = build_gold_daily_summary(obs_df)

    expected_columns = {
        "date_key", "total_vehicles_entered", "total_vehicles_exited", "net_traffic_flow", "avg_noise",
        "avg_pollution", "avg_light", "avg_raining", "telegram_message_count", "total_observations",
        "load_dt", "source_format", "source_file", "run_id",
    }
    assert set(result.columns) == expected_columns


def test_one_row_per_date_across_all_three_observation_types(spark):
    obs_df = _obs_df(
        spark,
        [
            ("traffic", 20240101, 5, 3, None, None, None, None, None),
            ("environmental", 20240101, None, None, 10.0, 5.0, 20.0, 0.3, None),
            ("telegram", 20240101, None, None, None, None, None, None, 1),
        ],
    )
    from pipelines.gold.gold_daily_summary import build_gold_daily_summary

    result = build_gold_daily_summary(obs_df)

    assert result.count() == 1


def test_measures_are_scoped_to_their_own_observation_type_not_mixed(spark):
    """A traffic row's enter/exit must not leak into another date's
    environmental averages, and vice versa -- each measure is computed with
    a conditional aggregate scoped to its own observation_type."""
    from pipelines.gold.gold_daily_summary import build_gold_daily_summary

    obs_df = _obs_df(
        spark,
        [
            ("traffic", 20240101, 5, 3, None, None, None, None, None),
            ("traffic", 20240101, 7, 2, None, None, None, None, None),
            ("environmental", 20240101, None, None, 10.0, 4.0, 20.0, 0.3, None),
            ("environmental", 20240101, None, None, 20.0, 6.0, 30.0, 0.5, None),
            ("telegram", 20240101, None, None, None, None, None, None, 1),
            ("telegram", 20240101, None, None, None, None, None, None, 1),
        ],
    )

    row = build_gold_daily_summary(obs_df).collect()[0]

    assert row["total_vehicles_entered"] == 12
    assert row["total_vehicles_exited"] == 5
    assert row["net_traffic_flow"] == 7
    assert row["avg_noise"] == 15.0
    assert row["avg_pollution"] == 5.0
    assert row["avg_light"] == 25.0
    assert row["avg_raining"] == 0.4
    assert row["telegram_message_count"] == 2
    assert row["total_observations"] == 6


def test_a_date_with_only_one_observation_type_has_null_measures_for_the_others(spark):
    from pipelines.gold.gold_daily_summary import build_gold_daily_summary

    obs_df = _obs_df(spark, [("traffic", 20240101, 5, 3, None, None, None, None, None)])
    row = build_gold_daily_summary(obs_df).collect()[0]

    assert row["total_vehicles_entered"] == 5
    assert row["avg_noise"] is None
    assert row["telegram_message_count"] is None
    assert row["total_observations"] == 1


def test_different_dates_produce_separate_rows(spark):
    from pipelines.gold.gold_daily_summary import build_gold_daily_summary

    obs_df = _obs_df(
        spark,
        [
            ("traffic", 20240101, 5, 3, None, None, None, None, None),
            ("traffic", 20240102, 8, 4, None, None, None, None, None),
        ],
    )
    rows = {r["date_key"]: r for r in build_gold_daily_summary(obs_df).collect()}

    assert len(rows) == 2
    assert rows[20240101]["total_vehicles_entered"] == 5
    assert rows[20240102]["total_vehicles_entered"] == 8
