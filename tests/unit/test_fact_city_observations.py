"""Unit tests for pipelines.gold.fact_city_observations -- the unified Gold
fact table. Focused on the two real bugs found in review: date_key
resolving to NULL for non-midnight timestamps (TIMESTAMP vs. DATE join
without F.to_date()), and technique_key being hardcoded instead of resolved
from the real, per-row varying silver_traffic.source_technique column.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_fact_city_observations.py -v
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[2]").appName("vstone-fact-city-observations-tests").getOrCreate()
    yield session
    session.stop()


_TRAFFIC_COLS = ["id", "location", "enter", "exit", "date", "source_technique", "load_dt", "source_format", "source_file", "run_id"]  # noqa: E501
_ENVIRONMENT_COLS = ["street_id", "date", "noise", "pollution", "light", "raining", "load_dt", "source_format", "source_file", "run_id"]  # noqa: E501
_TELEGRAM_COLS = ["message", "event_timestamp", "load_dt", "source_format", "source_file", "run_id"]


def _traffic_schema():
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("id", IntegerType()),
            StructField("location", IntegerType()),
            StructField("enter", IntegerType()),
            StructField("exit", IntegerType()),
            StructField("date", StringType()),
            StructField("source_technique", StringType()),
            StructField("load_dt", TimestampType()),
            StructField("source_format", StringType()),
            StructField("source_file", StringType()),
            StructField("run_id", StringType()),
        ]
    )


def _environment_schema():
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("street_id", IntegerType()),
            StructField("date", StringType()),
            StructField("noise", DoubleType()),
            StructField("pollution", DoubleType()),
            StructField("light", DoubleType()),
            StructField("raining", DoubleType()),
            StructField("load_dt", TimestampType()),
            StructField("source_format", StringType()),
            StructField("source_file", StringType()),
            StructField("run_id", StringType()),
        ]
    )


def _telegram_schema():
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("message", StringType()),
            StructField("event_timestamp", TimestampType()),
            StructField("load_dt", TimestampType()),
            StructField("source_format", StringType()),
            StructField("source_file", StringType()),
            StructField("run_id", StringType()),
        ]
    )


_TRAFFIC_AUDIT = (datetime(2024, 1, 1, 12, 0, 0), "delta", "silver_traffic", "run-1")
_ENV_AUDIT = (datetime(2024, 1, 1, 13, 0, 0), "delta", "silver_environment", "run-1")
_TELEGRAM_AUDIT = (datetime(2024, 1, 1, 14, 0, 0), "csv", "silver_telegram", "run-1")


def _dim_street_df(spark):
    return spark.createDataFrame([(1, 1, True)], ["street_key", "street_id", "is_current"])


def _dim_location_df(spark):
    return spark.createDataFrame([(1, 1)], ["location_key", "location"])


def _dim_date_df(spark):
    # Deliberately a STRING full_date -- Spark implicitly compares a DATE
    # against a STRING here, same as production's real dim_date.
    return spark.createDataFrame(
        [(20240101, "2024-01-01"), (20240102, "2024-01-02")], ["date_key", "full_date"]
    )


def _dim_audit_df(spark):
    return spark.createDataFrame(
        [(1,) + _TRAFFIC_AUDIT, (2,) + _ENV_AUDIT, (3,) + _TELEGRAM_AUDIT],
        ["audit_key", "load_dt", "source_format", "source_file", "run_id"],
    )


def _dim_technique_df(spark):
    return spark.createDataFrame(
        [(1, "autoloader"), (2, "copyinto"), (3, "dlt"), (4, "pyspark")],
        ["technique_key", "technique_name"],
    )


def _build(spark, traffic_rows=None, environment_rows=None, telegram_rows=None):
    from pipelines.gold.fact_city_observations import build_fact_city_observations

    traffic_df = spark.createDataFrame(traffic_rows, _TRAFFIC_COLS) if traffic_rows else spark.createDataFrame([], _traffic_schema())  # noqa: E501
    environment_df = spark.createDataFrame(environment_rows, _ENVIRONMENT_COLS) if environment_rows else spark.createDataFrame([], _environment_schema())  # noqa: E501
    telegram_df = spark.createDataFrame(telegram_rows, _TELEGRAM_COLS) if telegram_rows else spark.createDataFrame([], _telegram_schema())  # noqa: E501

    return build_fact_city_observations(
        traffic_df,
        environment_df,
        telegram_df,
        _dim_street_df(spark),
        _dim_location_df(spark),
        _dim_date_df(spark),
        _dim_audit_df(spark),
        _dim_technique_df(spark),
    )


def test_date_key_resolves_for_traffic_rows_with_a_non_midnight_timestamp(spark):
    """Regression test for the real bug: the original join compared
    traffic.date (TIMESTAMP) directly to dim_date.full_date (DATE) with no
    F.to_date() cast, so any row with a real time-of-day component (like
    every real traffic reading) resolved to a NULL date_key."""
    result = _build(
        spark,
        traffic_rows=[(100, 1, 5, 3, "2024-01-02T10:00:00", "copyinto") + _TRAFFIC_AUDIT],
    )
    row = result.filter(result.observation_type == "traffic").collect()[0]

    assert row["date_key"] == 20240102


def test_date_key_resolves_for_environmental_rows_with_a_non_midnight_timestamp(spark):
    """Same regression as above, for the environmental branch's join."""
    result = _build(
        spark,
        environment_rows=[(1, "2024-01-02T15:30:00", 10.0, 5.0, 20.0, 0.3) + _ENV_AUDIT],
    )
    row = result.filter(result.observation_type == "environmental").collect()[0]

    assert row["date_key"] == 20240102


def test_traffic_technique_key_resolves_per_row_from_source_technique(spark):
    """Regression test for the real bug: technique_key was hardcoded to a
    constant (3, "dlt") for every traffic row, discarding source_technique's
    real per-row variation across all 4 ingestion techniques."""
    result = _build(
        spark,
        traffic_rows=[
            (100, 1, 5, 3, "2024-01-01T10:00:00", "copyinto") + _TRAFFIC_AUDIT,
            (101, 1, 6, 4, "2024-01-01T11:00:00", "autoloader") + _TRAFFIC_AUDIT,
        ],
    )
    rows = {r["enter"]: r for r in result.filter(result.observation_type == "traffic").collect()}

    assert rows[5]["technique_key"] == 2  # copyinto
    assert rows[6]["technique_key"] == 1  # autoloader


def test_environmental_and_telegram_technique_key_resolve_to_the_single_copyinto_technique(spark):
    """environment/telegram carry no per-row source_technique column --
    both real Bronze sources behind them (bronze_streets, bronze_telegram)
    use copy_into, so technique_key must resolve to that technique's key,
    not an invented technique that doesn't exist in dim_technique."""
    result = _build(
        spark,
        environment_rows=[(1, "2024-01-01T15:30:00", 10.0, 5.0, 20.0, 0.3) + _ENV_AUDIT],
        telegram_rows=[("a report", datetime(2024, 1, 1, 9, 0, 0)) + _TELEGRAM_AUDIT],
    )
    env_row = result.filter(result.observation_type == "environmental").collect()[0]
    telegram_row = result.filter(result.observation_type == "telegram").collect()[0]

    assert env_row["technique_key"] == 2
    assert telegram_row["technique_key"] == 2


def test_row_count_is_the_sum_of_all_three_sources_no_fan_out(spark):
    result = _build(
        spark,
        traffic_rows=[(100, 1, 5, 3, "2024-01-01T10:00:00", "copyinto") + _TRAFFIC_AUDIT],
        environment_rows=[(1, "2024-01-01T15:30:00", 10.0, 5.0, 20.0, 0.3) + _ENV_AUDIT],
        telegram_rows=[("a report", datetime(2024, 1, 1, 9, 0, 0)) + _TELEGRAM_AUDIT],
    )

    assert result.count() == 3


def test_schema_matches_expected_shape(spark):
    result = _build(spark, traffic_rows=[(100, 1, 5, 3, "2024-01-01T10:00:00", "copyinto") + _TRAFFIC_AUDIT])

    expected_columns = {
        "observation_type", "street_key", "location_key", "date_key", "time_key", "technique_key", "audit_key",
        "noise", "pollution", "light", "raining", "enter", "exit", "message_count", "message_length",
        "observation_id",
    }
    assert set(result.columns) == expected_columns


def test_traffic_row_has_null_environmental_and_telegram_measures(spark):
    result = _build(spark, traffic_rows=[(100, 1, 5, 3, "2024-01-01T10:00:00", "copyinto") + _TRAFFIC_AUDIT])
    row = result.collect()[0]

    assert row["enter"] == 5
    assert row["exit"] == 3
    assert row["noise"] is None
    assert row["pollution"] is None
    assert row["message_count"] is None
    assert row["message_length"] is None


def test_audit_key_resolves_from_the_fact_sides_own_audit_columns(spark):
    result = _build(spark, traffic_rows=[(100, 1, 5, 3, "2024-01-01T10:00:00", "copyinto") + _TRAFFIC_AUDIT])
    row = result.collect()[0]

    assert row["audit_key"] == 1
