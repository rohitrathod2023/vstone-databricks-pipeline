"""Unit tests for pipelines.gold.agg_hourly_telegram_activity -- hourly
telegram message-volume rollup. ALTERNATIVE DESIGN, pending trainer review:
no observation_type column -- telegram rows are identified via
message_count.isNotNull() instead (see
docs/fact_table_without_discriminator_alternative.md).

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_agg_hourly_telegram_activity.py -v
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
        SparkSession.builder.master("local[2]").appName("vstone-agg-hourly-telegram-activity-tests").getOrCreate()
    )
    yield session
    session.stop()


def _obs_df(spark, rows):
    from pyspark.sql.types import IntegerType, StructField, StructType

    schema = StructType(
        [
            StructField("date_key", IntegerType()),
            StructField("time_key", IntegerType()),
            StructField("message_count", IntegerType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


def _dim_date_df(spark):
    return spark.createDataFrame(
        [(20240101, "2024-01-01", 2024, 1, "Monday", False)],
        ["date_key", "full_date", "year", "month", "day_name", "is_weekend"],
    )


def _dim_time_df(spark, rows=None):
    rows = rows or [(93000, 9), (93500, 9), (103000, 10)]
    return spark.createDataFrame(rows, ["time_key", "hour"])


def _build(spark, rows, dim_time_df=None):
    from pipelines.gold.agg_hourly_telegram_activity import build_agg_hourly_telegram_activity

    return build_agg_hourly_telegram_activity(
        _obs_df(spark, rows), _dim_date_df(spark), dim_time_df or _dim_time_df(spark)
    )


def test_schema_matches_expected_shape(spark):
    result = _build(spark, [(20240101, 93000, 1)])

    expected_columns = {
        "date_key", "full_date", "year", "month", "day_name", "is_weekend", "hour", "total_messages",
        "observation_count", "load_dt", "source_format", "source_file", "run_id",
    }
    assert set(result.columns) == expected_columns
    assert "message_length" not in result.columns
    assert "avg_message_length" not in result.columns


def test_messages_are_grouped_by_date_and_hour_not_mixed(spark):
    result = _build(
        spark,
        [
            (20240101, 93000, 1),  # 09:30 -> hour 9
            (20240101, 93500, 1),  # 09:35 -> hour 9
            (20240101, 103000, 1),  # 10:30 -> hour 10
        ],
    )
    rows = {r["hour"]: r for r in result.collect()}

    assert rows[9]["total_messages"] == 2
    assert rows[9]["observation_count"] == 2
    assert rows[10]["total_messages"] == 1


def test_only_rows_with_message_count_populated_are_aggregated(spark):
    """No observation_type column on this branch -- telegram rows are
    identified via message_count.isNotNull()."""
    result = _build(
        spark,
        [
            (20240101, 93000, 1),
            (20240101, 93000, None),
            (20240101, 93000, None),
        ],
    )

    assert result.count() == 1
    assert result.collect()[0]["total_messages"] == 1
