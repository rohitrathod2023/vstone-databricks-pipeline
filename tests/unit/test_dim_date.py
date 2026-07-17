"""Unit tests for pipelines.gold.dim_date -- generated calendar dimension.
Row count is a real, checkable arithmetic fact (days between min/max
inclusive); date_key/day_of_week/is_weekend are spot-checked against known
real dates, not just assumed correct.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_dim_date.py -v
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

    session = SparkSession.builder.master("local[2]").appName("vstone-dim-date-tests").getOrCreate()
    yield session
    session.stop()


def test_row_count_matches_days_between_range_inclusive(spark):
    from pipelines.gold.dim_date import build_dim_date

    df = build_dim_date(spark, "2024-01-01", "2024-01-10")

    assert df.count() == 10


def test_schema_matches_expected_shape(spark):
    from pipelines.gold.dim_date import build_dim_date

    df = build_dim_date(spark, "2024-01-01", "2024-01-02")

    expected_columns = {
        "date_key",
        "full_date",
        "year",
        "month",
        "month_name",
        "day_of_month",
        "day_of_week",
        "day_name",
        "quarter",
        "is_weekend",
        "load_dt",
        "source_format",
        "source_file",
        "run_id",
    }
    assert set(df.columns) == expected_columns


def test_date_key_encodes_full_date_as_yyyymmdd(spark):
    from pipelines.gold.dim_date import build_dim_date

    df = build_dim_date(spark, "2024-01-01", "2024-01-01")
    row = df.collect()[0]

    assert row["date_key"] == 20240101
    assert str(row["full_date"]) == "2024-01-01"


def test_known_saturday_and_sunday_are_marked_weekend(spark):
    from pipelines.gold.dim_date import build_dim_date

    df = build_dim_date(spark, "2024-01-06", "2024-01-07")
    rows = {str(r["full_date"]): r for r in df.collect()}

    assert rows["2024-01-06"]["day_name"] == "Saturday"
    assert rows["2024-01-06"]["is_weekend"] is True
    assert rows["2024-01-07"]["day_name"] == "Sunday"
    assert rows["2024-01-07"]["is_weekend"] is True


def test_known_weekday_is_not_marked_weekend(spark):
    from pipelines.gold.dim_date import build_dim_date

    df = build_dim_date(spark, "2024-01-08", "2024-01-08")
    row = df.collect()[0]

    assert row["day_name"] == "Monday"
    assert row["is_weekend"] is False


def test_year_month_and_quarter_are_correct_for_a_known_date(spark):
    from pipelines.gold.dim_date import build_dim_date

    df = build_dim_date(spark, "2024-01-01", "2024-01-01")
    row = df.collect()[0]

    assert row["year"] == 2024
    assert row["month"] == 1
    assert row["month_name"] == "January"
    assert row["day_of_month"] == 1
    assert row["quarter"] == 1


def test_compute_date_range_takes_the_overall_min_max_and_buffers_it(spark):
    """Uses synthetic DataFrames standing in for silver_traffic/environment/
    telegram -- confirms compute_date_range takes the true overall min/max
    across all three (not just one table) and applies the buffer."""
    from pipelines.gold.dim_date import compute_date_range

    traffic_df = spark.createDataFrame([("2024-02-01",)], ["date"])
    environment_df = spark.createDataFrame([("2024-01-15",)], ["date"])
    telegram_df = spark.createDataFrame([("2024-03-01",)], ["event_timestamp"])

    min_date, max_date = compute_date_range(traffic_df, environment_df, telegram_df, buffer_days=3)

    assert min_date == "2024-01-12"  # 2024-01-15 - 3 days
    assert max_date == "2024-03-04"  # 2024-03-01 + 3 days
