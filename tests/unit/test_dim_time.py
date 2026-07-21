"""Unit tests for pipelines.gold.dim_time -- the generated one-row-per-second
time-of-day dimension.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_dim_time.py -v
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

    session = SparkSession.builder.master("local[2]").appName("vstone-dim-time-tests").getOrCreate()
    yield session
    session.stop()


def test_row_count_is_one_per_second_of_day(spark):
    from pipelines.gold.dim_time import build_dim_time

    result = build_dim_time(spark)

    assert result.count() == 86_400


def test_midnight_row_has_expected_values(spark):
    from pipelines.gold.dim_time import build_dim_time

    row = build_dim_time(spark).filter("second_of_day = 0").collect()[0]

    assert row["time_key"] == 0
    assert row["full_time"] == "00:00:00"
    assert row["hour"] == 0
    assert row["hour_12"] == 12
    assert row["am_pm"] == "AM"
    assert row["is_business_hours"] is False


def test_an_arbitrary_time_computes_correct_hour_minute_second_and_time_key(spark):
    from pipelines.gold.dim_time import build_dim_time

    # 14:30:52 -> second_of_day = 14*3600 + 30*60 + 52 = 52252
    row = build_dim_time(spark).filter("second_of_day = 52252").collect()[0]

    assert row["hour"] == 14
    assert row["minute"] == 30
    assert row["second"] == 52
    assert row["time_key"] == 143052
    assert row["full_time"] == "14:30:52"
    assert row["hour_12"] == 2
    assert row["am_pm"] == "PM"
    assert row["is_business_hours"] is True


def test_business_hours_boundary_is_9am_inclusive_to_5pm_exclusive(spark):
    from pipelines.gold.dim_time import build_dim_time

    result = build_dim_time(spark)

    nine_am = result.filter("second_of_day = " + str(9 * 3600)).collect()[0]
    five_pm = result.filter("second_of_day = " + str(17 * 3600)).collect()[0]
    one_second_before_five_pm = result.filter("second_of_day = " + str(17 * 3600 - 1)).collect()[0]

    assert nine_am["is_business_hours"] is True
    assert five_pm["is_business_hours"] is False
    assert one_second_before_five_pm["is_business_hours"] is True


def test_schema_matches_expected_shape(spark):
    from pipelines.gold.dim_time import build_dim_time

    result = build_dim_time(spark)

    expected_columns = {
        "time_key", "full_time", "hour", "minute", "second", "hour_12", "am_pm", "time_of_day",
        "is_business_hours", "minute_of_day", "second_of_day",
    }
    assert set(result.columns) == expected_columns
