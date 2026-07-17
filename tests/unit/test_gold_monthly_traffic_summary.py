"""Unit tests for pipelines.gold.gold_monthly_traffic_summary.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_gold_monthly_traffic_summary.py -v
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
        SparkSession.builder.master("local[2]").appName("vstone-gold-monthly-traffic-summary-tests").getOrCreate()
    )
    yield session
    session.stop()


def _fact_cols():
    return ["location_key", "date_key", "id", "enter", "exit", "source_technique"]


def _dim_date_cols():
    return ["date_key", "year", "month"]


def test_schema_matches_expected_shape(spark):
    from pipelines.gold.gold_monthly_traffic_summary import build_gold_monthly_traffic_summary

    fact_df = spark.createDataFrame([(1, 20240101, 1, 10, 5, "auto")], _fact_cols())
    dim_date_df = spark.createDataFrame([(20240101, 2024, 1)], _dim_date_cols())

    result = build_gold_monthly_traffic_summary(fact_df, dim_date_df)

    expected_columns = {
        "location_key", "year", "month", "total_enter", "total_exit", "total_traffic_volume",
        "avg_daily_traffic_volume", "busiest_rank_in_month",
        "load_dt", "source_format", "source_file", "run_id",
    }
    assert set(result.columns) == expected_columns


def test_totals_are_summed_within_the_same_location_and_month(spark):
    from pipelines.gold.gold_monthly_traffic_summary import build_gold_monthly_traffic_summary

    fact_df = spark.createDataFrame(
        [
            (1, 20240101, 1, 10, 5, "auto"),
            (1, 20240102, 2, 20, 8, "auto"),
        ],
        _fact_cols(),
    )
    dim_date_df = spark.createDataFrame(
        [(20240101, 2024, 1), (20240102, 2024, 1)], _dim_date_cols()
    )

    result = build_gold_monthly_traffic_summary(fact_df, dim_date_df)
    row = result.collect()[0]

    assert row["total_enter"] == 30
    assert row["total_exit"] == 13
    assert row["total_traffic_volume"] == 43


def test_avg_daily_traffic_volume_divides_by_distinct_days_present_not_a_hardcoded_calendar_count(spark):
    from pipelines.gold.gold_monthly_traffic_summary import build_gold_monthly_traffic_summary

    # Only 2 distinct days of data exist for January, even though January has 31 days.
    fact_df = spark.createDataFrame(
        [
            (1, 20240101, 1, 10, 10, "auto"),
            (1, 20240102, 2, 20, 20, "auto"),
        ],
        _fact_cols(),
    )
    dim_date_df = spark.createDataFrame(
        [(20240101, 2024, 1), (20240102, 2024, 1)], _dim_date_cols()
    )

    result = build_gold_monthly_traffic_summary(fact_df, dim_date_df)
    row = result.collect()[0]

    # total_traffic_volume = 60, divided by 2 distinct days = 30, NOT 60/31.
    assert row["avg_daily_traffic_volume"] == 30.0


def test_busiest_rank_in_month_ranks_locations_within_the_same_month_only(spark):
    from pipelines.gold.gold_monthly_traffic_summary import build_gold_monthly_traffic_summary

    fact_df = spark.createDataFrame(
        [
            (1, 20240101, 1, 100, 100, "auto"),  # location 1, Jan: busiest
            (2, 20240101, 2, 10, 10, "auto"),  # location 2, Jan: least busy
            (3, 20240201, 3, 5, 5, "auto"),  # location 3, Feb: only location that month -> rank 1
        ],
        _fact_cols(),
    )
    dim_date_df = spark.createDataFrame(
        [(20240101, 2024, 1), (20240201, 2024, 2)], _dim_date_cols()
    )

    result = build_gold_monthly_traffic_summary(fact_df, dim_date_df)
    ranks = {(r["location_key"], r["year"], r["month"]): r["busiest_rank_in_month"] for r in result.collect()}

    assert ranks[(1, 2024, 1)] == 1
    assert ranks[(2, 2024, 1)] == 2
    assert ranks[(3, 2024, 2)] == 1  # separate partition -- Feb doesn't compete against Jan
