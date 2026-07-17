"""Unit tests for pipelines.gold.gold_street_risk_summary -- especially the
month-over-month risk_changed_flag/risk_direction logic, which only becomes
meaningful once Dim_Street accumulates real SCD2 history across months.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_gold_street_risk_summary.py -v
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

    session = SparkSession.builder.master("local[2]").appName("vstone-gold-street-risk-summary-tests").getOrCreate()
    yield session
    session.stop()


def _fact_cols():
    return ["street_key", "date_key", "noise", "pollution", "light", "raining"]


def _dim_street_cols():
    return ["street_key", "street_id", "dangerous"]


def _dim_date_cols():
    return ["date_key", "year", "month"]


def test_schema_matches_expected_shape(spark):
    from pipelines.gold.gold_street_risk_summary import build_gold_street_risk_summary

    fact_df = spark.createDataFrame([(1, 20240101, 10.0, 5.0, 20.0, 0.3)], _fact_cols())
    dim_street_df = spark.createDataFrame([(1, 1, 0.4)], _dim_street_cols())
    dim_date_df = spark.createDataFrame([(20240101, 2024, 1)], _dim_date_cols())

    result = build_gold_street_risk_summary(fact_df, dim_street_df, dim_date_df)

    expected_columns = {
        "street_id", "year", "month", "avg_noise", "avg_pollution", "avg_light", "rain_event_count",
        "dangerous_rating_this_month", "dangerous_rating_prior_month", "risk_changed_flag", "risk_direction",
        "load_dt", "source_format", "source_file", "run_id",
    }
    assert set(result.columns) == expected_columns


def test_averages_and_rain_event_count_computed_within_the_same_street_and_month(spark):
    from pipelines.gold.gold_street_risk_summary import build_gold_street_risk_summary

    fact_df = spark.createDataFrame(
        [
            (1, 20240101, 10.0, 4.0, 20.0, 50.0),  # not a rain event (<= 100)
            (1, 20240102, 20.0, 6.0, 30.0, 150.0),  # rain event (> 100)
        ],
        _fact_cols(),
    )
    dim_street_df = spark.createDataFrame([(1, 1, 0.4)], _dim_street_cols())
    dim_date_df = spark.createDataFrame(
        [(20240101, 2024, 1), (20240102, 2024, 1)], _dim_date_cols()
    )

    result = build_gold_street_risk_summary(fact_df, dim_street_df, dim_date_df)
    row = result.collect()[0]

    assert row["avg_noise"] == 15.0
    assert row["avg_pollution"] == 5.0
    assert row["avg_light"] == 25.0
    assert row["rain_event_count"] == 1


def test_dangerous_rating_this_month_reflects_the_resolved_streets_dangerous_value(spark):
    from pipelines.gold.gold_street_risk_summary import build_gold_street_risk_summary

    fact_df = spark.createDataFrame([(1, 20240101, 10.0, 5.0, 20.0, 0.3)], _fact_cols())
    dim_street_df = spark.createDataFrame([(1, 1, 0.7)], _dim_street_cols())
    dim_date_df = spark.createDataFrame([(20240101, 2024, 1)], _dim_date_cols())

    result = build_gold_street_risk_summary(fact_df, dim_street_df, dim_date_df)
    row = result.collect()[0]

    assert row["dangerous_rating_this_month"] == 0.7


def test_first_month_has_no_prior_and_is_reported_as_stable_not_changed(spark):
    """No prior month to compare against -- risk_changed_flag must be False
    and risk_direction 'stable', matching the documented first-build
    behavior (no Dim_Street history to compare across months yet)."""
    from pipelines.gold.gold_street_risk_summary import build_gold_street_risk_summary

    fact_df = spark.createDataFrame([(1, 20240101, 10.0, 5.0, 20.0, 0.3)], _fact_cols())
    dim_street_df = spark.createDataFrame([(1, 1, 0.7)], _dim_street_cols())
    dim_date_df = spark.createDataFrame([(20240101, 2024, 1)], _dim_date_cols())

    result = build_gold_street_risk_summary(fact_df, dim_street_df, dim_date_df)
    row = result.collect()[0]

    assert row["dangerous_rating_prior_month"] is None
    assert row["risk_changed_flag"] is False
    assert row["risk_direction"] == "stable"


def test_risk_changed_flag_true_when_crossing_the_safe_dangerous_threshold_month_over_month(spark):
    """street_id=1 resolves to a different street_key (and dangerous value)
    in Feb than in Jan -- simulates a real SCD2 version change landing
    between two calendar months. Jan=0.3 (safe), Feb=0.8 (dangerous) should
    cross the 0.5 threshold and flag as changed/increased."""
    from pipelines.gold.gold_street_risk_summary import build_gold_street_risk_summary

    fact_df = spark.createDataFrame(
        [
            (10, 20240101, 10.0, 5.0, 20.0, 0.3),  # Jan, resolves to old version
            (11, 20240201, 12.0, 6.0, 21.0, 0.4),  # Feb, resolves to new version
        ],
        _fact_cols(),
    )
    dim_street_df = spark.createDataFrame(
        [(10, 1, 0.3), (11, 1, 0.8)], _dim_street_cols()
    )
    dim_date_df = spark.createDataFrame(
        [(20240101, 2024, 1), (20240201, 2024, 2)], _dim_date_cols()
    )

    result = build_gold_street_risk_summary(fact_df, dim_street_df, dim_date_df)
    rows = {r["month"]: r for r in result.collect()}

    assert rows[1]["risk_changed_flag"] is False
    assert rows[1]["risk_direction"] == "stable"
    assert rows[2]["dangerous_rating_prior_month"] == 0.3
    assert rows[2]["risk_changed_flag"] is True
    assert rows[2]["risk_direction"] == "increased"


def test_risk_direction_decreased_when_dangerous_rating_drops_without_crossing_threshold(spark):
    """A numeric drop that doesn't cross 0.5 should still report
    risk_direction='decreased' even though risk_changed_flag stays False --
    the two signals answer different questions (class change vs. trend)."""
    from pipelines.gold.gold_street_risk_summary import build_gold_street_risk_summary

    fact_df = spark.createDataFrame(
        [
            (10, 20240101, 10.0, 5.0, 20.0, 0.3),
            (11, 20240201, 12.0, 6.0, 21.0, 0.4),
        ],
        _fact_cols(),
    )
    dim_street_df = spark.createDataFrame(
        [(10, 1, 0.8), (11, 1, 0.6)], _dim_street_cols()
    )
    dim_date_df = spark.createDataFrame(
        [(20240101, 2024, 1), (20240201, 2024, 2)], _dim_date_cols()
    )

    result = build_gold_street_risk_summary(fact_df, dim_street_df, dim_date_df)
    rows = {r["month"]: r for r in result.collect()}

    assert rows[2]["risk_changed_flag"] is False  # 0.8 and 0.6 both >= 0.5
    assert rows[2]["risk_direction"] == "decreased"
