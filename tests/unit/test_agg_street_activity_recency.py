"""Unit tests for pipelines.gold.agg_street_activity_recency -- the
churn-style metric adapted from the Day 7 brief's "customers with no
transaction in 6 months" example.

The real dataset has zero churn (confirmed live: all 36 streets report
gap-free through the very last observed day), so these tests use synthetic
gaps to prove the detection logic actually works, rather than relying on
real data that doesn't currently exercise the "churned" path.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_agg_street_activity_recency.py -v
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
        SparkSession.builder.master("local[2]").appName("vstone-agg-street-activity-recency-tests").getOrCreate()
    )
    yield session
    session.stop()


def _obs_df(spark, rows):
    from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

    schema = StructType(
        [
            StructField("street_key", IntegerType()),
            StructField("date_key", IntegerType()),
            StructField("noise", DoubleType()),
            StructField("enter", IntegerType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


def _dim_street_df(spark, rows=None):
    rows = rows or [
        (1, 10, "Main Street", 0.4, True),
        (2, 20, "Second Street", 0.6, True),
    ]
    return spark.createDataFrame(rows, ["street_key", "street_id", "street", "dangerous", "is_current"])


def _build(spark, rows, dim_street_df=None, churn_threshold_days=30):
    from pipelines.gold.agg_street_activity_recency import build_agg_street_activity_recency

    return build_agg_street_activity_recency(
        _obs_df(spark, rows), dim_street_df or _dim_street_df(spark), churn_threshold_days
    )


def test_schema_matches_expected_shape(spark):
    result = _build(spark, [(1, 20240101, 10.0, None)])

    expected_columns = {
        "street_key", "street_id", "street", "dangerous", "last_observation_date",
        "days_since_last_observation", "is_churned", "total_observation_days", "observation_count",
        "load_dt", "source_format", "source_file", "run_id",
    }
    assert set(result.columns) == expected_columns


def test_street_reporting_up_to_the_reference_date_is_not_churned(spark):
    """A street whose last reading IS the max date across the whole table
    has 0 days since last observation and is_churned=False."""
    result = _build(
        spark,
        [
            (1, 20240101, 10.0, None),
            (1, 20240301, 12.0, None),  # this street's (and the table's) latest reading
        ],
    )
    row = result.filter(result.street_key == 1).collect()[0]

    assert row["days_since_last_observation"] == 0
    assert row["is_churned"] is False
    assert str(row["last_observation_date"]) == "2024-03-01"


def test_street_with_a_real_gap_is_detected_as_churned(spark):
    """Regression/proof test: the real dataset has no gaps, so this
    synthetic case is what actually exercises the churn-detection logic.
    Street 2's last reading is 2024-01-01; the table's reference date
    (latest reading anywhere) is 2024-03-01 -- a 60-day gap, past the
    30-day threshold."""
    result = _build(
        spark,
        [
            (1, 20240301, 12.0, None),  # sets the table-wide reference date
            (2, 20240101, 8.0, None),  # street 2 goes quiet after this
        ],
    )
    row = result.filter(result.street_key == 2).collect()[0]

    assert row["days_since_last_observation"] == 60
    assert row["is_churned"] is True
    assert str(row["last_observation_date"]) == "2024-01-01"


def test_churn_threshold_is_configurable_not_hardcoded(spark):
    """A 20-day gap should NOT be churned at the default 30-day threshold,
    but SHOULD be churned if the caller passes a stricter 10-day threshold
    -- proves the threshold is a real parameter, not baked into the logic."""
    rows = [
        (1, 20240301, 12.0, None),
        (2, 20240210, 8.0, None),  # 20 days before the reference date
    ]
    default_result = _build(spark, rows, churn_threshold_days=30)
    strict_result = _build(spark, rows, churn_threshold_days=10)

    assert default_result.filter(default_result.street_key == 2).collect()[0]["is_churned"] is False
    assert strict_result.filter(strict_result.street_key == 2).collect()[0]["is_churned"] is True


def test_reference_date_spans_the_whole_fact_table_not_just_environmental_rows(spark):
    """The reference date ("as of when") must be the latest date across
    ALL branches (including traffic-only rows with no noise value), not
    just the latest environmental reading -- otherwise a street's own
    recency would be measured against the wrong baseline whenever traffic
    data is more recent than environmental data."""
    result = _build(
        spark,
        [
            (1, 20240101, 10.0, None),  # street 1's only environmental reading
            (None, 20240301, None, 5),  # a later traffic-only row, no street_key, no noise
        ],
    )
    row = result.filter(result.street_key == 1).collect()[0]

    # Reference date must be 2024-03-01 (the traffic row's date), not 2024-01-01.
    assert row["days_since_last_observation"] == 60


def test_total_observation_days_and_observation_count(spark):
    result = _build(
        spark,
        [
            (1, 20240101, 10.0, None),
            (1, 20240101, 11.0, None),  # same day, second reading
            (1, 20240102, 12.0, None),
        ],
    )
    row = result.filter(result.street_key == 1).collect()[0]

    assert row["total_observation_days"] == 2  # distinct days
    assert row["observation_count"] == 3  # raw reading count
