"""Unit tests for pipelines.gold.agg_monthly_street_summary -- monthly
environmental rollup per street. ALTERNATIVE DESIGN, pending trainer review:
no observation_type column -- environmental rows are identified via
noise.isNotNull() instead (see
docs/fact_table_without_discriminator_alternative.md). Also covers the real
bug found in review: days_with_rain must count distinct rainy days, not raw
sensor readings.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_agg_monthly_street_summary.py -v
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
        SparkSession.builder.master("local[2]").appName("vstone-agg-monthly-street-summary-tests").getOrCreate()
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
            StructField("pollution", DoubleType()),
            StructField("light", DoubleType()),
            StructField("raining", DoubleType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


def _dim_street_df(spark):
    return spark.createDataFrame(
        [(1, 10, "Main Street", 0.4, True)], ["street_key", "street_id", "street", "dangerous", "is_current"]
    )


def _dim_date_df(spark, rows=None):
    rows = rows or [
        (20240101, 2024, 1),
        (20240102, 2024, 1),
        (20240103, 2024, 1),
    ]
    return spark.createDataFrame(rows, ["date_key", "year", "month"])


def _build(spark, rows, dim_date_df=None):
    from pipelines.gold.agg_monthly_street_summary import build_agg_monthly_street_summary

    return build_agg_monthly_street_summary(
        _obs_df(spark, rows), _dim_street_df(spark), dim_date_df or _dim_date_df(spark)
    )


def test_schema_matches_expected_shape(spark):
    result = _build(spark, [(1, 20240101, 10.0, 5.0, 20.0, 0.3)])

    expected_columns = {
        "street_key", "street_id", "street", "dangerous", "year", "month", "avg_noise", "avg_pollution",
        "avg_light", "max_noise", "max_pollution", "max_light", "days_with_rain", "observation_count",
        "observation_days", "load_dt", "source_format", "source_file", "run_id",
    }
    assert set(result.columns) == expected_columns


def test_days_with_rain_counts_distinct_days_not_raw_readings(spark):
    """Regression test for the real bug: F.sum(F.when(raining > 0, 1)) counts
    individual sensor readings, not days -- 500 rainy readings on one day
    would read as "500 days of rain." Must be countDistinct(date_key)."""
    result = _build(
        spark,
        [
            # 3 rainy readings, all on the same day (20240101).
            (1, 20240101, 10.0, 5.0, 20.0, 5.0),
            (1, 20240101, 10.0, 5.0, 20.0, 8.0),
            (1, 20240101, 10.0, 5.0, 20.0, 2.0),
            # 1 non-rainy day.
            (1, 20240102, 10.0, 5.0, 20.0, -1.0),
        ],
    )
    row = result.collect()[0]

    assert row["days_with_rain"] == 1  # not 3
    assert row["observation_count"] == 4
    assert row["observation_days"] == 2


def test_only_rows_with_noise_populated_are_aggregated(spark):
    """No observation_type column on this branch -- environmental rows are
    identified via noise.isNotNull()."""
    result = _build(
        spark,
        [
            (1, 20240101, 10.0, 5.0, 20.0, 0.3),
            (1, 20240101, None, None, None, None),
        ],
    )

    assert result.count() == 1
    assert result.collect()[0]["observation_count"] == 1
