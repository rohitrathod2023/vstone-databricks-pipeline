"""Unit tests for pipelines.gold.fact_traffic_counts -- join correctness
(no fan-out, no silent drops) and surrogate-key resolution.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_fact_traffic_counts.py -v
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

    session = SparkSession.builder.master("local[2]").appName("vstone-fact-traffic-counts-tests").getOrCreate()
    yield session
    session.stop()


def _dim_location_df(spark):
    return spark.createDataFrame([(1, 1, 38.9, -0.5), (2, 2, 38.91, -0.51)], ["location_key", "location", "lat", "lon"])


def _dim_date_df(spark):
    return spark.createDataFrame([(20240101, "2024-01-01"), (20240102, "2024-01-02")], ["date_key", "full_date"])


def test_row_count_matches_input_exactly_no_fan_out(spark):
    from pipelines.gold.fact_traffic_counts import build_fact_traffic_counts

    traffic_df = spark.createDataFrame(
        [
            (100, 1, 5, 3, "2024-01-01T10:00:00", "copyinto"),
            (101, 2, 6, 4, "2024-01-02T11:00:00", "dlt"),
        ],
        ["id", "location", "enter", "exit", "date", "source_technique"],
    )
    result = build_fact_traffic_counts(traffic_df, _dim_location_df(spark), _dim_date_df(spark))

    assert result.count() == 2


def test_schema_matches_expected_shape(spark):
    from pipelines.gold.fact_traffic_counts import build_fact_traffic_counts

    traffic_df = spark.createDataFrame(
        [(100, 1, 5, 3, "2024-01-01T10:00:00", "copyinto")],
        ["id", "location", "enter", "exit", "date", "source_technique"],
    )
    result = build_fact_traffic_counts(traffic_df, _dim_location_df(spark), _dim_date_df(spark))

    expected_columns = {
        "location_key", "date_key", "id", "enter", "exit", "source_technique",
        "load_dt", "source_format", "source_file", "run_id",
    }
    assert set(result.columns) == expected_columns


def test_location_key_and_date_key_resolve_correctly_when_matched(spark):
    from pipelines.gold.fact_traffic_counts import build_fact_traffic_counts

    traffic_df = spark.createDataFrame(
        [(100, 2, 5, 3, "2024-01-02T10:00:00", "copyinto")],
        ["id", "location", "enter", "exit", "date", "source_technique"],
    )
    row = build_fact_traffic_counts(traffic_df, _dim_location_df(spark), _dim_date_df(spark)).collect()[0]

    assert row["location_key"] == 2
    assert row["date_key"] == 20240102


def test_an_unresolvable_location_produces_null_key_not_a_dropped_row(spark):
    """Proves the LEFT join choice: an unmatched location must surface as a
    NULL location_key, not silently vanish -- an inner join would make a
    row-count mismatch impossible to detect."""
    from pipelines.gold.fact_traffic_counts import build_fact_traffic_counts

    traffic_df = spark.createDataFrame(
        [(100, 999, 5, 3, "2024-01-01T10:00:00", "copyinto")],
        ["id", "location", "enter", "exit", "date", "source_technique"],
    )
    result = build_fact_traffic_counts(traffic_df, _dim_location_df(spark), _dim_date_df(spark))

    assert result.count() == 1
    assert result.collect()[0]["location_key"] is None
