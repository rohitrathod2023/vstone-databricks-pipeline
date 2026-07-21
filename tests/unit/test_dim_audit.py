"""Unit tests for pipelines.gold.dim_audit -- deduping audit combinations
across silver_traffic/environment/telegram and assigning stable surrogate keys.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_dim_audit.py -v
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

    session = SparkSession.builder.master("local[2]").appName("vstone-dim-audit-tests").getOrCreate()
    yield session
    session.stop()


_AUDIT_COLS = ["load_dt", "source_format", "source_file", "run_id"]


def _empty_audit_df(spark):
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    schema = StructType(
        [
            StructField("load_dt", TimestampType()),
            StructField("source_format", StringType()),
            StructField("source_file", StringType()),
            StructField("run_id", StringType()),
        ]
    )
    return spark.createDataFrame([], schema)


def test_distinct_audit_combinations_are_deduped_across_tables(spark):
    from pipelines.gold.dim_audit import build_dim_audit

    shared = (datetime(2024, 1, 1, 12, 0, 0), "delta", "shared_file", "run-1")
    traffic_df = spark.createDataFrame([shared], _AUDIT_COLS)
    environment_df = spark.createDataFrame([shared], _AUDIT_COLS)
    telegram_df = spark.createDataFrame(
        [(datetime(2024, 1, 1, 14, 0, 0), "csv", "telegram.csv", "run-1")], _AUDIT_COLS
    )

    result = build_dim_audit(traffic_df, environment_df, telegram_df)

    # The identical combination shared by traffic/environment collapses to
    # one row -- only 2 distinct combinations across all 3 inputs.
    assert result.count() == 2


def test_audit_key_is_unique_per_distinct_combination(spark):
    from pipelines.gold.dim_audit import build_dim_audit

    traffic_df = spark.createDataFrame(
        [
            (datetime(2024, 1, 1, 10, 0, 0), "delta", "chunk1.csv", "run-1"),
            (datetime(2024, 1, 1, 11, 0, 0), "json", "chunk3.json", "run-1"),
        ],
        _AUDIT_COLS,
    )
    environment_df = _empty_audit_df(spark)
    telegram_df = _empty_audit_df(spark)

    result = build_dim_audit(traffic_df, environment_df, telegram_df)
    keys = [r["audit_key"] for r in result.collect()]

    assert len(keys) == 2
    assert len(set(keys)) == 2


def test_schema_matches_expected_shape(spark):
    from pipelines.gold.dim_audit import build_dim_audit

    traffic_df = spark.createDataFrame([(datetime(2024, 1, 1, 10, 0, 0), "delta", "chunk1.csv", "run-1")], _AUDIT_COLS)  # noqa: E501
    environment_df = _empty_audit_df(spark)
    telegram_df = _empty_audit_df(spark)

    result = build_dim_audit(traffic_df, environment_df, telegram_df)

    expected_columns = {"audit_key", "load_dt", "source_format", "source_file", "run_id", "created_timestamp"}
    assert set(result.columns) == expected_columns
