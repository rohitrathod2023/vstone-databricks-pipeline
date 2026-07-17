"""Unit tests for pipelines.silver.quarantine -- the shared rejection_reason/
valid_rows/rejected_rows split reused across every Silver table with
validation rules.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_silver_quarantine.py -v
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

    session = SparkSession.builder.master("local[2]").appName("vstone-silver-quarantine-tests").getOrCreate()
    yield session
    session.stop()


_RULES = {
    "non_negative": ("n < 0", "non_negative: n is below 0"),
    "within_hundred": ("n < -100 OR n > 100", "within_hundred: n is outside [-100, 100]"),
}


def test_a_row_passing_every_rule_gets_a_null_reason(spark):
    from pipelines.silver.quarantine import build_rejection_reason_expr

    df = spark.createDataFrame([(5,)], ["n"])
    result = df.withColumn("rejection_reason", build_rejection_reason_expr(_RULES)).collect()[0]

    assert result["rejection_reason"] is None


def test_a_row_failing_one_rule_names_that_rule(spark):
    from pipelines.silver.quarantine import build_rejection_reason_expr

    df = spark.createDataFrame([(-1,)], ["n"])
    result = df.withColumn("rejection_reason", build_rejection_reason_expr(_RULES)).collect()[0]

    assert result["rejection_reason"] == "non_negative: n is below 0"


def test_a_row_failing_both_rules_names_both(spark):
    """A row can fail multiple rules at once -- the combined reason must
    name every one, not just the first match."""
    from pipelines.silver.quarantine import build_rejection_reason_expr

    df = spark.createDataFrame([(-200,)], ["n"])
    result = df.withColumn("rejection_reason", build_rejection_reason_expr(_RULES)).collect()[0]

    assert result["rejection_reason"] == "non_negative: n is below 0; within_hundred: n is outside [-100, 100]"


def test_valid_rows_drops_rejection_reason_and_keeps_only_passing_rows(spark):
    from pipelines.silver.quarantine import build_rejection_reason_expr, valid_rows

    df = spark.createDataFrame([(5,), (-1,)], ["n"])
    checked = df.withColumn("rejection_reason", build_rejection_reason_expr(_RULES))
    result = valid_rows(checked).collect()

    assert [r["n"] for r in result] == [5]
    assert "rejection_reason" not in valid_rows(checked).columns


def test_rejected_rows_keeps_rejection_reason_and_only_failing_rows(spark):
    from pipelines.silver.quarantine import build_rejection_reason_expr, rejected_rows

    df = spark.createDataFrame([(5,), (-1,)], ["n"])
    checked = df.withColumn("rejection_reason", build_rejection_reason_expr(_RULES))
    result = rejected_rows(checked).collect()

    assert len(result) == 1
    assert result[0]["n"] == -1
    assert result[0]["rejection_reason"] == "non_negative: n is below 0"
