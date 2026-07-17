"""Unit tests for pipelines.silver.environment -- typing + dedup insurance +
quarantine split. raining_out_of_range (raining > 100 only) reacts to a real
found defect (~44,004 real rows exceed 100); negative raining is the normal
"not currently raining" baseline (98.6% of real rows), not a defect. The
other three rules are insurance only.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_silver_environment.py -v
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

    session = SparkSession.builder.master("local[2]").appName("vstone-silver-environment-tests").getOrCreate()
    yield session
    session.stop()


def _cols():
    return ["noise", "pollution", "date", "light", "raining", "street_id"]


def _row(noise="10.5", pollution="8.2", date="2024-01-01T10:00:00", light="30.0", raining="0.0", street_id="1"):
    return (noise, pollution, date, light, raining, street_id)


def test_valid_output_schema_matches_silver_environment_schema_exactly(spark):
    from pyspark.testing.utils import assertSchemaEqual

    from config.silver_schemas import SILVER_ENVIRONMENT_SCHEMA
    from pipelines.silver.environment import build_checked_environment
    from pipelines.silver.quarantine import valid_rows

    df = spark.createDataFrame([_row()], _cols())
    valid = valid_rows(build_checked_environment(df))

    assertSchemaEqual(valid.schema, SILVER_ENVIRONMENT_SCHEMA)


def test_rejected_output_schema_matches_silver_environment_rejected_schema(spark):
    from pyspark.testing.utils import assertSchemaEqual

    from config.silver_schemas import SILVER_ENVIRONMENT_REJECTED_SCHEMA
    from pipelines.silver.environment import build_checked_environment
    from pipelines.silver.quarantine import rejected_rows

    df = spark.createDataFrame([_row(raining="150.0")], _cols())
    rejected = rejected_rows(build_checked_environment(df))

    assertSchemaEqual(rejected.schema, SILVER_ENVIRONMENT_REJECTED_SCHEMA)


def test_a_row_duplicated_on_street_id_and_date_collapses_to_one(spark):
    """Defensive insurance -- a real GROUP BY ... HAVING COUNT(*) > 1 query
    found zero duplicate groups today, so this proves the dedup logic
    itself works, not that it fixes a real problem."""
    from pipelines.silver.environment import build_checked_environment
    from pipelines.silver.quarantine import valid_rows

    duplicate = _row(street_id="5", date="2024-03-01T09:00:00")
    df = spark.createDataFrame([duplicate, duplicate], _cols())
    valid = valid_rows(build_checked_environment(df))

    assert valid.count() == 1


def test_valid_rows_are_not_quarantined(spark):
    from pipelines.silver.environment import build_checked_environment
    from pipelines.silver.quarantine import rejected_rows, valid_rows

    df = spark.createDataFrame([_row()], _cols())
    checked = build_checked_environment(df)

    assert valid_rows(checked).count() == 1
    assert rejected_rows(checked).count() == 0


def test_negative_raining_is_valid_not_quarantined(spark):
    """A negative raining value is the normal "not currently raining"
    baseline (98.6% of real rows fall between -1 and 1) -- built and run
    for real with an earlier `raining < 0` rule, this rejected 49.3% of the
    table, which is what proved the original 0-100 assumption wrong."""
    from pipelines.silver.environment import build_checked_environment
    from pipelines.silver.quarantine import valid_rows

    df = spark.createDataFrame([_row(raining="-0.9999999834")], _cols())
    valid = valid_rows(build_checked_environment(df))

    assert valid.count() == 1


def test_raining_above_hundred_is_quarantined(spark):
    from pipelines.silver.environment import build_checked_environment
    from pipelines.silver.quarantine import rejected_rows

    df = spark.createDataFrame([_row(raining="100.9999")], _cols())
    rejected_row = rejected_rows(build_checked_environment(df)).collect()[0]

    assert rejected_row["rejection_reason"] == "raining_out_of_range: raining is above 100"


def test_negative_noise_is_quarantined(spark):
    from pipelines.silver.environment import build_checked_environment
    from pipelines.silver.quarantine import rejected_rows

    df = spark.createDataFrame([_row(noise="-1.0")], _cols())
    rejected_row = rejected_rows(build_checked_environment(df)).collect()[0]

    assert rejected_row["rejection_reason"] == "negative_noise: noise is negative"


def test_negative_pollution_is_quarantined(spark):
    from pipelines.silver.environment import build_checked_environment
    from pipelines.silver.quarantine import rejected_rows

    df = spark.createDataFrame([_row(pollution="-1.0")], _cols())
    rejected_row = rejected_rows(build_checked_environment(df)).collect()[0]

    assert rejected_row["rejection_reason"] == "negative_pollution: pollution is negative"


def test_negative_light_is_quarantined(spark):
    from pipelines.silver.environment import build_checked_environment
    from pipelines.silver.quarantine import rejected_rows

    df = spark.createDataFrame([_row(light="-1.0")], _cols())
    rejected_row = rejected_rows(build_checked_environment(df)).collect()[0]

    assert rejected_row["rejection_reason"] == "negative_light: light is negative"


def test_a_row_failing_multiple_rules_names_all_of_them(spark):
    from pipelines.silver.environment import build_checked_environment
    from pipelines.silver.quarantine import rejected_rows

    df = spark.createDataFrame([_row(noise="-1.0", pollution="-2.0", raining="200.0")], _cols())
    rejected_row = rejected_rows(build_checked_environment(df)).collect()[0]

    assert rejected_row["rejection_reason"] == (
        "raining_out_of_range: raining is above 100; "
        "negative_noise: noise is negative; "
        "negative_pollution: pollution is negative"
    )
