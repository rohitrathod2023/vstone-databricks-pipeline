"""Unit tests for pipelines.silver.streets -- typing + header/value
standardization + quarantine split. Both quarantine rules are defensive
insurance (Phase 1 profiling found all 36 real rows clean on both), so this
uses synthetic bad rows to prove the rules actually fire.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_silver_streets.py -v
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

    session = SparkSession.builder.master("local[2]").appName("vstone-silver-streets-tests").getOrCreate()
    yield session
    session.stop()


def _cols():
    return [
        "street", "long", "latitude", "longitude", "dangerous", "street_id",
        "load_dt", "source_format", "source_file", "run_id",
    ]


# Fixed Bronze audit values appended to every synthetic row below -- proves
# build_checked_streets carries them through unchanged rather than
# regenerating (see streets.py's build_checked_streets Notes).
_AUDIT_VALUES = (datetime(2024, 1, 1, 12, 0, 0), "csv", "streets_list.csv", "test-run-id")


def test_valid_output_schema_matches_silver_streets_schema_exactly(spark):
    from pyspark.testing.utils import assertSchemaEqual

    from config.silver_schemas import SILVER_STREETS_SCHEMA
    from pipelines.silver.quarantine import valid_rows
    from pipelines.silver.streets import build_checked_streets

    df = spark.createDataFrame([("CV-645A", "574", "38.98492", "-0.538044", "0.5", "1") + _AUDIT_VALUES], _cols())
    valid = valid_rows(build_checked_streets(df))

    assertSchemaEqual(valid.schema, SILVER_STREETS_SCHEMA)


def test_rejected_output_schema_matches_silver_streets_rejected_schema(spark):
    from pyspark.testing.utils import assertSchemaEqual

    from config.silver_schemas import SILVER_STREETS_REJECTED_SCHEMA
    from pipelines.silver.quarantine import rejected_rows
    from pipelines.silver.streets import build_checked_streets

    df = spark.createDataFrame([("Bad Street", "100", "0.0", "0.0", "0.5", "2") + _AUDIT_VALUES], _cols())
    rejected = rejected_rows(build_checked_streets(df))

    assertSchemaEqual(rejected.schema, SILVER_STREETS_REJECTED_SCHEMA)


def test_long_is_cast_to_int_and_not_confused_with_longitude(spark):
    """`long` is street length in meters -- confirm it casts to the length
    value, not longitude's value."""
    from pipelines.silver.quarantine import valid_rows
    from pipelines.silver.streets import build_checked_streets

    df = spark.createDataFrame([("CV-645A", "574", "38.98492", "-0.538044", "0.5", "1") + _AUDIT_VALUES], _cols())
    row = valid_rows(build_checked_streets(df)).collect()[0]

    assert row["long"] == 574
    assert row["longitude"] == -0.538044


def test_street_name_whitespace_is_normalized(spark):
    from pipelines.silver.quarantine import valid_rows
    from pipelines.silver.streets import build_checked_streets

    df = spark.createDataFrame(
        [("  Corts   Valencianes  1A  ", "194", "38.985471", "-0.536866", "0.7", "3") + _AUDIT_VALUES], _cols()
    )
    row = valid_rows(build_checked_streets(df)).collect()[0]

    assert row["street"] == "Corts Valencianes 1A"


def test_valid_rows_are_not_quarantined(spark):
    from pipelines.silver.quarantine import rejected_rows, valid_rows
    from pipelines.silver.streets import build_checked_streets

    df = spark.createDataFrame([("CV-645A", "574", "38.98492", "-0.538044", "0.5", "1") + _AUDIT_VALUES], _cols())
    checked = build_checked_streets(df)

    assert valid_rows(checked).count() == 1
    assert rejected_rows(checked).count() == 0


def test_bad_coordinates_alone_is_quarantined_with_that_reason(spark):
    from pipelines.silver.quarantine import rejected_rows
    from pipelines.silver.streets import build_checked_streets

    df = spark.createDataFrame([("Bad Coords", "100", "0.0", "0.0", "0.5", "2") + _AUDIT_VALUES], _cols())
    rejected_row = rejected_rows(build_checked_streets(df)).collect()[0]

    assert rejected_row["rejection_reason"] == "invalid_coordinates: latitude and longitude are both 0"


def test_out_of_range_dangerous_alone_is_quarantined_with_that_reason(spark):
    from pipelines.silver.quarantine import rejected_rows
    from pipelines.silver.streets import build_checked_streets

    df = spark.createDataFrame([("Too Dangerous", "300", "38.99", "-0.53", "1.5", "5") + _AUDIT_VALUES], _cols())
    rejected_row = rejected_rows(build_checked_streets(df)).collect()[0]

    assert rejected_row["rejection_reason"] == "dangerous_out_of_range: dangerous score is outside [0, 1]"


def test_a_row_failing_both_rules_names_both_reasons(spark):
    """A row can fail both validation rules at once -- confirm the combined
    reason names both, not just whichever rule was checked first."""
    from pipelines.silver.quarantine import rejected_rows
    from pipelines.silver.streets import build_checked_streets

    df = spark.createDataFrame([("Both Bad", "100", "0.0", "0.0", "-0.2", "6") + _AUDIT_VALUES], _cols())
    rejected_row = rejected_rows(build_checked_streets(df)).collect()[0]

    assert rejected_row["rejection_reason"] == (
        "invalid_coordinates: latitude and longitude are both 0; "
        "dangerous_out_of_range: dangerous score is outside [0, 1]"
    )
