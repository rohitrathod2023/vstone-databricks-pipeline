"""Unit tests for pipelines.silver.traffic -- schema-equality precondition,
UNION ALL + source_technique tagging, dedup insurance, and quarantine split.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_silver_traffic.py -v
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

    session = SparkSession.builder.master("local[2]").appName("vstone-silver-traffic-tests").getOrCreate()
    yield session
    session.stop()


def _cols():
    return ["enter", "exit", "date", "id", "location"]


def _row(enter="5", exit_="3", date="2024-01-01T10:00:00", id_="100", location="1"):
    return (enter, exit_, date, id_, location)


def test_assert_schemas_match_passes_for_identical_columns(spark):
    from pipelines.silver.traffic import assert_schemas_match

    dfs = [spark.createDataFrame([_row()], _cols()) for _ in range(4)]

    assert_schemas_match(dfs)  # should not raise


def test_assert_schemas_match_raises_on_a_real_mismatch(spark):
    """The PySpark/XML path's sanitize_column_name() is the one most likely
    to produce a different column name than the other 3 techniques -- this
    confirms a mismatch is caught, not silently coerced."""
    from pipelines.silver.traffic import assert_schemas_match

    good_dfs = [spark.createDataFrame([_row()], _cols()) for _ in range(3)]
    mismatched_df = spark.createDataFrame([_row()], ["enter", "exit", "date_", "id", "location"])

    with pytest.raises(ValueError):
        assert_schemas_match(good_dfs + [mismatched_df])


def test_valid_output_schema_matches_silver_traffic_schema_exactly(spark):
    from pyspark.testing.utils import assertSchemaEqual

    from config.silver_schemas import SILVER_TRAFFIC_SCHEMA
    from pipelines.silver.quarantine import valid_rows
    from pipelines.silver.traffic import build_checked_traffic

    dfs = [
        spark.createDataFrame([_row(id_=str(100 + i), location=str(i + 1))], _cols())
        for i in range(4)
    ]
    valid = valid_rows(build_checked_traffic(dfs))

    assertSchemaEqual(valid.schema, SILVER_TRAFFIC_SCHEMA)


def test_rejected_output_schema_matches_silver_traffic_rejected_schema(spark):
    from pyspark.testing.utils import assertSchemaEqual

    from config.silver_schemas import SILVER_TRAFFIC_REJECTED_SCHEMA
    from pipelines.silver.quarantine import rejected_rows
    from pipelines.silver.traffic import build_checked_traffic

    dfs = [spark.createDataFrame([_row(enter="-1", id_=str(100 + i), location=str(i + 1))], _cols()) for i in range(4)]
    rejected = rejected_rows(build_checked_traffic(dfs))

    assertSchemaEqual(rejected.schema, SILVER_TRAFFIC_REJECTED_SCHEMA)


def test_each_branch_is_tagged_with_its_own_source_technique(spark):
    from pipelines.silver.quarantine import valid_rows
    from pipelines.silver.traffic import SOURCE_TECHNIQUES, build_checked_traffic

    dfs = [
        spark.createDataFrame([_row(id_=str(100 + i), location=str(i + 1))], _cols())
        for i in range(4)
    ]
    rows = {r["id"]: r["source_technique"] for r in valid_rows(build_checked_traffic(dfs)).collect()}

    for i, technique in enumerate(SOURCE_TECHNIQUES):
        assert rows[100 + i] == technique


def test_a_row_duplicated_across_two_techniques_collapses_to_one(spark):
    """Defensive insurance -- Phase 1 profiling found zero duplicate groups
    today, so this proves the dedup logic itself works, not that it fixes a
    real problem."""
    from pipelines.silver.quarantine import valid_rows
    from pipelines.silver.traffic import build_checked_traffic

    duplicate = _row(id_="500", location="9", date="2024-06-01T08:00:00")
    dfs = [
        spark.createDataFrame([duplicate], _cols()),  # copyinto
        spark.createDataFrame([duplicate], _cols()),  # dlt -- same id+location+date
        spark.createDataFrame([_row(id_="600", location="2")], _cols()),  # autoloader
        spark.createDataFrame([_row(id_="700", location="3")], _cols()),  # pyspark
    ]
    valid = valid_rows(build_checked_traffic(dfs))

    assert valid.filter("id = 500").count() == 1
    assert valid.count() == 3


def test_negative_enter_or_exit_is_quarantined(spark):
    from pipelines.silver.quarantine import rejected_rows
    from pipelines.silver.traffic import build_checked_traffic

    dfs = [
        spark.createDataFrame([_row(enter="-5", id_="1", location="1")], _cols()),
        spark.createDataFrame([_row(id_="2", location="2")], _cols()),
        spark.createDataFrame([_row(id_="3", location="3")], _cols()),
        spark.createDataFrame([_row(id_="4", location="4")], _cols()),
    ]
    rejected_row = rejected_rows(build_checked_traffic(dfs)).collect()[0]

    assert rejected_row["rejection_reason"] == "negative_count: enter or exit is negative"


def test_unparseable_date_is_quarantined(spark):
    from pipelines.silver.quarantine import rejected_rows
    from pipelines.silver.traffic import build_checked_traffic

    dfs = [
        spark.createDataFrame([_row(date="not-a-real-date", id_="1", location="1")], _cols()),
        spark.createDataFrame([_row(id_="2", location="2")], _cols()),
        spark.createDataFrame([_row(id_="3", location="3")], _cols()),
        spark.createDataFrame([_row(id_="4", location="4")], _cols()),
    ]
    rejected_row = rejected_rows(build_checked_traffic(dfs)).collect()[0]

    assert rejected_row["rejection_reason"] == "unparseable_date: date could not be parsed"


def test_a_row_failing_both_rules_names_both_reasons(spark):
    from pipelines.silver.quarantine import rejected_rows
    from pipelines.silver.traffic import build_checked_traffic

    dfs = [
        spark.createDataFrame([_row(enter="-1", date="garbage", id_="1", location="1")], _cols()),
        spark.createDataFrame([_row(id_="2", location="2")], _cols()),
        spark.createDataFrame([_row(id_="3", location="3")], _cols()),
        spark.createDataFrame([_row(id_="4", location="4")], _cols()),
    ]
    rejected_row = rejected_rows(build_checked_traffic(dfs)).collect()[0]

    assert rejected_row["rejection_reason"] == (
        "negative_count: enter or exit is negative; unparseable_date: date could not be parsed"
    )


def test_valid_rows_are_not_quarantined(spark):
    from pipelines.silver.quarantine import rejected_rows, valid_rows
    from pipelines.silver.traffic import build_checked_traffic

    dfs = [
        spark.createDataFrame([_row(id_=str(100 + i), location=str(i + 1))], _cols())
        for i in range(4)
    ]
    checked = build_checked_traffic(dfs)

    assert valid_rows(checked).count() == 4
    assert rejected_rows(checked).count() == 0
