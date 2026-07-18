"""Unit tests for pipelines.silver.telegram -- message normalization,
date+hour combined into event_timestamp, dedup insurance, and quarantine
split.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_silver_telegram.py -v
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

    session = SparkSession.builder.master("local[2]").appName("vstone-silver-telegram-tests").getOrCreate()
    yield session
    session.stop()


def _cols():
    return ["message", "date", "hour", "load_dt", "source_format", "source_file", "run_id"]


# Fixed Bronze audit values appended to every synthetic row -- proves
# build_checked_telegram carries them through unchanged rather than
# regenerating (see telegram.py's build_checked_telegram Notes).
_AUDIT_VALUES = (datetime(2024, 1, 1, 12, 0, 0), "csv", "telegram.csv", "test-run-id")


def _row(message="Traffic is bad today", date="15/07/2026", hour="14:30:00"):
    return (message, date, hour) + _AUDIT_VALUES


def test_valid_output_schema_matches_silver_telegram_schema_exactly(spark):
    from pyspark.testing.utils import assertSchemaEqual

    from config.silver_schemas import SILVER_TELEGRAM_SCHEMA
    from pipelines.silver.quarantine import valid_rows
    from pipelines.silver.telegram import build_checked_telegram

    df = spark.createDataFrame([_row()], _cols())
    valid = valid_rows(build_checked_telegram(df))

    assertSchemaEqual(valid.schema, SILVER_TELEGRAM_SCHEMA)


def test_rejected_output_schema_matches_silver_telegram_rejected_schema(spark):
    from pyspark.testing.utils import assertSchemaEqual

    from config.silver_schemas import SILVER_TELEGRAM_REJECTED_SCHEMA
    from pipelines.silver.quarantine import rejected_rows
    from pipelines.silver.telegram import build_checked_telegram

    df = spark.createDataFrame([_row(date="not-a-date")], _cols())
    rejected = rejected_rows(build_checked_telegram(df))

    assertSchemaEqual(rejected.schema, SILVER_TELEGRAM_REJECTED_SCHEMA)


def test_date_and_hour_combine_into_the_correct_event_timestamp(spark):
    from pipelines.silver.quarantine import valid_rows
    from pipelines.silver.telegram import build_checked_telegram

    df = spark.createDataFrame([_row(date="15/07/2026", hour="14:30:00")], _cols())
    row = valid_rows(build_checked_telegram(df)).collect()[0]

    assert str(row["event_timestamp"]) == "2026-07-15 14:30:00"


def test_message_whitespace_is_normalized(spark):
    """Reuses the whitespace-normalization UDF originally built for
    streets_list.street -- brought into scope for message per Rohit's
    July 16 decision."""
    from pipelines.silver.quarantine import valid_rows
    from pipelines.silver.telegram import build_checked_telegram

    df = spark.createDataFrame([_row(message="  The car   does not respect  the rule  ")], _cols())
    row = valid_rows(build_checked_telegram(df)).collect()[0]

    assert row["message"] == "The car does not respect the rule"


def test_a_row_duplicated_on_message_date_and_hour_collapses_to_one(spark):
    """Defensive insurance -- a real GROUP BY message, date, hour
    HAVING COUNT(*) > 1 query against Bronze confirmed zero duplicate
    groups today, so this proves the dedup logic itself works, not that it
    fixes a real problem."""
    from pipelines.silver.quarantine import valid_rows
    from pipelines.silver.telegram import build_checked_telegram

    duplicate = _row(message="Same report twice", date="01/03/2026", hour="09:00:00")
    df = spark.createDataFrame([duplicate, duplicate], _cols())
    valid = valid_rows(build_checked_telegram(df))

    assert valid.count() == 1


def test_valid_rows_are_not_quarantined(spark):
    from pipelines.silver.quarantine import rejected_rows, valid_rows
    from pipelines.silver.telegram import build_checked_telegram

    df = spark.createDataFrame([_row()], _cols())
    checked = build_checked_telegram(df)

    assert valid_rows(checked).count() == 1
    assert rejected_rows(checked).count() == 0


def test_unparseable_date_hour_combination_is_quarantined(spark):
    from pipelines.silver.quarantine import rejected_rows
    from pipelines.silver.telegram import build_checked_telegram

    df = spark.createDataFrame([_row(date="not-a-date", hour="not-a-time")], _cols())
    rejected_row = rejected_rows(build_checked_telegram(df)).collect()[0]

    assert rejected_row["rejection_reason"] == (
        "unparseable_event_timestamp: date/hour could not be combined into a valid timestamp"
    )
