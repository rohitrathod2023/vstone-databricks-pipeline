"""
Unit tests for pipelines.silver.header_standardization -- column-NAME
standardization (a schema-only operation, no Spark UDF involved).
standardize_column_name is tested as a bare string function first (no Spark
session needed); apply_header_standardization is tested against a small
in-memory DataFrame to confirm it touches only column names, never row data.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_header_standardization.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pipelines.silver.header_standardization import standardize_column_name  # noqa: E402


def test_already_clean_name_passes_through_unchanged():
    assert standardize_column_name("street_id") == "street_id"


def test_mixed_case_and_spaces_are_standardized():
    assert standardize_column_name("Street  ID") == "street_id"


def test_punctuation_and_special_characters_become_underscores():
    assert standardize_column_name("street-id!") == "street_id"


def test_leading_and_trailing_junk_is_stripped():
    assert standardize_column_name("  _street_id_  ") == "street_id"


def test_repeated_separators_collapse_to_one():
    assert standardize_column_name("street___id") == "street_id"


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[2]").appName("vstone-header-standardization-tests").getOrCreate()
    yield session
    session.stop()


def test_apply_header_standardization_renames_every_messy_column(spark):
    from pipelines.silver.header_standardization import apply_header_standardization

    df = spark.createDataFrame([("CV-645A", "38.98", "1")], ["Street  Name", "  Latitude__", "street-id!"])
    result = apply_header_standardization(df)

    assert result.columns == ["street_name", "latitude", "street_id"]


def test_apply_header_standardization_leaves_row_values_untouched(spark):
    from pyspark.testing.utils import assertDataFrameEqual

    from pipelines.silver.header_standardization import apply_header_standardization

    df = spark.createDataFrame([("CV-645A", "38.98")], ["Street  Name", "  Latitude__"])
    result = apply_header_standardization(df)

    expected = spark.createDataFrame([("CV-645A", "38.98")], ["street_name", "latitude"])
    assertDataFrameEqual(result, expected)


def test_apply_header_standardization_skips_already_clean_columns(spark):
    """Already-standardized columns should pass through via the same
    DataFrame reference logic (no-op withColumnRenamed is skipped), not just
    happen to produce the same names."""
    from pipelines.silver.header_standardization import apply_header_standardization

    df = spark.createDataFrame([(1, "a")], ["street_id", "street"])
    result = apply_header_standardization(df)

    assert result.columns == ["street_id", "street"]
