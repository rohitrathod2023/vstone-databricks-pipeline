"""
Tests common.audit.add_audit_columns using the exact testing utilities the
brief calls out by name (assertDataFrameEqual, assertSchemaEqual), since this
function is shared by every layer (Bronze/Silver/Gold) — get it right once,
here, and every day's ingestion code inherits a tested audit implementation.
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

    session = SparkSession.builder.master("local[2]").appName("vstone-unit-tests").getOrCreate()
    yield session
    session.stop()


def test_audit_columns_are_added_with_correct_schema(spark):
    from pyspark.testing.utils import assertSchemaEqual
    from pyspark.sql.types import (
        StructType,
        StructField,
        IntegerType,
        TimestampType,
        StringType,
    )

    from common.audit import add_audit_columns

    df = spark.createDataFrame([(1,), (2,)], schema=StructType([StructField("enter", IntegerType())]))
    result = add_audit_columns(df, source_format="csv", source_file="chunk1.csv")

    expected_schema = StructType(
        [
            StructField("enter", IntegerType()),
            StructField("load_dt", TimestampType(), nullable=False),
            StructField("source_format", StringType(), nullable=False),
            StructField("source_file", StringType(), nullable=False),
            StructField("run_id", StringType(), nullable=False),
        ]
    )
    assertSchemaEqual(result.schema, expected_schema)


def test_audit_columns_values_are_constant_across_rows(spark):
    from pyspark.testing.utils import assertDataFrameEqual
    from pyspark.sql.types import StructType, StructField, IntegerType

    from common.audit import add_audit_columns

    df = spark.createDataFrame([(1,), (2,), (3,)], schema=StructType([StructField("enter", IntegerType())]))
    result = add_audit_columns(df, source_format="json", source_file="chunk3.json")

    distinct_metadata = result.select("source_format", "source_file").distinct()
    expected = spark.createDataFrame([("json", "chunk3.json")], schema=["source_format", "source_file"])

    assertDataFrameEqual(distinct_metadata, expected)
