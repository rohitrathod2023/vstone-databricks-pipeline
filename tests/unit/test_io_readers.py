"""
Unit tests for common.io_readers -- confirms an explicit schema passed to
read_csv/read_json actually produces the expected string-typed DataFrame
schema (not just that the code compiles). Uses a local PySpark session and
real, tiny on-disk fixture files -- no Databricks cluster needed.

XML (spark-xml) and cloudFiles (Auto Loader) aren't exercised here since
neither is available in a plain local PySpark session -- see
tests/unit/test_schemas.py for the schema-resolution coverage that applies to
those two techniques instead.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_io_readers.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pyspark.sql.types import StringType  # noqa: E402

from common.io_readers import read_csv, read_json  # noqa: E402
from config.schemas import CARS_SCHEMA  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[2]").appName("vstone-io-readers-tests").getOrCreate()
    yield session
    session.stop()


def test_read_csv_with_explicit_schema_produces_all_string_columns(spark, tmp_path):
    csv_path = tmp_path / "cars_fixture.csv"
    csv_path.write_text("enter,exit,date,id,location\n1,2,2023-06-02T12:00:00.000Z,530,7\n")

    df = read_csv(spark, csv_path.as_posix(), schema=CARS_SCHEMA)

    assert [f.name for f in df.schema.fields] == ["enter", "exit", "date", "id", "location"]
    assert all(isinstance(f.dataType, StringType) for f in df.schema.fields)
    row = df.collect()[0]
    assert row["enter"] == "1"  # a genuine string, not silently cast back to int
    assert row["id"] == "530"


def test_read_csv_without_schema_still_falls_back_to_inferschema(spark, tmp_path):
    """Existing callers that don't pass schema= (none remain in src/, but the
    parameter is optional) must keep working exactly as before."""
    csv_path = tmp_path / "untyped_fixture.csv"
    csv_path.write_text("a,b\n1,2\n")

    df = read_csv(spark, csv_path.as_posix())

    assert dict(df.dtypes)["a"] == "int"  # inferred, not string -- proves inferSchema still ran


def test_read_json_with_explicit_schema_produces_all_string_columns(spark, tmp_path):
    json_path = tmp_path / "chunk3_fixture.json"
    json_path.write_text('{"enter": 1, "exit": 2, "date": "2023-06-02T12:00:00.000Z", "id": 530, "location": 7}\n')

    df = read_json(spark, json_path.as_posix(), schema=CARS_SCHEMA)

    assert all(isinstance(f.dataType, StringType) for f in df.schema.fields)
    row = df.collect()[0]
    assert row["enter"] == "1"
    assert row["location"] == "7"
