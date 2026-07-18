"""Unit tests for pipelines.gold.dim_street -- Dim_Street's build_dim_street
function, which reads stg_dim_street_scd2's full history (a plain batch
DataFrame) and adds street_key/is_current. The AUTO CDC mechanism itself
(create_streaming_table/create_auto_cdc_from_snapshot_flow) only runs inside
a real Lakeflow pipeline, same limitation as every other `@dlt.table`/
`spark.readStream` decorator usage in this project.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_dim_street.py -v
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

    session = SparkSession.builder.master("local[2]").appName("vstone-dim-street-tests").getOrCreate()
    yield session
    session.stop()


def _cdc_schema():
    """Explicit schema, not inference -- a synthetic row with __END_AT=None
    can't have its type inferred from data alone."""
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("street_id", IntegerType()),
            StructField("street", StringType()),
            StructField("long", IntegerType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("dangerous", DoubleType()),
            StructField("__START_AT", StringType()),
            StructField("__END_AT", StringType()),
            StructField("load_dt", TimestampType()),
            StructField("source_format", StringType()),
            StructField("source_file", StringType()),
            StructField("run_id", StringType()),
        ]
    )


# Fixed silver_streets audit values appended to every synthetic row below --
# proves build_dim_street carries them through from stg_dim_street_scd2
# unchanged rather than regenerating (see dim_street.py's build_dim_street
# Notes).
_AUDIT_VALUES = (datetime(2024, 1, 1, 12, 0, 0), "delta", "silver_streets", "test-run-id")


def test_schema_matches_expected_shape(spark):
    from pipelines.gold.dim_street import build_dim_street

    df = spark.createDataFrame(
        [(1, "CV-645A", 574, 38.98492, -0.538044, 0.5, "2026-01-01T00:00:00", None) + _AUDIT_VALUES], _cdc_schema()
    )
    result = build_dim_street(df)

    expected_columns = {
        "street_key", "street_id", "street", "long", "latitude", "longitude", "dangerous",
        "__START_AT", "__END_AT", "is_current", "load_dt", "source_format", "source_file", "run_id",
    }
    assert set(result.columns) == expected_columns


def test_a_row_with_null_end_at_is_current(spark):
    from pipelines.gold.dim_street import build_dim_street

    df = spark.createDataFrame(
        [(1, "CV-645A", 574, 38.98492, -0.538044, 0.5, "2026-01-01T00:00:00", None) + _AUDIT_VALUES], _cdc_schema()
    )
    row = build_dim_street(df).collect()[0]

    assert row["is_current"] is True


def test_a_row_with_a_real_end_at_is_not_current(spark):
    """A closed-out prior SCD2 version (has an __END_AT) is not current --
    this is the case that will start mattering once a street's `dangerous`
    score actually changes and a second version appears."""
    from pipelines.gold.dim_street import build_dim_street

    df = spark.createDataFrame(
        [
            (1, "CV-645A", 574, 38.98492, -0.538044, 0.5, "2026-01-01T00:00:00", "2026-02-01T00:00:00")
            + _AUDIT_VALUES
        ],
        _cdc_schema(),
    )
    row = build_dim_street(df).collect()[0]

    assert row["is_current"] is False


def test_street_key_is_contiguous_unique_int_ordered_by_street_id_and_start_at(spark):
    """row_number() OVER (ORDER BY street_id, __START_AT) should produce
    1..N with no gaps or duplicates -- this is the exact behavior that
    failed outright (streaming) or overflowed (monotonic ID) in the earlier
    single-table design, so this test is the real regression guard."""
    from pyspark.sql.types import IntegerType

    from pipelines.gold.dim_street import build_dim_street

    rows = [
        (3, "Street C", 100, 38.9, -0.5, 0.3, "2026-01-01T00:00:00", None) + _AUDIT_VALUES,
        (1, "Street A", 574, 38.98492, -0.538044, 0.5, "2026-01-01T00:00:00", None) + _AUDIT_VALUES,
        (2, "Street B", 194, 38.985471, -0.536866, 0.7, "2026-01-01T00:00:00", None) + _AUDIT_VALUES,
    ]
    df = spark.createDataFrame(rows, _cdc_schema())
    result = build_dim_street(df)

    ordered = result.orderBy("street_key").collect()
    assert [r["street_key"] for r in ordered] == [1, 2, 3]
    assert [r["street_id"] for r in ordered] == [1, 2, 3]  # ordered by street_id, so key order matches
    assert dict(result.dtypes)["street_key"] == IntegerType().simpleString()


def test_audit_columns_are_carried_through_from_stg_dim_street_scd2_not_regenerated(spark):
    """The real fix this test guards: Gold used to call add_audit_columns()
    fresh here too, overwriting the audit lineage carried through the CDC
    flow from silver_streets. Now it must select those values straight
    through unchanged."""
    from pipelines.gold.dim_street import build_dim_street

    df = spark.createDataFrame(
        [(1, "CV-645A", 574, 38.98492, -0.538044, 0.5, "2026-01-01T00:00:00", None) + _AUDIT_VALUES], _cdc_schema()
    )
    row = build_dim_street(df).collect()[0]

    assert row["load_dt"] == _AUDIT_VALUES[0]
    assert row["source_format"] == "delta"
    assert row["source_file"] == "silver_streets"
    assert row["run_id"] == "test-run-id"


def test_tracked_columns_covers_dangerous_and_the_other_scd2_attributes():
    """dangerous is the attribute the brief's SCD2 requirement is really
    about -- confirm it's actually in the tracked list, not accidentally
    left out."""
    from pipelines.gold.dim_street import TRACKED_COLUMNS

    assert "dangerous" in TRACKED_COLUMNS
    assert set(TRACKED_COLUMNS) == {"dangerous", "street", "long", "latitude", "longitude"}
