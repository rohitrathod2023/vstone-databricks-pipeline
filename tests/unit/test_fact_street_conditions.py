"""Unit tests for pipelines.gold.fact_street_conditions -- the range-join
resolution against Dim_Street's SCD2 history is the critical behavior here
(not just a plain equi-join, which would fan out once a street has more
than one version).

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_fact_street_conditions.py -v
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

    session = SparkSession.builder.master("local[2]").appName("vstone-fact-street-conditions-tests").getOrCreate()
    yield session
    session.stop()


def _dim_date_df(spark):
    return spark.createDataFrame(
        [(20240101, "2024-01-01"), (20240601, "2024-06-01")], ["date_key", "full_date"]
    )


def _env_cols():
    return ["street_id", "date", "noise", "pollution", "light", "raining"]


def _dim_street_schema():
    """Explicit schema, not inference -- a synthetic row with __END_AT=None
    can't have its type inferred from data alone."""
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    return StructType(
        [
            StructField("street_key", IntegerType()),
            StructField("street_id", IntegerType()),
            StructField("__START_AT", StringType()),
            StructField("__END_AT", StringType()),
        ]
    )


def test_row_count_matches_input_exactly_no_fan_out_with_a_single_scd2_version(spark):
    from pipelines.gold.fact_street_conditions import build_fact_street_conditions

    dim_street_df = spark.createDataFrame(
        [(1, 1, "2023-01-01T00:00:00", None)], _dim_street_schema()
    )
    environment_df = spark.createDataFrame(
        [(1, "2024-01-01T10:00:00", 10.0, 5.0, 20.0, 0.3)], _env_cols()
    )
    result = build_fact_street_conditions(environment_df, dim_street_df, _dim_date_df(spark))

    assert result.count() == 1


def test_schema_matches_expected_shape(spark):
    from pipelines.gold.fact_street_conditions import build_fact_street_conditions

    dim_street_df = spark.createDataFrame(
        [(1, 1, "2023-01-01T00:00:00", None)], _dim_street_schema()
    )
    environment_df = spark.createDataFrame(
        [(1, "2024-01-01T10:00:00", 10.0, 5.0, 20.0, 0.3)], _env_cols()
    )
    result = build_fact_street_conditions(environment_df, dim_street_df, _dim_date_df(spark))

    expected_columns = {
        "street_key", "date_key", "noise", "pollution", "light", "raining",
        "load_dt", "source_format", "source_file", "run_id",
    }
    assert set(result.columns) == expected_columns


def test_a_fact_resolves_to_the_scd2_version_active_on_its_own_date_not_just_the_latest(spark):
    """The real test of the range join: street_id=1 has two versions
    (closed on 2024-03-01, current after that). A fact from BEFORE the
    change must resolve to the OLD version's street_key, not the new one --
    an equi-join on street_id alone would instead match BOTH versions and
    fan out to 2 rows."""
    from pipelines.gold.fact_street_conditions import build_fact_street_conditions

    dim_street_df = spark.createDataFrame(
        [
            (10, 1, "2023-01-01T00:00:00", "2024-03-01T00:00:00"),  # old version, dangerous=low (implied)
            (11, 1, "2024-03-01T00:00:00", None),  # current version, dangerous=high (implied)
        ],
        ["street_key", "street_id", "__START_AT", "__END_AT"],
    )
    environment_df = spark.createDataFrame(
        [
            (1, "2024-01-01T10:00:00", 10.0, 5.0, 20.0, 0.3),  # before the change -> should resolve to key 10
            (1, "2024-06-01T10:00:00", 12.0, 6.0, 21.0, 0.4),  # after the change -> should resolve to key 11
        ],
        _env_cols(),
    )
    result = build_fact_street_conditions(environment_df, dim_street_df, _dim_date_df(spark))

    # No fan-out: still exactly 2 rows in, 2 rows out.
    assert result.count() == 2

    rows = {r["date_key"]: r["street_key"] for r in result.collect()}
    assert rows[20240101] == 10
    assert rows[20240601] == 11


def test_an_unresolvable_street_produces_null_key_not_a_dropped_row(spark):
    from pipelines.gold.fact_street_conditions import build_fact_street_conditions

    dim_street_df = spark.createDataFrame(
        [(1, 1, "2023-01-01T00:00:00", None)], _dim_street_schema()
    )
    environment_df = spark.createDataFrame(
        [(999, "2024-01-01T10:00:00", 10.0, 5.0, 20.0, 0.3)], _env_cols()
    )
    result = build_fact_street_conditions(environment_df, dim_street_df, _dim_date_df(spark))

    assert result.count() == 1
    assert result.collect()[0]["street_key"] is None


def test_a_fact_older_than_the_dimensions_tracking_start_clamps_to_the_earliest_version(spark):
    """The real bug found live: AUTO CDC FROM SNAPSHOT stamps __START_AT
    with wall-clock processing time (e.g. today), completely disconnected
    from this dataset's 2023-2024 historical fact dates -- every fact
    predates every version's __START_AT, so the plain range condition alone
    resolved 100% of real rows to NULL. This proves the clamp fix: a fact
    from before Dim_Street's earliest known version resolves to that
    earliest version instead of NULL."""
    from pipelines.gold.fact_street_conditions import build_fact_street_conditions

    dim_street_df = spark.createDataFrame(
        [(1, 1, "2026-07-17T00:00:00", None)], _dim_street_schema()
    )
    environment_df = spark.createDataFrame(
        [(1, "2023-06-02T10:00:00", 10.0, 5.0, 20.0, 0.3)], _env_cols()
    )
    result = build_fact_street_conditions(environment_df, dim_street_df, _dim_date_df(spark))
    row = result.collect()[0]

    assert row["street_key"] == 1
    assert result.count() == 1  # still no fan-out


def test_backfill_clamp_does_not_fan_out_when_multiple_versions_exist(spark):
    """With two versions of the same street, a fact predating BOTH must
    clamp to only the earliest one -- not match both and fan out."""
    from pipelines.gold.fact_street_conditions import build_fact_street_conditions

    dim_street_df = spark.createDataFrame(
        [
            (10, 1, "2026-01-01T00:00:00", "2026-06-01T00:00:00"),
            (11, 1, "2026-06-01T00:00:00", None),
        ],
        ["street_key", "street_id", "__START_AT", "__END_AT"],
    )
    environment_df = spark.createDataFrame(
        [(1, "2023-06-02T10:00:00", 10.0, 5.0, 20.0, 0.3)], _env_cols()
    )
    result = build_fact_street_conditions(environment_df, dim_street_df, _dim_date_df(spark))

    assert result.count() == 1
    assert result.collect()[0]["street_key"] == 10
