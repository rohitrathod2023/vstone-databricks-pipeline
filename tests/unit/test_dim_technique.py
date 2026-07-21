"""Unit tests for pipelines.gold.dim_technique -- the static ingestion
technique reference dimension.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_dim_technique.py -v
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

    session = SparkSession.builder.master("local[2]").appName("vstone-dim-technique-tests").getOrCreate()
    yield session
    session.stop()


def test_row_count_matches_the_real_number_of_ingestion_techniques(spark):
    """Only 4 real techniques are used anywhere in this pipeline (see
    pipelines.silver.traffic.SOURCE_TECHNIQUES and sources.yml's technique:
    fields) -- no fabricated 'telegram_csv' technique, since bronze_telegram
    really uses copy_into, same as bronze_streets."""
    from pipelines.gold.dim_technique import build_dim_technique

    result = build_dim_technique(spark)

    assert result.count() == 4


def test_technique_names_match_silver_traffics_real_source_technique_values(spark):
    from pipelines.gold.dim_technique import build_dim_technique
    from pipelines.silver.traffic import SOURCE_TECHNIQUES

    result = build_dim_technique(spark)
    names = {r["technique_name"] for r in result.collect()}

    assert names == set(SOURCE_TECHNIQUES)


def test_technique_key_is_integer_type_not_long(spark):
    """Regression test for a real bug caught live on deploy:
    spark.createDataFrame() infers technique_key as LongType from the plain
    Python int literals, but DIM_TECHNIQUE_SCHEMA declares it INT -- Delta
    rejected the mismatch (DELTA_MERGE_INCOMPATIBLE_DATATYPE) until
    build_dim_technique explicitly cast it."""
    from pyspark.sql.types import IntegerType

    from pipelines.gold.dim_technique import build_dim_technique

    result = build_dim_technique(spark)

    assert result.schema["technique_key"].dataType == IntegerType()


def test_technique_key_is_unique_and_not_null(spark):
    from pipelines.gold.dim_technique import build_dim_technique

    result = build_dim_technique(spark)
    keys = [r["technique_key"] for r in result.collect()]

    assert all(k is not None for k in keys)
    assert len(keys) == len(set(keys))


def test_schema_matches_expected_shape(spark):
    from pipelines.gold.dim_technique import build_dim_technique

    result = build_dim_technique(spark)

    expected_columns = {
        "technique_key", "technique_name", "technique_type", "description", "supports_streaming", "created_date",
    }
    assert set(result.columns) == expected_columns
