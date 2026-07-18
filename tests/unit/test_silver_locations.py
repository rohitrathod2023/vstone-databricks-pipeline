"""Unit tests for pipelines.silver.locations -- typing + quarantine split,
using synthetic data that reproduces the real location=7 bad-coordinate row
found during Phase 1 profiling.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_silver_locations.py -v
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

    session = SparkSession.builder.master("local[2]").appName("vstone-silver-locations-tests").getOrCreate()
    yield session
    session.stop()


# Fixed Bronze audit values appended to every synthetic row below -- proves
# build_checked_locations carries them through unchanged rather than
# regenerating (see locations.py's build_checked_locations Notes).
_AUDIT_VALUES = (datetime(2024, 1, 1, 12, 0, 0), "csv", "node_locations.csv", "test-run-id")


def _make_bronze_df(spark):
    """Reproduces the real node_locations shape: 2 good rows + the real bad
    row (location=7, lat/long exactly 0.0/0.0), plus messy header names to
    confirm header standardization runs too."""
    return spark.createDataFrame(
        [
            ("38.985252", "-0.537441", "1") + _AUDIT_VALUES,
            ("0.0", "0.0", "7") + _AUDIT_VALUES,  # the real bad row found during Phase 1 profiling
            ("38.991913", "-0.524291", "9") + _AUDIT_VALUES,
        ],
        ["Latitude", "  Longitude", "location", "load_dt", "source_format", "source_file", "run_id"],
    )


def test_valid_output_schema_matches_silver_locations_schema_exactly(spark):
    from pyspark.testing.utils import assertSchemaEqual

    from config.silver_schemas import SILVER_LOCATIONS_SCHEMA
    from pipelines.silver.locations import build_checked_locations
    from pipelines.silver.quarantine import valid_rows

    checked = build_checked_locations(_make_bronze_df(spark))
    valid = valid_rows(checked)

    assertSchemaEqual(valid.schema, SILVER_LOCATIONS_SCHEMA)


def test_rejected_output_schema_matches_silver_locations_rejected_schema(spark):
    from pyspark.testing.utils import assertSchemaEqual

    from config.silver_schemas import SILVER_LOCATIONS_REJECTED_SCHEMA
    from pipelines.silver.locations import build_checked_locations
    from pipelines.silver.quarantine import rejected_rows

    checked = build_checked_locations(_make_bronze_df(spark))
    rejected = rejected_rows(checked)

    assertSchemaEqual(rejected.schema, SILVER_LOCATIONS_REJECTED_SCHEMA)


def test_bad_coordinate_row_is_quarantined_not_kept(spark):
    from pipelines.silver.locations import build_checked_locations
    from pipelines.silver.quarantine import rejected_rows, valid_rows

    checked = build_checked_locations(_make_bronze_df(spark))
    valid = valid_rows(checked)
    rejected = rejected_rows(checked)

    valid_locations = {r["location"] for r in valid.collect()}
    rejected_locations = {r["location"] for r in rejected.collect()}

    assert valid_locations == {1, 9}
    assert rejected_locations == {7}


def test_rejected_row_carries_the_exact_reason_text(spark):
    from pipelines.silver.locations import build_checked_locations
    from pipelines.silver.quarantine import rejected_rows

    checked = build_checked_locations(_make_bronze_df(spark))
    rejected_row = rejected_rows(checked).collect()[0]

    assert rejected_row["rejection_reason"] == "invalid_coordinates: latitude and longitude are both 0"


def test_header_standardization_is_applied(spark):
    """Column names are already clean in real data -- this confirms the
    insurance function actually ran, not that it fixed a real problem."""
    from pipelines.silver.locations import build_checked_locations

    checked = build_checked_locations(_make_bronze_df(spark))

    assert set(checked.columns) == {
        "location", "latitude", "longitude", "rejection_reason",
        "load_dt", "source_format", "source_file", "run_id",
    }
