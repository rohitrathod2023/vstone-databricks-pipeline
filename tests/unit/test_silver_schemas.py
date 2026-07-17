"""Unit tests for config.silver_schemas -- confirms each _rejected schema is
exactly its non-rejected counterpart plus a rejection_reason column, so the
two can never silently drift apart.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_silver_schemas.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config.silver_schemas import (  # noqa: E402
    SILVER_LOCATIONS_REJECTED_SCHEMA,
    SILVER_LOCATIONS_SCHEMA,
    SILVER_STREETS_REJECTED_SCHEMA,
    SILVER_STREETS_SCHEMA,
)


def test_silver_locations_rejected_schema_adds_only_rejection_reason():
    assert SILVER_LOCATIONS_REJECTED_SCHEMA.fields[:-1] == SILVER_LOCATIONS_SCHEMA.fields
    assert SILVER_LOCATIONS_REJECTED_SCHEMA.fields[-1].name == "rejection_reason"
    assert SILVER_LOCATIONS_REJECTED_SCHEMA.fields[-1].dataType.typeName() == "string"


def test_silver_streets_rejected_schema_adds_only_rejection_reason():
    assert SILVER_STREETS_REJECTED_SCHEMA.fields[:-1] == SILVER_STREETS_SCHEMA.fields
    assert SILVER_STREETS_REJECTED_SCHEMA.fields[-1].name == "rejection_reason"
    assert SILVER_STREETS_REJECTED_SCHEMA.fields[-1].dataType.typeName() == "string"


def test_long_is_not_confused_with_longitude_in_the_schema():
    """`long` (street length in meters) and `longitude` are both present and
    distinct -- a real naming collision risk given how similar they read."""
    names = [f.name for f in SILVER_STREETS_SCHEMA.fields]
    assert "long" in names
    assert "longitude" in names
    assert names.count("long") == 1
    assert names.count("longitude") == 1
