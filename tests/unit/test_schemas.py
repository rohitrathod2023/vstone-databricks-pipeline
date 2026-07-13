"""
Unit tests for config/schemas.py and config_loader.get_source_schema() --
pure Python, no Spark session needed. Confirms every source's schema is
explicit and entirely StringType (deliberately permissive at Bronze; strict
typing is Silver's job), and that the source_key indirection (bronze_streets
-> raw_streets, etc.) resolves the same way get_source_config() already does.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_schemas.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pyspark.sql.types import StringType  # noqa: E402

from common.config_loader import get_source_schema  # noqa: E402
from config.schemas import SCHEMAS  # noqa: E402


@pytest.mark.parametrize("source_key", sorted(SCHEMAS))
def test_every_declared_schema_is_entirely_string_typed(source_key):
    schema = SCHEMAS[source_key]
    non_string_fields = [f.name for f in schema.fields if not isinstance(f.dataType, StringType)]
    assert not non_string_fields, f"{source_key} has non-string fields: {non_string_fields}"


def test_cars_schema_column_order_matches_the_real_csv_header():
    """Order matters for CSV's positional schema application -- verified
    directly against the raw file (enter,exit,date,id,location), not just
    docs/dataset_profiling.md's listing."""
    from config.schemas import CARS_SCHEMA

    assert [f.name for f in CARS_SCHEMA.fields] == ["enter", "exit", "date", "id", "location"]


def test_all_four_chunks_reuse_the_same_cars_schema():
    from config.schemas import CARS_SCHEMA

    for chunk_key in ("chunk1_csv", "chunk2_csv", "chunk3_json", "chunk4_xml"):
        assert SCHEMAS[chunk_key] is CARS_SCHEMA


def test_get_source_schema_resolves_source_key_indirection():
    """bronze_streets/bronze_node_locations/bronze_streets_list/bronze_telegram
    don't declare their own schema -- they point at raw_* via source_key, the
    same indirection get_source_config() already resolves."""
    from config.schemas import NODE_LOCATIONS_SCHEMA, STREETS_LIST_SCHEMA, STREETS_SCHEMA, TELEGRAM_SCHEMA

    assert get_source_schema("bronze_streets") is STREETS_SCHEMA
    assert get_source_schema("bronze_node_locations") is NODE_LOCATIONS_SCHEMA
    assert get_source_schema("bronze_streets_list") is STREETS_LIST_SCHEMA
    assert get_source_schema("bronze_telegram") is TELEGRAM_SCHEMA


def test_get_source_schema_raises_a_clear_error_for_an_unknown_key():
    with pytest.raises(KeyError, match="No sources.yml entry"):
        get_source_schema("nonexistent_source")


def test_primary_key_columns_are_the_only_non_nullable_fields():
    """node_locations.location and streets_list.street_id are the only
    genuinely-always-present columns (one row per intersection/street) --
    every other field stays nullable=True, the permissive Bronze default."""
    from config.schemas import CARS_SCHEMA, NODE_LOCATIONS_SCHEMA, STREETS_LIST_SCHEMA, STREETS_SCHEMA, TELEGRAM_SCHEMA

    assert all(f.nullable for f in CARS_SCHEMA.fields)
    assert all(f.nullable for f in STREETS_SCHEMA.fields)
    assert all(f.nullable for f in TELEGRAM_SCHEMA.fields)

    non_nullable = {f.name for f in NODE_LOCATIONS_SCHEMA.fields if not f.nullable}
    assert non_nullable == {"location"}

    non_nullable = {f.name for f in STREETS_LIST_SCHEMA.fields if not f.nullable}
    assert non_nullable == {"street_id"}
