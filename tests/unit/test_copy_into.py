"""
Unit tests for pipelines.bronze.copy_into's SQL-string construction —
pure logic, no Spark session or live warehouse needed, since
build_copy_into_sql() takes a plain config dict and returns a string.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_copy_into.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pipelines.bronze.copy_into import build_copy_into_sql, build_create_table_sql  # noqa: E402


def test_create_table_sql_declares_every_source_column_as_string():
    """Bronze's target table shape must be explicit, not inferred from a
    zero-row CTAS over read_files(...) -- every source column is declared
    STRING (deliberately permissive per Databricks' own medallion guidance;
    strict typing is Silver's job). Audit columns keep their own real types."""
    cfg = {"target_table": "cat.schema.table"}
    sql = build_create_table_sql(cfg, source_key="raw_node_locations")

    assert "CREATE TABLE IF NOT EXISTS cat.schema.table (" in sql
    assert "latitude STRING" in sql
    assert "longitude STRING" in sql
    assert "location STRING" in sql
    assert "load_dt TIMESTAMP" in sql
    assert "source_format STRING" in sql
    assert "source_file STRING" in sql
    assert "run_id STRING" in sql
    assert "USING DELTA" in sql
    assert "inferSchema" not in sql
    assert "read_files" not in sql


def test_create_table_sql_uses_cars_schema_for_every_chunk():
    """chunk1_csv..chunk4_xml all share cars.csv's 5 columns regardless of
    on-disk format (CSV/JSON/XML) -- same source, different technique."""
    cfg = {"target_table": "cat.bronze.traffic_counts"}

    for chunk_key in ("chunk1_csv", "chunk2_csv", "chunk3_json", "chunk4_xml"):
        sql = build_create_table_sql(cfg, source_key=chunk_key)
        for column in ("enter", "exit", "date", "id", "location"):
            assert f"{column} STRING" in sql, f"{chunk_key} missing column {column}"


def test_copy_into_sql_targets_the_configured_table():
    cfg = {
        "target_table": "vstone_traffic_dev.dev_rohitrathodcomp_bronze.traffic_counts_copyinto",
        "path": "/Volumes/vstone_traffic_dev/dev_rohitrathodcomp_raw/raw_volume/chunks/chunk1.csv",
        "format": "csv",
    }
    sql = build_copy_into_sql(cfg, run_id="test-run-123")

    assert "COPY INTO vstone_traffic_dev.dev_rohitrathodcomp_bronze.traffic_counts_copyinto" in sql
    assert "FROM '/Volumes/vstone_traffic_dev/dev_rohitrathodcomp_raw/raw_volume/chunks/chunk1.csv'" in sql
    assert "FILEFORMAT = CSV" in sql
    assert "inferSchema" not in sql


def test_copy_into_sql_tags_every_row_with_audit_columns():
    cfg = {
        "target_table": "cat.schema.table",
        "path": "/Volumes/cat/raw/raw_volume/incoming/streets.csv",
        "format": "csv",
    }
    sql = build_copy_into_sql(cfg, run_id="run-abc")

    assert "current_timestamp() AS load_dt" in sql
    assert "'csv' AS source_format" in sql
    assert "'streets.csv' AS source_file" in sql
    assert "'run-abc' AS run_id" in sql


def test_copy_into_sql_derives_source_file_from_path():
    cfg = {
        "target_table": "cat.schema.table",
        "path": "/Volumes/cat/raw/raw_volume/incoming/telegram.csv",
        "format": "csv",
    }
    sql = build_copy_into_sql(cfg, run_id="run-xyz")

    assert "'telegram.csv' AS source_file" in sql


def test_same_source_key_resolves_to_different_schema_per_environment():
    """The exact thing that broke in Day 1 before schema-name parameterization
    was added: dev's schemas are prefixed (mode: development), test/prod's
    aren't -- config_loader must resolve each correctly, not just for dev."""
    from utils.config_loader import get_source_config

    dev_cfg = get_source_config("chunk1_csv", env="dev")
    test_cfg = get_source_config("chunk1_csv", env="test")
    prod_cfg = get_source_config("chunk1_csv", env="prod")

    assert dev_cfg["target_table"] == "vstone_traffic_dev.dev_rohitrathodcomp_bronze.traffic_counts_copyinto"
    assert test_cfg["target_table"] == "vstone_traffic_test.bronze.traffic_counts_copyinto"
    assert prod_cfg["target_table"] == "vstone_traffic_prod.bronze.traffic_counts_copyinto"


def test_source_key_indirection_resolves_path_and_format():
    """bronze_streets etc. don't define their own path/format -- they set
    source_key: raw_streets to reuse another entry's path/format/description.
    This broke the first Day 2 job run (KeyError: 'path') before
    config_loader learned to resolve it."""
    from utils.config_loader import get_source_config

    cfg = get_source_config("bronze_streets", env="dev")
    assert cfg["path"].endswith("streets.csv")
    assert cfg["format"] == "csv"
    assert cfg["target_table"] == "vstone_traffic_dev.dev_rohitrathodcomp_bronze.street_conditions"
    assert "description" in cfg


def test_chunk_sources_strip_and_retag_existing_audit_columns():
    """chunk1_csv etc. were already written by chunking.py's add_audit_columns()
    call, so they already have load_dt/source_format/source_file/run_id from
    that extraction event -- SELECT * would collide (COLUMN_ALREADY_EXISTS on
    a real warehouse). has_audit_columns: true in sources.yml makes copy_into
    strip and re-tag instead."""
    cfg = {
        "target_table": "cat.bronze.traffic_counts_copyinto",
        "path": "/Volumes/cat/raw/raw_volume/chunks/chunk1.csv",
        "format": "csv",
        "has_audit_columns": True,
    }
    copy_sql = build_copy_into_sql(cfg, run_id="run-1")

    # The EXCEPT applies to the actual COPY INTO SELECT (reading the file,
    # which already carries audit columns from chunking.py) -- it has no
    # bearing on build_create_table_sql, which only ever declares the target
    # table's own column list once, from the schema, regardless of this flag.
    assert "* EXCEPT (load_dt, source_format, source_file, run_id)" in copy_sql
    # still re-tags with fresh Bronze-load values afterward
    assert "current_timestamp() AS load_dt" in copy_sql


def test_raw_file_sources_use_plain_select_star():
    """The 4 un-chunked raw files (streets.csv etc.) have never been touched by
    add_audit_columns() -- has_audit_columns is absent/false for these, so a
    plain SELECT * is correct and sufficient."""
    cfg = {
        "target_table": "cat.bronze.street_conditions",
        "path": "/Volumes/cat/raw/raw_volume/incoming/streets.csv",
        "format": "csv",
    }
    copy_sql = build_copy_into_sql(cfg, run_id="run-2")

    assert "SELECT *," in copy_sql
    assert "EXCEPT" not in copy_sql


def test_the_4_chunks_target_separate_bronze_tables():
    """Bronze is the raw landing layer -- one table per source/technique is the
    correct grain. Reconciling the 4 chunks into one stream is Silver's job
    (Day 4-5), not something to force at Bronze."""
    from utils.config_loader import get_source_config

    targets = {
        get_source_config(key, env="dev")["target_table"]
        for key in ("chunk1_csv", "chunk2_csv", "chunk3_json", "chunk4_xml")
    }
    assert len(targets) == 4
