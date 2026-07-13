"""
Unit tests for pipelines.bronze.autoloader_ingest's checkpoint-path and
options construction — pure logic, no Spark session or live streaming needed.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_autoloader_ingest.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pipelines.bronze.autoloader_ingest import (  # noqa: E402
    build_autoloader_options,
    checkpoint_path,
    schema_location,
)


def test_autoloader_options_have_no_path_glob_filter():
    """cloudFiles.pathGlobFilter proved unreliable against a real run on this
    Spark Connect runtime (the option key got case-folded server-side into
    an unrecognized key, CF_UNKNOWN_OPTION_KEYS_ERROR). run_autoloader loads
    the exact file path instead of a directory + filter, so no glob filter
    option should ever appear here -- confirm none leaks back in by accident."""
    cfg = {
        "path": "/Volumes/cat/raw/raw_volume/chunks/chunk3.json",
        "format": "json",
    }
    options = build_autoloader_options(cfg, schema_loc="/Volumes/cat/ops/checkpoints_volume/table/schema")

    assert "pathGlobFilter" not in options
    assert "cloudFiles.pathGlobFilter" not in options
    assert options["cloudFiles.format"] == "json"
    assert options["cloudFiles.schemaLocation"] == "/Volumes/cat/ops/checkpoints_volume/table/schema"


def test_checkpoint_path_lives_under_the_ops_volume_not_raw():
    """Checkpoints are pipeline operational state, not data -- they live in
    the dedicated ops schema/volume (resources/catalog.yml), never nested
    inside the raw landing Volume."""
    cfg = {"target_table": "vstone_traffic_dev.dev_rohitrathodcomp_bronze.traffic_counts_autoloader"}

    path = checkpoint_path(cfg, env="dev")

    assert path.startswith("/Volumes/vstone_traffic_dev/dev_rohitrathodcomp_ops/checkpoints_volume/")
    assert "raw_volume" not in path


def test_checkpoint_and_schema_location_are_different_paths():
    """Per Databricks' own guidance: conflating checkpointLocation and
    cloudFiles.schemaLocation makes it impossible to clear schema-evolution
    state without also discarding the checkpoint's exactly-once history."""
    cfg = {"target_table": "vstone_traffic_dev.dev_rohitrathodcomp_bronze.traffic_counts_autoloader"}

    assert checkpoint_path(cfg, env="dev") != schema_location(cfg, env="dev")


def test_checkpoint_path_is_keyed_by_target_table_name():
    """A different target table must get a different checkpoint -- otherwise
    two Auto Loader pipelines sharing a source directory would corrupt each
    other's state."""
    cfg_a = {"target_table": "cat.bronze.table_a"}
    cfg_b = {**cfg_a, "target_table": "cat.bronze.table_b"}

    assert checkpoint_path(cfg_a, env="dev") != checkpoint_path(cfg_b, env="dev")
    assert "/table_a/" in checkpoint_path(cfg_a, env="dev")
    assert "/table_b/" in checkpoint_path(cfg_b, env="dev")


def test_chunk3_json_still_lives_in_the_shared_chunks_folder():
    """Confirms sources.yml wasn't reverted back to a dedicated subfolder --
    the exact-path approach means chunk3.json never needed to move from Day
    1's original output location."""
    from common.config_loader import get_source_config

    cfg = get_source_config("chunk3_json", env="dev")
    assert cfg["path"].endswith("/chunks/chunk3.json")


def test_run_autoloader_would_apply_the_explicit_cars_schema():
    """cloudFiles itself needs a live Databricks cluster (not available in a
    plain local PySpark session), so this can't exercise run_autoloader()'s
    actual stream read -- it confirms the schema run_autoloader() passes to
    .schema(...) is the correct one instead (see test_io_readers.py for a real
    local read of the same schema against the JSON format cloudFiles wraps)."""
    from common.config_loader import get_source_schema
    from config.schemas import CARS_SCHEMA

    assert get_source_schema("chunk3_json") is CARS_SCHEMA
