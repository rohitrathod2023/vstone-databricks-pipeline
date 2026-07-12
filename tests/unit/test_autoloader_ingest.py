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

from pipelines.bronze.autoloader_ingest import build_autoloader_options, checkpoint_path  # noqa: E402


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
    options = build_autoloader_options(cfg, checkpoint="/Volumes/cat/raw/raw_volume/_checkpoints/table")

    assert "pathGlobFilter" not in options
    assert "cloudFiles.pathGlobFilter" not in options
    assert options["cloudFiles.format"] == "json"
    assert options["cloudFiles.schemaLocation"] == "/Volumes/cat/raw/raw_volume/_checkpoints/table"


def test_checkpoint_path_lives_under_the_same_raw_volume():
    cfg = {
        "path": "/Volumes/vstone_traffic_dev/dev_rohitrathodcomp_raw/raw_volume/chunks/chunk3.json",
        "target_table": "vstone_traffic_dev.dev_rohitrathodcomp_bronze.traffic_counts_autoloader",
    }
    assert checkpoint_path(cfg) == (
        "/Volumes/vstone_traffic_dev/dev_rohitrathodcomp_raw/raw_volume/_checkpoints/traffic_counts_autoloader"
    )


def test_checkpoint_path_is_keyed_by_target_table_name():
    """A different target table must get a different checkpoint -- otherwise
    two Auto Loader pipelines sharing a source directory would corrupt each
    other's state."""
    cfg_a = {
        "path": "/Volumes/cat/raw/raw_volume/chunks/chunk3.json",
        "target_table": "cat.bronze.table_a",
    }
    cfg_b = {**cfg_a, "target_table": "cat.bronze.table_b"}

    assert checkpoint_path(cfg_a) != checkpoint_path(cfg_b)
    assert checkpoint_path(cfg_a).endswith("/table_a")
    assert checkpoint_path(cfg_b).endswith("/table_b")


def test_chunk3_json_still_lives_in_the_shared_chunks_folder():
    """Confirms sources.yml wasn't reverted back to a dedicated subfolder --
    the exact-path approach means chunk3.json never needed to move from Day
    1's original output location."""
    from common.config_loader import get_source_config

    cfg = get_source_config("chunk3_json", env="dev")
    assert cfg["path"].endswith("/chunks/chunk3.json")
