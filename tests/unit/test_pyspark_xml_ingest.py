"""
Unit tests for pipelines.bronze.pyspark_xml_ingest's column-name sanitizing
and skip-based idempotency -- the sanitizing tests need no Spark session;
the run()-skip/force tests use a real local PySpark session (spark-xml
itself isn't available locally, so read_xml is monkeypatched rather than
exercising a real XML read).

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_pyspark_xml_ingest.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pipelines.bronze.pyspark_xml_ingest import sanitize_column_name  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[2]").appName("vstone-pyspark-xml-tests").getOrCreate()
    yield session
    session.stop()


def test_sanitize_replaces_hyphens_and_colons():
    assert sanitize_column_name("street-name") == "street_name"
    assert sanitize_column_name("node:id") == "node_id"


def test_sanitize_replaces_spaces():
    assert sanitize_column_name("col with spaces") == "col_with_spaces"


def test_sanitize_leaves_already_valid_names_untouched():
    assert sanitize_column_name("valid_name") == "valid_name"
    assert sanitize_column_name("enter") == "enter"


def test_sanitize_handles_multiple_consecutive_invalid_chars():
    assert sanitize_column_name("a--b::c") == "a__b__c"


def test_run_pyspark_xml_would_apply_the_explicit_cars_schema():
    """spark-xml isn't available in a plain local PySpark session, so this
    can't exercise a real XML read -- confirms the schema run_pyspark_xml()
    passes to read_xml(schema=...) is the correct one instead."""
    from utils.config_loader import get_source_schema
    from config.schemas import CARS_SCHEMA

    assert get_source_schema("chunk4_xml") is CARS_SCHEMA


def _fake_cfg(tmp_path):
    return {
        "path": (tmp_path / "chunk4.xml").as_posix(),
        "format": "xml",
        "target_table": "cat.bronze.traffic_counts_pyspark",
    }


def test_pyspark_xml_already_complete_reflects_fingerprint_match(tmp_path, monkeypatch):
    import pipelines.bronze.pyspark_xml_ingest as xml_mod

    (tmp_path / "chunk4.xml").write_text("<records></records>")
    # Nested under a subdirectory that does NOT pre-exist -- mirrors the real
    # checkpoints_volume/<table_name>/_pyspark_xml_complete structure, so this
    # test would actually catch mark_pyspark_xml_complete() failing to create
    # its parent directory first (a real bug found during live verification).
    marker_path = (tmp_path / "traffic_counts_pyspark" / "_pyspark_xml_complete").as_posix()
    monkeypatch.setattr(xml_mod, "_completion_marker_path", lambda cfg, env="dev": marker_path)
    cfg = _fake_cfg(tmp_path)

    assert xml_mod.pyspark_xml_already_complete(cfg, env="dev") is False

    xml_mod.mark_pyspark_xml_complete(cfg, env="dev")

    assert xml_mod.pyspark_xml_already_complete(cfg, env="dev") is True


def test_pyspark_xml_already_complete_detects_a_changed_source_file(tmp_path, monkeypatch):
    """A crash-free but edited/replaced source file must not be mistaken for
    "already loaded" -- the fingerprint (size+mtime), not just marker
    existence, is what's checked."""
    import pipelines.bronze.pyspark_xml_ingest as xml_mod

    (tmp_path / "chunk4.xml").write_text("<records></records>")
    marker_path = (tmp_path / "traffic_counts_pyspark" / "_pyspark_xml_complete").as_posix()
    monkeypatch.setattr(xml_mod, "_completion_marker_path", lambda cfg, env="dev": marker_path)
    cfg = _fake_cfg(tmp_path)

    xml_mod.mark_pyspark_xml_complete(cfg, env="dev")
    assert xml_mod.pyspark_xml_already_complete(cfg, env="dev") is True

    (tmp_path / "chunk4.xml").write_text("<records><record/></records>")  # different size
    assert xml_mod.pyspark_xml_already_complete(cfg, env="dev") is False


def test_fingerprint_ignores_spark_internal_marker_files_in_a_chunk_directory(tmp_path):
    """chunk4.xml is actually a Spark output *directory* (part-*.xml +
    _started_/_committed_ transaction markers), not a plain file -- found
    live: stat'ing the directory itself picked up mtime changes from
    unrelated leftover marker files from past chunking runs, breaking the
    skip-check. The fingerprint must only look at the real part file(s)."""
    from pipelines.bronze.pyspark_xml_ingest import _source_fingerprint

    chunk_dir = tmp_path / "chunk4.xml"
    chunk_dir.mkdir()
    (chunk_dir / "part-00000-abc-c000.xml").write_text("<records><record/></records>")
    (chunk_dir / "_started_123").write_text("")
    (chunk_dir / "_committed_123").write_text("some transaction metadata")

    fingerprint_before = _source_fingerprint(chunk_dir.as_posix())

    # A new, unrelated marker appears (as if another run touched the
    # directory) -- the real data file is untouched, so the fingerprint
    # must NOT change.
    (chunk_dir / "_committed_456").write_text("more transaction metadata")

    assert _source_fingerprint(chunk_dir.as_posix()) == fingerprint_before

    # Now the actual data file changes -- the fingerprint MUST change.
    (chunk_dir / "part-00000-abc-c000.xml").write_text("<records><record/><record/></records>")

    assert _source_fingerprint(chunk_dir.as_posix()) != fingerprint_before


def test_run_skips_when_already_complete_and_not_forced(tmp_path, monkeypatch):
    import pipelines.bronze.pyspark_xml_ingest as xml_mod

    (tmp_path / "chunk4.xml").write_text("<records></records>")
    cfg = _fake_cfg(tmp_path)
    read_calls = []

    class FakeTable:
        def count(self):
            return 246

    class FakeSpark:
        def table(self, name):
            assert name == cfg["target_table"]
            return FakeTable()

    monkeypatch.setattr(xml_mod, "get_source_config", lambda key, env="dev": cfg)
    monkeypatch.setattr(xml_mod, "pyspark_xml_already_complete", lambda cfg, env: True)
    monkeypatch.setattr(xml_mod, "mark_pyspark_xml_complete", lambda cfg, env: None)
    monkeypatch.setattr(xml_mod, "read_xml", lambda *a, **kw: read_calls.append(1))

    result = xml_mod.run_pyspark_xml(FakeSpark(), "chunk4_xml", env="dev", force=False)

    assert read_calls == []
    assert result == {"source_key": "chunk4_xml", "target_table": cfg["target_table"], "row_count": 246}


def test_run_force_bypasses_skip_even_when_already_complete(tmp_path, monkeypatch, spark):
    import pipelines.bronze.pyspark_xml_ingest as xml_mod

    (tmp_path / "chunk4.xml").write_text("<records></records>")
    cfg = _fake_cfg(tmp_path)
    write_calls = []
    marked = []

    fake_df = spark.createDataFrame(
        [("1", "2", "2023-06-02", "530", "7")], ["enter", "exit", "date", "id", "location"]
    )

    class FakeTable:
        def count(self):
            return 1

    class FakeSpark:
        def table(self, name):
            return FakeTable()

    monkeypatch.setattr(xml_mod, "get_source_config", lambda key, env="dev": cfg)
    monkeypatch.setattr(xml_mod, "pyspark_xml_already_complete", lambda cfg, env: True)
    monkeypatch.setattr(xml_mod, "mark_pyspark_xml_complete", lambda cfg, env: marked.append(env))
    monkeypatch.setattr(xml_mod, "read_xml", lambda *a, **kw: fake_df)
    monkeypatch.setattr(xml_mod, "_write_bronze_table", lambda df, table: write_calls.append(table))

    result = xml_mod.run_pyspark_xml(FakeSpark(), "chunk4_xml", env="dev", force=True)

    assert write_calls == [cfg["target_table"]]
    assert marked == ["dev"]
    assert result["row_count"] == 1
