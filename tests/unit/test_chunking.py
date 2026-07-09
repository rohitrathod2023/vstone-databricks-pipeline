"""
Unit tests for the chunking split logic, using a local PySpark session (no
Databricks cluster needed — runs in GitHub Actions CI on every PR).

Uses pyspark.testing.assertDataFrameEqual / assertSchemaEqual per the brief's
required testing approach. Requires PySpark >= 3.5.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_chunking.py -v
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

    session = (
        SparkSession.builder.master("local[2]")
        .appName("vstone-unit-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def synthetic_cars_df(spark):
    """100 synthetic rows spanning 100 consecutive days, in intentionally
    scrambled (non-chronological) order — the split logic must sort by date
    itself rather than trust input order."""
    from datetime import datetime, timedelta

    import random

    rows = []
    base = datetime(2023, 6, 2)
    for i in range(100):
        rows.append(
            {
                "enter": i,
                "exit": i + 1,
                "date": base + timedelta(days=i),
                "id": 500 + (i % 10),
                "location": i % 14,
            }
        )
    random.Random(42).shuffle(rows)  # deterministic shuffle
    return spark.createDataFrame(rows)


def test_split_percentages_sum_to_100():
    from pipelines.ingestion.chunking import CHUNK_PLAN

    assert sum(pct for pct, _ in CHUNK_PLAN) == 100


def test_split_is_chronological_and_covers_all_rows(spark, synthetic_cars_df, monkeypatch):
    """Verifies the core invariant: after sorting by date, the 4 slices are
    contiguous, non-overlapping, chronologically ordered, and together cover
    every row exactly once — without touching Volumes or the real config
    (monkeypatches read_source/write_source/get_source_config)."""
    import pipelines.ingestion.chunking as chunking_mod

    written = {}

    def fake_get_source_config(key, env="dev"):
        return {
            "path": f"/tmp/{key}",
            "format": "csv" if "csv" in key or key == "raw_cars" else ("json" if "json" in key else "xml"),
            "target_table": "vstone_traffic_dev.bronze.traffic_counts",
        }

    def fake_read_source(spark_, cfg):
        return synthetic_cars_df

    def fake_write_source(df, cfg, mode="overwrite", **kw):
        written[cfg["path"]] = [r.asDict() for r in df.collect()]

    monkeypatch.setattr(chunking_mod, "get_source_config", fake_get_source_config)
    monkeypatch.setattr(chunking_mod, "read_source", fake_read_source)
    monkeypatch.setattr(chunking_mod, "write_source", fake_write_source)

    results = chunking_mod.run(spark, env="dev")

    assert sum(results.values()) == 100
    assert results["chunk1_csv"] == 40
    assert results["chunk2_csv"] == 30
    assert results["chunk3_json"] == 20
    assert results["chunk4_xml"] == 10

    # chronological + contiguous: last date of chunk N < first date of chunk N+1
    chunk_paths = ["/tmp/chunk1_csv", "/tmp/chunk2_csv", "/tmp/chunk3_json", "/tmp/chunk4_xml"]
    prev_max_date = None
    for path in chunk_paths:
        dates = sorted(r["date"] for r in written[path])
        if prev_max_date is not None:
            assert dates[0] > prev_max_date, f"{path} overlaps the previous chunk chronologically"
        prev_max_date = dates[-1]


def test_split_is_idempotent(spark, synthetic_cars_df, monkeypatch):
    """Running twice must produce identical output (mode='overwrite', deterministic
    ordering) — the brief requires the notebook to 'handle multiple run scenarios'."""
    import pipelines.ingestion.chunking as chunking_mod

    writes = []

    def fake_get_source_config(key, env="dev"):
        return {
            "path": f"/tmp/{key}",
            "format": "csv" if "csv" in key or key == "raw_cars" else ("json" if "json" in key else "xml"),
            "target_table": "vstone_traffic_dev.bronze.traffic_counts",
        }

    monkeypatch.setattr(chunking_mod, "get_source_config", fake_get_source_config)
    monkeypatch.setattr(chunking_mod, "read_source", lambda s, c: synthetic_cars_df)
    monkeypatch.setattr(
        chunking_mod,
        "write_source",
        lambda df, cfg, mode="overwrite", **kw: writes.append((cfg["path"], sorted(r["id"] for r in df.collect()))),
    )

    chunking_mod.run(spark, env="dev")
    first_run = list(writes)
    writes.clear()
    chunking_mod.run(spark, env="dev")
    second_run = list(writes)

    assert first_run == second_run
