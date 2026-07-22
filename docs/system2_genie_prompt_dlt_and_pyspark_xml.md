# Prompt for Databricks workspace AI assistant — apply on System 2

Copy everything below this line into your Databricks workspace's AI assistant chat, in the repo root context (it needs to read/write files under `src/`, `resources/`, and `tests/`).

---

I need you to make two independent, already-designed changes to this Databricks Asset Bundle repo (`vstone-databricks-pipeline`). Both changes are already implemented and verified on a separate copy of this same project — I'm giving you the exact target code, not asking you to design anything. Apply them exactly as specified, then run the verification steps, then handle git.

## Background context you need

This is a medallion-architecture capstone project. Bronze layer has 4 ingestion techniques: COPY INTO, Delta Live Tables (DLT), Auto Loader, and plain PySpark read/write. Config is fully externalized: `src/config/sources.yml` (per-dataset path/format/target_table), `src/config/env.yml` (per-environment catalog/schema names — `mode: development` auto-prefixes these per-user), `src/config/schemas.py` (explicit, deliberately permissive `StringType` schema per source — Bronze never infers schema, matching Databricks' own medallion guidance that strict typing belongs to Silver, not Bronze). `src/common/config_loader.py` exposes `get_source_config(key, env)`, `get_env_config(env)`, and `get_source_schema(key)` as the only way any pipeline code touches config. There's also a dedicated `ops` schema + `checkpoints_volume` (declared in `resources/catalog.yml`, with `ops_schema` in `env.yml`) used for pipeline operational state (streaming checkpoints, completion markers) — deliberately kept separate from the `raw` schema, which holds only landed data.

---

## TASK 1 — Convert the DLT Bronze table from a materialized view to a streaming table

**File to edit:** `src/pipelines/bronze/dlt_traffic_counts.py`

**Why:** I checked Databricks' own documentation. Bronze-layer ingestion should be a **streaming table** (append-only, incremental) — materialized views are meant for Silver/Gold transformations, not raw ingestion. The current code does a plain `spark.read` (batch) inside a `@dlt.table` function, which DLT automatically treats as a **materialized view** (full recompute every pipeline run). That's backwards from the documented pattern.

**Exact change:** Inside the `@dlt.table`-decorated function `traffic_counts_dlt()`, replace this:
```python
def traffic_counts_dlt():
    # Explicit, permissive (string-typed) schema -- see config/schemas.py.
    # Strict typing/validation is deferred to Silver, not done at Bronze.
    df = spark.read.option("header", "true").schema(get_source_schema("chunk2_csv")).csv(_CFG["path"])
    # chunk2_csv already carries its own audit columns from chunking.py --
    # withColumn() overwrites same-named columns (see autoloader_ingest.py's
    # note), so this safely re-tags with Bronze's own load event.
    return add_audit_columns(df, source_format=_CFG["format"], source_file=_SOURCE_FILE)
```
with this:
```python
def traffic_counts_dlt():
    # cloudFiles (Auto Loader), not a plain spark.read -- this is what makes
    # DLT treat traffic_counts_dlt as a streaming table instead of a
    # materialized view. .load() is given the exact file path, not the
    # shared chunks/ folder, same reasoning as autoloader_ingest.py: avoids
    # picking up chunk1.csv/chunk3.json/chunk4.xml as siblings.
    #
    # Explicit, permissive (string-typed) schema -- see config/schemas.py.
    # Strict typing/validation is deferred to Silver, not done at Bronze.
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", _CFG["format"])
        .option("header", "true")
        .schema(get_source_schema("chunk2_csv"))
        .load(_CFG["path"])
    )
    # chunk2_csv already carries its own audit columns from chunking.py --
    # withColumn() overwrites same-named columns (see autoloader_ingest.py's
    # note), so this safely re-tags with Bronze's own load event.
    return add_audit_columns(df, source_format=_CFG["format"], source_file=_SOURCE_FILE)
```

**Important — do NOT add `cloudFiles.schemaLocation` or any checkpoint option here.** Inside a DLT/Lakeflow pipeline, the schema/checkpoint location is managed automatically by the pipeline under its own storage root. Adding one manually would be wrong here, unlike the standalone Auto Loader module (`autoloader_ingest.py`), which isn't running inside a DLT pipeline and has to manage that itself.

Also add a short module-level comment near the top of the file (after the existing docstring-style `# MAGIC` comments) noting: *"Streaming table, not a materialized view — a `@dlt.table` function doing a plain `spark.read` becomes a materialized view; Bronze should be a streaming table per Databricks' own guidance, materialized views are for Silver/Gold."*

**Verification steps — read this carefully, there is a known gotcha:**

1. `databricks bundle validate -t dev` then `databricks bundle deploy -t dev`.
2. Run the job: `databricks bundle run bronze_autoloader_pyspark_dlt_job -t dev`.
3. **Expect this to FAIL the first time**, with an error like: `[CANNOT_CHANGE_DATASET_TYPE] Cannot change the dataset type of a pipeline table from MATERIALIZED_VIEW to STREAMING_TABLE for ... traffic_counts_dlt. To change the dataset type, please drop the existing dataset first.` This is expected and actually good news — it's Databricks itself confirming the old table really was a materialized view.
4. Drop the table: `databricks tables delete <catalog>.<your_bronze_schema>.traffic_counts_dlt` (find the exact catalog/schema names from `src/config/env.yml`'s `dev` block).
5. Run the job again — it should succeed this time.
6. Confirm the fix actually worked: `databricks tables get <catalog>.<your_bronze_schema>.traffic_counts_dlt` and check the JSON output's `table_type` field — it must say `STREAMING_TABLE`, not `MATERIALIZED_VIEW`. Also check that the `properties` field contains keys starting with `spark.internal.streaming_table.` — their presence confirms the table is genuinely managed as a streaming table now.
7. Confirm the schema is unaffected: every source column (`enter`, `exit`, `date`, `id`, `location`) should still show `STRING`, and `load_dt` should still show `TIMESTAMP`.

---

## TASK 2 — Add skip-based idempotency to the PySpark XML technique

**Why:** Every other Bronze technique already skips redundant work: COPY INTO tracks loaded files internally, Auto Loader tracks them via its streaming checkpoint, and `chunking.py` has its own completion-marker skip-check. The plain PySpark XML technique (`chunk4_xml`) had none of this — `mode="overwrite"` made it *safe* to re-run (no duplication) but not *efficient* (it always fully re-read and rewrote the file even with nothing changed). This task brings it in line with the others.

### 2a. Rewrite `src/pipelines/bronze/pyspark_xml_ingest.py`

Replace the entire file with this exact content:

```python
"""
Plain PySpark read/write for chunk4_xml -> Bronze. Unlike COPY INTO (Day 2)
and Auto Loader/DLT (this file's siblings), a plain read/write has no
built-in rerun-safety of its own -- mode="overwrite" is the explicit fix:
chunk4.xml is read and rewritten wholesale on every run (not incrementally),
so there's no "new rows since last run" concept a keyed MERGE would need to
reconcile -- overwrite is both simpler to justify and the correct choice
here, not just the easy one.

Skip-based idempotency on top of that (force=False by default), same
philosophy as chunking.py/copy_into.py's default-skip/force-reprocess
behavior: re-running with chunk4.xml unchanged skips the read+sanitize+write
entirely instead of just guaranteeing it wouldn't duplicate anything.
Change-detection is a cheap size+mtime fingerprint, not a full content
hash, so checking "has this changed" never requires reading the file. The
completion marker lives in the dedicated ops volume (see
resources/catalog.yml), not the raw landing Volume -- it's pipeline
operational state, not data, same reasoning as autoloader_ingest.py's
checkpoint relocation.

Note: the reference column-sanitizing fix mentioned for this technique
(XML_Data_Read_and_Write.py/.ipynb, Section 7 course assets) isn't present
in this repo -- this reimplements the same standard fix (invalid XML
tag/attribute characters aren't valid Delta/Parquet column names) rather
than copying unseen code.

    from pipelines.bronze.pyspark_xml_ingest import run_pyspark_xml
    result = run_pyspark_xml(spark, "chunk4_xml", env="dev")
"""
from __future__ import annotations

import os
import re
from typing import Any, Dict

from common.audit import add_audit_columns
from common.config_loader import get_env_config, get_source_config, get_source_schema
from common.io_readers import read_xml

_INVALID_COLUMN_CHARS = re.compile(r"[^0-9a-zA-Z_]")


def sanitize_column_name(name: str) -> str:
    """XML tag/attribute names can contain characters Delta/Parquet column
    names can't (spaces, '-', ':', etc.) -- replace anything that isn't
    alphanumeric or underscore with '_'."""
    return _INVALID_COLUMN_CHARS.sub("_", name)


def _completion_marker_path(cfg: Dict[str, Any], env: str = "dev") -> str:
    """Lives in the dedicated ops volume, not the raw landing Volume --
    keyed by target table name, same convention as autoloader_ingest.py's
    checkpoint."""
    catalog = cfg["target_table"].split(".")[0]
    ops_schema = get_env_config(env)["ops_schema"]
    table_name = cfg["target_table"].rsplit(".", 1)[-1]
    return f"/Volumes/{catalog}/{ops_schema}/checkpoints_volume/{table_name}/_pyspark_xml_complete"


def _source_fingerprint(source_path: str) -> str:
    """Size + modification time -- cheap enough to check on every run
    without reading the file itself, which would partly defeat the point
    of skipping the read.

    chunk4.xml (like every chunk output from chunking.py) is actually a
    Spark output *directory*, not a plain file -- it holds the real data in
    one or more part-*.xml files alongside _started_/_committed_ transaction
    markers left behind by past writes. Stat'ing the directory itself picks
    up mtime changes from those unrelated marker files (confirmed live: the
    fingerprint changed between two runs with an untouched part file,
    because leftover markers from an earlier chunking run touched the
    directory's own mtime) -- so this fingerprints the actual data file(s)
    inside instead, ignoring anything Spark-internal (leading underscore)."""
    if os.path.isdir(source_path):
        part_files = sorted(f for f in os.listdir(source_path) if not f.startswith("_"))
        stats = [os.stat(os.path.join(source_path, f)) for f in part_files]
        total_size = sum(s.st_size for s in stats)
        latest_mtime = max((int(s.st_mtime) for s in stats), default=0)
        return f"{total_size}:{latest_mtime}"
    stat = os.stat(source_path)
    return f"{stat.st_size}:{int(stat.st_mtime)}"


def pyspark_xml_already_complete(cfg: Dict[str, Any], env: str = "dev") -> bool:
    """True only if a prior run finished loading this exact version of the
    source file -- compares the recorded fingerprint against the file's
    current size/mtime, not just "does a marker exist," so an edited or
    replaced source file is correctly treated as needing a reload."""
    marker_path = _completion_marker_path(cfg, env)
    if not os.path.exists(marker_path):
        return False
    with open(marker_path, "r") as f:
        recorded = f.read().strip()
    return recorded == _source_fingerprint(cfg["path"])


def mark_pyspark_xml_complete(cfg: Dict[str, Any], env: str = "dev") -> None:
    marker_path = _completion_marker_path(cfg, env)
    # Unlike Auto Loader's checkpoint (which Spark creates automatically),
    # this is a plain file write -- the table-name subdirectory under
    # checkpoints_volume/ doesn't exist yet on a first run, and open()
    # doesn't create missing parent directories.
    os.makedirs(os.path.dirname(marker_path), exist_ok=True)
    with open(marker_path, "w") as f:
        f.write(_source_fingerprint(cfg["path"]))


def _write_bronze_table(df, target_table: str) -> None:
    """Kept separate from run_pyspark_xml() so the skip/force behavior is
    unit-testable without needing a real Unity Catalog table to write to."""
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)


def run_pyspark_xml(spark, source_key: str, env: str = "dev", force: bool = False) -> Dict[str, Any]:
    cfg = get_source_config(source_key, env=env)
    source_file = cfg["path"].rsplit("/", 1)[-1]

    if not force and pyspark_xml_already_complete(cfg, env):
        row_count = spark.table(cfg["target_table"]).count()
        return {"source_key": source_key, "target_table": cfg["target_table"], "row_count": row_count}

    df = read_xml(spark, cfg["path"], schema=get_source_schema(source_key), rowTag="record")
    for original in df.columns:
        sanitized = sanitize_column_name(original)
        if sanitized != original:
            df = df.withColumnRenamed(original, sanitized)

    # chunk4_xml already carries its own audit columns from chunking.py --
    # withColumn() overwrites same-named columns (see autoloader_ingest.py's
    # note), so this safely re-tags with Bronze's own load event.
    audited_df = add_audit_columns(df, source_format=cfg["format"], source_file=source_file)
    _write_bronze_table(audited_df, cfg["target_table"])
    mark_pyspark_xml_complete(cfg, env)

    row_count = spark.table(cfg["target_table"]).count()
    return {"source_key": source_key, "target_table": cfg["target_table"], "row_count": row_count}
```

### 2b. Edit `src/notebooks/04_bronze_pyspark_xml.py`

Add a `force` widget and thread it through. Change:
```python
dbutils.widgets.text("source_key", "chunk4_xml", "Source key (see sources.yml)")
dbutils.widgets.text("catalog", "vstone_traffic_dev", "Catalog")
dbutils.widgets.text("env", "dev", "Environment (dev/test/prod)")

source_key = dbutils.widgets.get("source_key")
catalog = dbutils.widgets.get("catalog")
env = dbutils.widgets.get("env")
```
to:
```python
dbutils.widgets.text("source_key", "chunk4_xml", "Source key (see sources.yml)")
dbutils.widgets.text("catalog", "vstone_traffic_dev", "Catalog")
dbutils.widgets.text("env", "dev", "Environment (dev/test/prod)")
dbutils.widgets.dropdown("force", "false", ["false", "true"], "Reprocess even if the source file hasn't changed")

source_key = dbutils.widgets.get("source_key")
catalog = dbutils.widgets.get("catalog")
env = dbutils.widgets.get("env")
force = dbutils.widgets.get("force") == "true"
```
And change the call:
```python
result = run_pyspark_xml(spark, source_key, env=env)
```
to:
```python
result = run_pyspark_xml(spark, source_key, env=env, force=force)
```

### 2c. Edit `resources/jobs/bronze_autoloader_pyspark_dlt_job.yml`

In the `pyspark_chunk4_xml` task's `base_parameters`, add one line:
```yaml
        - task_key: pyspark_chunk4_xml
          notebook_task:
            notebook_path: ../../src/notebooks/04_bronze_pyspark_xml.py
            base_parameters:
              source_key: chunk4_xml
              catalog: ${var.catalog}
              env: ${bundle.target}
              force: "false"
```

### 2d. Add tests to `tests/unit/test_pyspark_xml_ingest.py`

Add a module-scoped local Spark fixture near the top (after the existing imports):
```python
import pytest


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[2]").appName("vstone-pyspark-xml-tests").getOrCreate()
    yield session
    session.stop()
```

Then append these test functions at the end of the file:
```python
def _fake_cfg(tmp_path):
    return {
        "path": (tmp_path / "chunk4.xml").as_posix(),
        "format": "xml",
        "target_table": "cat.bronze.traffic_counts_pyspark",
    }


def test_pyspark_xml_already_complete_reflects_fingerprint_match(tmp_path, monkeypatch):
    import pipelines.bronze.pyspark_xml_ingest as xml_mod

    (tmp_path / "chunk4.xml").write_text("<records></records>")
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

    (chunk_dir / "_committed_456").write_text("more transaction metadata")
    assert _source_fingerprint(chunk_dir.as_posix()) == fingerprint_before

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
```

### Local verification for Task 2

```bash
flake8 src tests --max-line-length=120
pytest tests/unit -v
```
Expect flake8 silent and all tests passing (10 tests in `test_pyspark_xml_ingest.py` specifically).

### Real Databricks verification for Task 2 — follow this exact sequence, order matters

1. `databricks bundle validate -t dev` then `databricks bundle deploy -t dev`.
2. If a marker file already exists at `/Volumes/<catalog>/<ops_schema>/checkpoints_volume/traffic_counts_pyspark/_pyspark_xml_complete` from any earlier partial attempt, delete it first: `databricks fs rm "dbfs:/Volumes/<catalog>/<ops_schema>/checkpoints_volume/traffic_counts_pyspark/_pyspark_xml_complete"`.
3. Run the job: `databricks bundle run bronze_autoloader_pyspark_dlt_job -t dev`. This should succeed and do a real load (marker didn't exist).
4. Check the marker got created with a real fingerprint: `databricks fs cat "dbfs:/Volumes/<catalog>/<ops_schema>/checkpoints_volume/traffic_counts_pyspark/_pyspark_xml_complete"` — note the timestamp via `databricks fs ls -l` on that same path.
5. Run the job again (same command). It should succeed.
6. Re-check the marker's timestamp via `databricks fs ls -l` on the same path — **it must be unchanged** from step 4. If the timestamp changed, the skip did not work — do not proceed, investigate.
7. Confirm `force=true` genuinely bypasses the skip: `databricks bundle run bronze_autoloader_pyspark_dlt_job -t dev --params force=true` (or `--params '{"force":"true"}'` if the first form isn't accepted). Check the marker's timestamp again — it **must** be newer than step 6's.

---

## Git workflow for both tasks

Use two separate short-lived branches off `dev`, matching this project's established convention (one focused change per branch/PR):

```bash
git checkout dev && git pull
git checkout -b fix/dlt-streaming-table
# apply Task 1's file change
git add src/pipelines/bronze/dlt_traffic_counts.py
git commit -m "DLT: convert traffic_counts_dlt from a materialized view to a streaming
table via cloudFiles, matching Databricks' documented Bronze pattern
(streaming tables for ingestion, materialized views reserved for
Silver/Gold). Confirmed live: table_type is now STREAMING_TABLE, not
MATERIALIZED_VIEW -- required dropping the existing table first since
DLT rejects an in-place dataset-type change (CANNOT_CHANGE_DATASET_TYPE)."
git push -u origin fix/dlt-streaming-table
gh pr create --base dev --head fix/dlt-streaming-table \
  --title "DLT: convert Bronze table from materialized view to streaming table"
```

Then, after that's merged (or independently, on top of updated `dev`):

```bash
git checkout dev && git pull
git checkout -b fix/pyspark-xml-idempotency
# apply Task 2's 4 file changes
git add resources/jobs/bronze_autoloader_pyspark_dlt_job.yml src/notebooks/04_bronze_pyspark_xml.py src/pipelines/bronze/pyspark_xml_ingest.py tests/unit/test_pyspark_xml_ingest.py
git commit -m "PySpark XML: add skip-based idempotency (force default false), same
philosophy as chunking.py/copy_into.py's default-skip/force-reprocess
behavior. Completion marker lives in the ops volume, not raw, consistent
with the Auto Loader checkpoint relocation.

Fixed two real bugs found during live verification: (1) the marker write
failed on a first run because the table-name subdirectory under
checkpoints_volume/ didn't exist yet; (2) chunk4.xml is actually a Spark
output directory, and fingerprinting the directory's own mtime picked up
unrelated marker-file changes from past chunking runs, breaking the
skip-check -- fixed by fingerprinting the real part file(s) only."
git push -u origin fix/pyspark-xml-idempotency
gh pr create --base dev --head fix/pyspark-xml-idempotency \
  --title "PySpark XML: skip-based idempotency, matching the other 3 Bronze techniques"
```

Do not merge either PR yourself — wait for CI to go green and for me to review.
