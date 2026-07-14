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
