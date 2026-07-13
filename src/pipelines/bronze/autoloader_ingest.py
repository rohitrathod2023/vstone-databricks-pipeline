"""
Auto Loader (cloudFiles) ingestion for chunk3_json -> Bronze. Auto Loader
tracks which files it has already processed via its checkpoint location, so
re-running this against the same source file is naturally idempotent --
same guarantee Day 2's COPY INTO has, via a different mechanism (streaming
trigger=AvailableNow + checkpoint, instead of COPY INTO's file-tracking
table).

chunk3.json shares its chunks/ folder with chunk1.csv/chunk2.csv/chunk4.xml.
Rather than pointing cloudFiles at the whole folder and trying to scope it
down with a filter option, .load() is given the exact file path -- Spark's
file source accepts a literal file path as validly as a directory or glob,
so Auto Loader only ever discovers this one file. No sibling-file collision,
no filter option needed, and chunk3.json never has to move from where
chunking.py already wrote it.

(An earlier attempt tried cloudFiles.pathGlobFilter to scope a directory
target instead -- that option got case-folded server-side into something
this Spark Connect runtime didn't recognize (CF_UNKNOWN_OPTION_KEYS_ERROR).
The exact-path approach here sidesteps that entirely rather than working
around it.)

    from pipelines.bronze.autoloader_ingest import run_autoloader
    result = run_autoloader(spark, "chunk3_json", env="dev")
"""
from __future__ import annotations

from typing import Any, Dict

from common.audit import add_audit_columns
from common.config_loader import get_env_config, get_source_config, get_source_schema


def _checkpoint_root(cfg: Dict[str, Any], env: str = "dev") -> str:
    """Lives in the dedicated ops schema/volume (see resources/catalog.yml),
    not the raw landing Volume -- a streaming checkpoint is pipeline
    operational state, not data, so it shouldn't be nested inside the raw
    zone (Unity Catalog also disallows nesting it under the target table's
    own storage). Keyed by target table name so two Auto Loader pipelines
    never share state."""
    catalog = cfg["target_table"].split(".")[0]
    ops_schema = get_env_config(env)["ops_schema"]
    table_name = cfg["target_table"].rsplit(".", 1)[-1]
    return f"/Volumes/{catalog}/{ops_schema}/checkpoints_volume/{table_name}"


def checkpoint_path(cfg: Dict[str, Any], env: str = "dev") -> str:
    return f"{_checkpoint_root(cfg, env)}/checkpoint"


def schema_location(cfg: Dict[str, Any], env: str = "dev") -> str:
    """Deliberately a different path from checkpoint_path() -- per Databricks'
    own guidance, conflating checkpointLocation and cloudFiles.schemaLocation
    makes it impossible to clear schema-evolution state without also
    discarding the checkpoint's exactly-once processing history."""
    return f"{_checkpoint_root(cfg, env)}/schema"


def build_autoloader_options(cfg: Dict[str, Any], schema_loc: str) -> Dict[str, str]:
    """Pure construction, no Spark session needed -- kept separate from
    run_autoloader() so the options dict is unit-testable on its own."""
    return {
        "cloudFiles.format": cfg["format"],
        "cloudFiles.schemaLocation": schema_loc,
    }


def run_autoloader(spark, source_key: str, env: str = "dev") -> Dict[str, Any]:
    cfg = get_source_config(source_key, env=env)
    source_file = cfg["path"].rsplit("/", 1)[-1]
    checkpoint = checkpoint_path(cfg, env)
    schema_loc = schema_location(cfg, env)

    # Explicit, permissive (string-typed) schema -- see config/schemas.py.
    # cloudFiles.schemaLocation still tracks schema evolution/rescue *beyond*
    # this base schema -- that's a different concern from inference and is
    # still worth keeping alongside it.
    stream_reader = spark.readStream.format("cloudFiles").schema(get_source_schema(source_key))
    for key, value in build_autoloader_options(cfg, schema_loc).items():
        stream_reader = stream_reader.option(key, value)
    # .load() is given the exact file path (cfg["path"]), not the parent
    # chunks/ directory -- see module docstring for why.
    stream_df = stream_reader.load(cfg["path"])

    # chunk3_json already carries load_dt/source_format/source_file/run_id
    # from chunking.py's own add_audit_columns() call -- withColumn()
    # overwrites an existing column of the same name (unlike SQL's
    # SELECT *, x AS col, which errors on a duplicate), so calling this
    # again here safely re-tags with Bronze's own load event. This is only
    # an issue for Day 2's SQL-based COPY INTO, not the DataFrame API used here.
    audited_df = add_audit_columns(stream_df, source_format=cfg["format"], source_file=source_file)

    query = (
        audited_df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(cfg["target_table"])
    )
    query.awaitTermination()

    row_count = spark.table(cfg["target_table"]).count()
    return {"source_key": source_key, "target_table": cfg["target_table"], "row_count": row_count}
