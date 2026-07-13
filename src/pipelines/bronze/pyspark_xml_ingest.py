"""
Plain PySpark read/write for chunk4_xml -> Bronze. Unlike COPY INTO (Day 2)
and Auto Loader/DLT (this file's siblings), a plain read/write has no
built-in rerun-safety of its own -- mode="overwrite" is the explicit fix:
chunk4.xml is read and rewritten wholesale on every run (not incrementally),
so there's no "new rows since last run" concept a keyed MERGE would need to
reconcile -- overwrite is both simpler to justify and the correct choice
here, not just the easy one.

Note: the reference column-sanitizing fix mentioned for this technique
(XML_Data_Read_and_Write.py/.ipynb, Section 7 course assets) isn't present
in this repo -- this reimplements the same standard fix (invalid XML
tag/attribute characters aren't valid Delta/Parquet column names) rather
than copying unseen code.

    from pipelines.bronze.pyspark_xml_ingest import run_pyspark_xml
    result = run_pyspark_xml(spark, "chunk4_xml", env="dev")
"""
from __future__ import annotations

import re
from typing import Any, Dict

from common.audit import add_audit_columns
from common.config_loader import get_source_config, get_source_schema
from common.io_readers import read_xml

_INVALID_COLUMN_CHARS = re.compile(r"[^0-9a-zA-Z_]")


def sanitize_column_name(name: str) -> str:
    """XML tag/attribute names can contain characters Delta/Parquet column
    names can't (spaces, '-', ':', etc.) -- replace anything that isn't
    alphanumeric or underscore with '_'."""
    return _INVALID_COLUMN_CHARS.sub("_", name)


def run_pyspark_xml(spark, source_key: str, env: str = "dev") -> Dict[str, Any]:
    cfg = get_source_config(source_key, env=env)
    source_file = cfg["path"].rsplit("/", 1)[-1]

    df = read_xml(spark, cfg["path"], schema=get_source_schema(source_key), rowTag="record")
    for original in df.columns:
        sanitized = sanitize_column_name(original)
        if sanitized != original:
            df = df.withColumnRenamed(original, sanitized)

    # chunk4_xml already carries its own audit columns from chunking.py --
    # withColumn() overwrites same-named columns (see autoloader_ingest.py's
    # note), so this safely re-tags with Bronze's own load event.
    audited_df = add_audit_columns(df, source_format=cfg["format"], source_file=source_file)
    audited_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(cfg["target_table"])

    row_count = spark.table(cfg["target_table"]).count()
    return {"source_key": source_key, "target_table": cfg["target_table"], "row_count": row_count}
