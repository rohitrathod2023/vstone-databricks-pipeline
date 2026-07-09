"""
Strategy-pattern dispatch for "read this source, regardless of format."

Adding a new format later (say Parquet) means adding one function + one
registry entry here — every notebook and job that calls read_source() picks
it up automatically, nothing else in the repo changes.

    from common.io_readers import read_source
    df = read_source(spark, source_config)   # source_config comes from config_loader.get_source_config(...)

Day 1 only needs CSV/JSON/XML read paths to exist (chunking splits cars.csv,
which is plain CSV read/write). The Auto Loader / DLT / COPY INTO *ingestion*
techniques are Day 2-3 work and get filled in here without touching the
chunking code, since chunking only ever calls read_csv/write_csv directly.
"""
from __future__ import annotations

from typing import Any, Callable, Dict


def read_csv(spark, path: str, **options):
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .options(**options)
        .load(path)
    )


def read_json(spark, path: str, **options):
    return spark.read.format("json").options(**options).load(path)


def read_xml(spark, path: str, **options):
    # Requires the spark-xml package (com.databricks:spark-xml_2.12) on the cluster/
    # serverless environment. rowTag matches the row-per-record shape written by
    # chunking.py — keep this in sync with write_xml()'s row_tag.
    return (
        spark.read.format("xml")
        .option("rowTag", options.pop("rowTag", "record"))
        .options(**options)
        .load(path)
    )


def write_csv(df, path: str, mode: str = "overwrite", **options):
    df.write.format("csv").option("header", "true").mode(mode).options(**options).save(path)


def write_json(df, path: str, mode: str = "overwrite", **options):
    df.write.format("json").mode(mode).options(**options).save(path)


def write_xml(df, path: str, mode: str = "overwrite", row_tag: str = "record", **options):
    (
        df.write.format("xml")
        .option("rowTag", row_tag)
        .mode(mode)
        .options(**options)
        .save(path)
    )


READERS: Dict[str, Callable[..., Any]] = {
    "csv": read_csv,
    "json": read_json,
    "xml": read_xml,
}

WRITERS: Dict[str, Callable[..., Any]] = {
    "csv": write_csv,
    "json": write_json,
    "xml": write_xml,
}


def read_source(spark, source_config: dict, **options):
    fmt = source_config["format"]
    if fmt not in READERS:
        raise ValueError(f"No reader registered for format '{fmt}'. Known: {sorted(READERS)}")
    return READERS[fmt](spark, source_config["path"], **options)


def write_source(df, source_config: dict, mode: str = "overwrite", **options):
    fmt = source_config["format"]
    if fmt not in WRITERS:
        raise ValueError(f"No writer registered for format '{fmt}'. Known: {sorted(WRITERS)}")
    return WRITERS[fmt](df, source_config["path"], mode=mode, **options)
