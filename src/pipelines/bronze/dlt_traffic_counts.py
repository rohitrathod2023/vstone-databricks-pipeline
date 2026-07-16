# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze DLT — chunk2_csv -> traffic_counts_dlt
# MAGIC DLT's own `@dlt.expect` is the native data-quality mechanism for this
# MAGIC technique — `utils.sanity_checks.run_sanity_check` (used by every other
# MAGIC Bronze pipeline) is deliberately NOT used here, since bolting an
# MAGIC imperative check onto a declarative `@dlt.table` function would fight
# MAGIC the framework rather than use its own DQ tooling. Table comments still
# MAGIC come from `utils.metadata`'s config-driven pattern via `comment=`.
# MAGIC
# MAGIC Catalog/target schema are set at the pipeline level (see
# MAGIC `resources/pipelines/bronze_dlt_pipeline.yml`), not in this file — DLT
# MAGIC tables are declared with their bare name only.
# MAGIC
# MAGIC **Streaming table, not a materialized view.** A `@dlt.table` function
# MAGIC that does a plain `spark.read` becomes a materialized view (DLT fully
# MAGIC recomputes it from scratch every run) -- Databricks' own documented
# MAGIC guidance is that Bronze should be a streaming table instead (append-only,
# MAGIC incremental), with materialized views reserved for Silver/Gold
# MAGIC transformations. The read below uses `spark.readStream.format("cloudFiles")`
# MAGIC specifically so this table is a genuine streaming table. Lakeflow
# MAGIC pipelines manage the Auto Loader schema/checkpoint location automatically
# MAGIC under the pipeline's own storage root -- no `cloudFiles.schemaLocation`
# MAGIC or checkpoint option is set here, unlike the standalone Auto Loader
# MAGIC module (`autoloader_ingest.py`), which isn't running inside a pipeline
# MAGIC and has to manage that itself.

# COMMAND ----------

import os
import sys

NOTEBOOK_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(NOTEBOOK_DIR, "..", "..", ".."))
SRC_DIR = os.path.join(REPO_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# COMMAND ----------

import dlt  # noqa: E402

from utils.audit import add_audit_columns  # noqa: E402
from utils.config_loader import get_source_config, get_source_schema  # noqa: E402

_ENV = spark.conf.get("env", "dev")
_CFG = get_source_config("chunk2_csv", env=_ENV)
_SOURCE_FILE = _CFG["path"].rsplit("/", 1)[-1]

# COMMAND ----------


@dlt.table(
    name="traffic_counts_dlt",
    comment=_CFG["description"],
)
@dlt.expect("non_null_enter_exit", "enter IS NOT NULL AND exit IS NOT NULL")
@dlt.expect("non_null_date", "date IS NOT NULL")
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
