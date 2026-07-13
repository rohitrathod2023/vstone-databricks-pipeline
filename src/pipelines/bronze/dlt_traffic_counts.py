# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze DLT — chunk2_csv -> traffic_counts_dlt
# MAGIC DLT's own `@dlt.expect` is the native data-quality mechanism for this
# MAGIC technique — `common.sanity_checks.run_sanity_check` (used by every other
# MAGIC Bronze pipeline) is deliberately NOT used here, since bolting an
# MAGIC imperative check onto a declarative `@dlt.table` function would fight
# MAGIC the framework rather than use its own DQ tooling. Table comments still
# MAGIC come from `common.metadata`'s config-driven pattern via `comment=`.
# MAGIC
# MAGIC Catalog/target schema are set at the pipeline level (see
# MAGIC `resources/pipelines/bronze_dlt_pipeline.yml`), not in this file — DLT
# MAGIC tables are declared with their bare name only.

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

from common.audit import add_audit_columns  # noqa: E402
from common.config_loader import get_source_config, get_source_schema  # noqa: E402

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
    # Explicit, permissive (string-typed) schema -- see config/schemas.py.
    # Strict typing/validation is deferred to Silver, not done at Bronze.
    df = spark.read.option("header", "true").schema(get_source_schema("chunk2_csv")).csv(_CFG["path"])
    # chunk2_csv already carries its own audit columns from chunking.py --
    # withColumn() overwrites same-named columns (see autoloader_ingest.py's
    # note), so this safely re-tags with Bronze's own load event.
    return add_audit_columns(df, source_format=_CFG["format"], source_file=_SOURCE_FILE)
