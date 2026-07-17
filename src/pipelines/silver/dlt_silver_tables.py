# Databricks notebook source
# MAGIC %md
# MAGIC # Silver DLT pipeline — dimension staging tables (Phase 3)
# MAGIC Every `@dlt.table` function here is a thin wrapper -- real transformation
# MAGIC logic lives in plain, pytest-testable functions under
# MAGIC `src/pipelines/silver/*.py` (locations.py, streets.py, quarantine.py,
# MAGIC header_standardization.py, text_normalization.py), same
# MAGIC notebook/pipeline separation pattern already used for Bronze.
# MAGIC
# MAGIC **Streaming tables, not materialized views.** Every function's query is a
# MAGIC `spark.readStream`, which is what makes DLT treat each as a streaming
# MAGIC table (same mechanism used for Bronze's `traffic_counts_dlt`). Every
# MAGIC table here is a row-level operation (typing, header/value
# MAGIC standardization, quarantine split) -- no joins or aggregations happen at
# MAGIC Silver, that's Gold's job.
# MAGIC
# MAGIC **Quarantine pattern**: a temporary streaming table computes a
# MAGIC `rejection_reason` column once, and two downstream streaming tables
# MAGIC filter it into the valid output and the rejected output -- not two
# MAGIC independent recomputations of the same check.
# MAGIC
# MAGIC Catalog/target schema are set at the pipeline level (see
# MAGIC `resources/pipelines/silver_dlt_pipeline.yml`), not in this file --
# MAGIC same convention as the Bronze DLT pipeline.
# MAGIC
# MAGIC **`import dlt` (legacy), not `from pyspark import pipelines as dp`** --
# MAGIC matches Bronze's DLT pipeline (dlt_traffic_counts.py), which is the
# MAGIC proven-working technique in this workspace.

# COMMAND ----------

import os
import sys


def _find_src_dir(start: str) -> str:
    """Walk upward from the notebook's own directory looking for the `src/`
    root (identified by a `pipelines` package inside it) instead of assuming
    a fixed number of parent levels -- this notebook's canonical location is
    src/pipelines/silver/, but it may also be deployed as a direct-imported
    copy under src/notebooks/, which sits at a different depth under src/.
    """
    current = start
    for _ in range(6):
        if os.path.isdir(os.path.join(current, "pipelines")):
            return current
        if os.path.isdir(os.path.join(current, "src", "pipelines")):
            return os.path.join(current, "src")
        current = os.path.dirname(current)
    raise RuntimeError(f"Could not locate src/ from {start}")


NOTEBOOK_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
SRC_DIR = _find_src_dir(NOTEBOOK_DIR)
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# COMMAND ----------

import dlt  # noqa: E402

from pipelines.silver.environment import build_checked_environment  # noqa: E402
from pipelines.silver.locations import build_checked_locations  # noqa: E402
from pipelines.silver.quarantine import rejected_rows, valid_rows  # noqa: E402
from pipelines.silver.streets import build_checked_streets  # noqa: E402
from pipelines.silver.telegram import build_checked_telegram  # noqa: E402
from pipelines.silver.traffic import SOURCE_TECHNIQUES, build_checked_traffic  # noqa: E402
from utils.config_loader import get_source_config  # noqa: E402

_ENV = spark.conf.get("env", "dev")

_LOCATIONS_TABLE = get_source_config("bronze_node_locations", env=_ENV)["target_table"]
_STREETS_TABLE = get_source_config("bronze_streets_list", env=_ENV)["target_table"]
_ENVIRONMENT_TABLE = get_source_config("bronze_streets", env=_ENV)["target_table"]
_TELEGRAM_TABLE = get_source_config("bronze_telegram", env=_ENV)["target_table"]

# SOURCE_TECHNIQUES order (copyinto, dlt, autoloader, pyspark) maps to these
# 4 Bronze source_keys in the same order -- see traffic.py.
_TRAFFIC_SOURCE_KEYS = ("chunk1_csv", "chunk2_csv", "chunk3_json", "chunk4_xml")
_TRAFFIC_TABLES = [get_source_config(k, env=_ENV)["target_table"] for k in _TRAFFIC_SOURCE_KEYS]

_SILVER_LOCATIONS_CFG = get_source_config("silver_locations", env=_ENV)
_SILVER_LOCATIONS_REJECTED_CFG = get_source_config("silver_locations_rejected", env=_ENV)
_SILVER_STREETS_CFG = get_source_config("silver_streets", env=_ENV)
_SILVER_STREETS_REJECTED_CFG = get_source_config("silver_streets_rejected", env=_ENV)
_SILVER_TRAFFIC_CFG = get_source_config("silver_traffic", env=_ENV)
_SILVER_TRAFFIC_REJECTED_CFG = get_source_config("silver_traffic_rejected", env=_ENV)
_SILVER_ENVIRONMENT_CFG = get_source_config("silver_environment", env=_ENV)
_SILVER_ENVIRONMENT_REJECTED_CFG = get_source_config("silver_environment_rejected", env=_ENV)
_SILVER_TELEGRAM_CFG = get_source_config("silver_telegram", env=_ENV)
_SILVER_TELEGRAM_REJECTED_CFG = get_source_config("silver_telegram_rejected", env=_ENV)

# COMMAND ----------

# DBTITLE 1,silver_locations tables
# silver_locations, quarantine split


@dlt.table(temporary=True)
def _locations_checked():
    df = spark.readStream.table(_LOCATIONS_TABLE)
    return build_checked_locations(df)


@dlt.table(name="silver_locations", comment=_SILVER_LOCATIONS_CFG["description"])
def silver_locations():
    return valid_rows(spark.readStream.table("_locations_checked"))


@dlt.table(name="silver_locations_rejected", comment=_SILVER_LOCATIONS_REJECTED_CFG["description"])
def silver_locations_rejected():
    return rejected_rows(spark.readStream.table("_locations_checked"))


# COMMAND ----------

# DBTITLE 1,silver_streets tables
# silver_streets, quarantine split


@dlt.table(temporary=True)
def _streets_checked():
    df = spark.readStream.table(_STREETS_TABLE)
    return build_checked_streets(df)


@dlt.table(name="silver_streets", comment=_SILVER_STREETS_CFG["description"])
def silver_streets():
    return valid_rows(spark.readStream.table("_streets_checked"))


@dlt.table(name="silver_streets_rejected", comment=_SILVER_STREETS_REJECTED_CFG["description"])
def silver_streets_rejected():
    return rejected_rows(spark.readStream.table("_streets_checked"))


# COMMAND ----------

# DBTITLE 1,silver_traffic tables
# silver_traffic, UNION ALL of the 4 Bronze traffic tables + quarantine split


@dlt.table(temporary=True)
def _traffic_checked():
    # Order matches SOURCE_TECHNIQUES (copyinto, dlt, autoloader, pyspark) --
    # see traffic.py.
    bronze_dfs = [spark.readStream.table(t) for t in _TRAFFIC_TABLES]
    return build_checked_traffic(bronze_dfs)


@dlt.table(name="silver_traffic", comment=_SILVER_TRAFFIC_CFG["description"])
def silver_traffic():
    return valid_rows(spark.readStream.table("_traffic_checked"))


@dlt.table(name="silver_traffic_rejected", comment=_SILVER_TRAFFIC_REJECTED_CFG["description"])
def silver_traffic_rejected():
    return rejected_rows(spark.readStream.table("_traffic_checked"))


# COMMAND ----------

# DBTITLE 1,silver_environment tables
# silver_environment, quarantine split


@dlt.table(temporary=True)
def _environment_checked():
    df = spark.readStream.table(_ENVIRONMENT_TABLE)
    return build_checked_environment(df)


@dlt.table(name="silver_environment", comment=_SILVER_ENVIRONMENT_CFG["description"])
def silver_environment():
    return valid_rows(spark.readStream.table("_environment_checked"))


@dlt.table(name="silver_environment_rejected", comment=_SILVER_ENVIRONMENT_REJECTED_CFG["description"])
def silver_environment_rejected():
    return rejected_rows(spark.readStream.table("_environment_checked"))


# COMMAND ----------

# DBTITLE 1,silver_telegram tables
# silver_telegram, quarantine split


@dlt.table(temporary=True)
def _telegram_checked():
    df = spark.readStream.table(_TELEGRAM_TABLE)
    return build_checked_telegram(df)


@dlt.table(name="silver_telegram", comment=_SILVER_TELEGRAM_CFG["description"])
def silver_telegram():
    return valid_rows(spark.readStream.table("_telegram_checked"))


@dlt.table(name="silver_telegram_rejected", comment=_SILVER_TELEGRAM_REJECTED_CFG["description"])
def silver_telegram_rejected():
    return rejected_rows(spark.readStream.table("_telegram_checked"))