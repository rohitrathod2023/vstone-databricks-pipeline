# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — column comment application
# MAGIC None of Silver's 10 `@dlt.table(...)` calls (see
# MAGIC `pipelines/silver/dlt_silver_tables.py`) declare an explicit `schema=`
# MAGIC either — same situation as Bronze's `traffic_counts_dlt`
# MAGIC (`11_bronze_dlt_column_comments.py`), same fix: `COMMENT ON COLUMN`
# MAGIC applied post-creation, safe since these are streaming tables (real
# MAGIC Delta tables), not materialized views.
# MAGIC
# MAGIC Run as a task depending on the Silver DLT pipeline task in
# MAGIC `silver_job.yml`, so it only runs after all 10 tables actually exist.

# COMMAND ----------

import os
import sys

NOTEBOOK_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(NOTEBOOK_DIR, "..", ".."))
SRC_DIR = os.path.join(REPO_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# COMMAND ----------

dbutils.widgets.text("catalog", "vstone_traffic_dev", "Catalog")
dbutils.widgets.text("env", "dev", "Environment (dev/test/prod)")

catalog = dbutils.widgets.get("catalog")
env = dbutils.widgets.get("env")

# COMMAND ----------

from utils.config_loader import get_silver_column_comments, get_source_config
from utils.logger import get_logger
from utils.metadata import apply_table_comments

log = get_logger(__name__, catalog=catalog, job_name="Silver Column Comments")

# COMMAND ----------

SILVER_SOURCE_KEYS = (
    "silver_locations",
    "silver_locations_rejected",
    "silver_streets",
    "silver_streets_rejected",
    "silver_traffic",
    "silver_traffic_rejected",
    "silver_environment",
    "silver_environment_rejected",
    "silver_telegram",
    "silver_telegram_rejected",
)

for source_key in SILVER_SOURCE_KEYS:
    cfg = get_source_config(source_key, env=env)
    apply_table_comments(spark, cfg["target_table"], cfg["description"], get_silver_column_comments(source_key))
    log.info(f"Applied table + column comments to {cfg['target_table']}")
