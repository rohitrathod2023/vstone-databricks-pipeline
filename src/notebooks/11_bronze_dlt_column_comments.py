# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze DLT — column comment application
# MAGIC `traffic_counts_dlt` (chunk2_csv) never declares an explicit `schema=`
# MAGIC in its `@dlt.table(...)` call (see `pipelines/bronze/dlt_traffic_counts.py`)
# MAGIC — it infers its schema from the DataFrame the function returns, so
# MAGIC there's no DDL string to attach column comments to the way Gold's
# MAGIC `table_schemas.py` does. `COMMENT ON COLUMN` runs fine against it
# MAGIC post-creation since it's a genuine streaming table (a real Delta table
# MAGIC in Unity Catalog), not a materialized view registered as a VIEW object
# MAGIC the way Gold's tables are.
# MAGIC
# MAGIC Run as a task depending on the Bronze DLT pipeline task in
# MAGIC `bronze_autoloader_pyspark_dlt_job.yml`, so it only runs after
# MAGIC `traffic_counts_dlt` actually exists.

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

from utils.config_loader import get_column_comments, get_source_config
from utils.logger import get_logger
from utils.metadata import apply_table_comments

log = get_logger(__name__, catalog=catalog, job_name="Bronze DLT Column Comments")

# COMMAND ----------

cfg = get_source_config("chunk2_csv", env=env)
apply_table_comments(spark, cfg["target_table"], cfg["description"], get_column_comments("chunk2_csv"))
log.info(f"Applied table + column comments to {cfg['target_table']}")
