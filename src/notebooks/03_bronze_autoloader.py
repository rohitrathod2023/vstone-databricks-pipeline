# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Auto Loader
# MAGIC Thin entrypoint for chunk3_json's Auto Loader ingestion — real logic
# MAGIC lives in `src/pipelines/bronze/autoloader_ingest.py` so it's
# MAGIC unit-testable outside a notebook (checkpoint-path and options
# MAGIC construction, at least — the streaming read itself needs a live cluster).

# COMMAND ----------

import os
import sys

NOTEBOOK_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(NOTEBOOK_DIR, "..", ".."))
SRC_DIR = os.path.join(REPO_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# COMMAND ----------

dbutils.widgets.text("source_key", "chunk3_json", "Source key (see sources.yml)")
dbutils.widgets.text("catalog", "vstone_traffic_dev", "Catalog")
dbutils.widgets.text("env", "dev", "Environment (dev/test/prod)")

source_key = dbutils.widgets.get("source_key")
catalog = dbutils.widgets.get("catalog")
env = dbutils.widgets.get("env")

# COMMAND ----------

from utils.config_loader import get_column_comments, get_source_config
from utils.logger import get_logger
from utils.metadata import apply_table_comments
from utils.sanity_checks import run_sanity_check
from pipelines.bronze.autoloader_ingest import run_autoloader

log = get_logger(__name__, catalog=catalog, job_name="Bronze Auto Loader")
log.info(f"Starting Auto Loader — source_key={source_key}, env={env}")

# COMMAND ----------

cfg = get_source_config(source_key, env=env)
result = run_autoloader(spark, source_key, env=env)
log.info(f"Loaded into {result['target_table']}: {result['row_count']} rows")

# COMMAND ----------

apply_table_comments(spark, cfg["target_table"], cfg["description"], get_column_comments(source_key))
log.info(f"Applied table + column comments to {cfg['target_table']}")

# COMMAND ----------

sanity = run_sanity_check(spark, cfg["target_table"], catalog=catalog, job_name="Bronze Auto Loader")
if not sanity["passed"]:
    raise RuntimeError(f"Sanity check failed for {cfg['target_table']}: {sanity['failures']}")

log.info(f"Bronze Auto Loader complete for {source_key}: {result}")
