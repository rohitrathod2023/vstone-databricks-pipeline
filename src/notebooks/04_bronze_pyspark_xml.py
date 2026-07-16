# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — PySpark read/write
# MAGIC Thin entrypoint for chunk4_xml's plain PySpark read/write ingestion —
# MAGIC real logic (including column-name sanitizing) lives in
# MAGIC `src/pipelines/bronze/pyspark_xml_ingest.py` so it's unit-testable
# MAGIC outside a notebook.

# COMMAND ----------

import os
import sys

NOTEBOOK_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(NOTEBOOK_DIR, "..", ".."))
SRC_DIR = os.path.join(REPO_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# COMMAND ----------

dbutils.widgets.text("source_key", "chunk4_xml", "Source key (see sources.yml)")
dbutils.widgets.text("catalog", "vstone_traffic_dev", "Catalog")
dbutils.widgets.text("env", "dev", "Environment (dev/test/prod)")
dbutils.widgets.dropdown("force", "false", ["false", "true"], "Reprocess even if the source file hasn't changed")

source_key = dbutils.widgets.get("source_key")
catalog = dbutils.widgets.get("catalog")
env = dbutils.widgets.get("env")
force = dbutils.widgets.get("force") == "true"

# COMMAND ----------

from utils.config_loader import get_source_config
from utils.logger import get_logger
from utils.metadata import apply_table_comments
from utils.sanity_checks import run_sanity_check
from pipelines.bronze.pyspark_xml_ingest import run_pyspark_xml

log = get_logger(__name__, catalog=catalog, job_name="Bronze PySpark XML")
log.info(f"Starting PySpark XML read/write — source_key={source_key}, env={env}")

# COMMAND ----------

cfg = get_source_config(source_key, env=env)
result = run_pyspark_xml(spark, source_key, env=env, force=force)
log.info(f"Loaded into {result['target_table']}: {result['row_count']} rows")

# COMMAND ----------

apply_table_comments(spark, cfg["target_table"], cfg["description"])
log.info(f"Applied table comment to {cfg['target_table']}")

# COMMAND ----------

sanity = run_sanity_check(spark, cfg["target_table"], catalog=catalog, job_name="Bronze PySpark XML")
if not sanity["passed"]:
    raise RuntimeError(f"Sanity check failed for {cfg['target_table']}: {sanity['failures']}")

log.info(f"Bronze PySpark XML complete for {source_key}: {result}")
