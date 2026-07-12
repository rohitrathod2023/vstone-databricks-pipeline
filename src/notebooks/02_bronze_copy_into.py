# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — COPY INTO — Day 2
# MAGIC One shared, parameterized notebook for every COPY INTO Bronze load. The
# MAGIC job (`bronze_copy_into_job`) runs this same notebook 5 times with a
# MAGIC different `source_key` each time (`chunk1_csv` + the 4 un-chunked raw
# MAGIC files) instead of 5 near-duplicate notebooks — all real logic lives in
# MAGIC `src/pipelines/bronze/copy_into.py` so it's unit-testable outside a
# MAGIC notebook (see `tests/unit/test_copy_into.py`).

# COMMAND ----------

import os
import sys

NOTEBOOK_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(NOTEBOOK_DIR, "..", ".."))
SRC_DIR = os.path.join(REPO_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# COMMAND ----------

dbutils.widgets.text("source_key", "chunk1_csv", "Source key (see sources.yml)")
dbutils.widgets.text("catalog", "vstone_traffic_dev", "Catalog")
dbutils.widgets.text("env", "dev", "Environment (dev/test/prod)")

source_key = dbutils.widgets.get("source_key")
catalog = dbutils.widgets.get("catalog")
env = dbutils.widgets.get("env")

# COMMAND ----------

from common.config_loader import get_source_config
from common.logger import get_logger
from common.metadata import apply_table_comments
from common.sanity_checks import run_sanity_check
from pipelines.bronze.copy_into import run_copy_into

log = get_logger(__name__, catalog=catalog, job_name="Bronze COPY INTO")
log.info(f"Starting COPY INTO — source_key={source_key}, env={env}")

# COMMAND ----------

cfg = get_source_config(source_key, env=env)
result = run_copy_into(spark, source_key, env=env)
log.info(f"Loaded into {result['target_table']}: {result['row_count']} rows, run_id={result['run_id']}")

# COMMAND ----------

apply_table_comments(spark, cfg["target_table"], cfg["description"])
log.info(f"Applied table comment to {cfg['target_table']}")

# COMMAND ----------

sanity = run_sanity_check(spark, cfg["target_table"], catalog=catalog, job_name="Bronze COPY INTO")
if not sanity["passed"]:
    raise RuntimeError(f"Sanity check failed for {cfg['target_table']}: {sanity['failures']}")

log.info(f"Bronze COPY INTO complete for {source_key}: {result}")
