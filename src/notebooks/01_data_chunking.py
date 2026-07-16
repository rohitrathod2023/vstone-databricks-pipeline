# Databricks notebook source
# MAGIC %md
# MAGIC # Data Chunking — Day 1
# MAGIC Thin entrypoint notebook for the **"Data Chunking"** DABs job.
# MAGIC All real logic lives in `src/pipelines/ingestion/chunking.py` so it's unit-testable
# MAGIC outside of a notebook — this cell just wires job parameters to that function.

# COMMAND ----------

import sys
import os

# Databricks Repos puts the repo root on disk but not automatically on sys.path
# for plain (non-package-install) imports — add src/ once, here, for every notebook.
NOTEBOOK_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(NOTEBOOK_DIR, "..", ".."))
SRC_DIR = os.path.join(REPO_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# COMMAND ----------

dbutils.widgets.text("catalog", "vstone_traffic_dev", "Catalog")
dbutils.widgets.text("env", "dev", "Environment (dev/test/prod)")
dbutils.widgets.dropdown("force", "false", ["false", "true"], "Reprocess even if chunks already exist")

catalog = dbutils.widgets.get("catalog")
env = dbutils.widgets.get("env")
force = dbutils.widgets.get("force") == "true"

# COMMAND ----------

from pipelines.ingestion.chunking import run
from utils.logger import get_logger

log = get_logger(__name__, catalog=catalog, job_name="Data Chunking")
log.info(f"Starting Data Chunking job — env={env}, catalog={catalog}, force={force}")

# COMMAND ----------

results = run(spark, env=env, force=force)
log.info(f"Data Chunking job finished successfully: {results}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanity check
# MAGIC Quick manual verification cell — run counts should roughly match the 40/30/20/10 split
# MAGIC and sum back to the total row count of `cars.csv`.

# COMMAND ----------

total = sum(results.values())
for chunk_key, count in results.items():
    pct = round(100 * count / total, 1) if total else 0
    print(f"{chunk_key:>14}: {count:>10,} rows  ({pct}%)")
print(f"{'TOTAL':>14}: {total:>10,} rows")
