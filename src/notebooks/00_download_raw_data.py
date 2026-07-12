# Databricks notebook source
# MAGIC %md
# MAGIC # Download Raw Data (Kaggle -> Volume) — Day 1 prerequisite
# MAGIC Downloads the 5 raw source files from Kaggle
# MAGIC ([xxjcaxx/trafficsimulator](https://www.kaggle.com/datasets/xxjcaxx/trafficsimulator))
# MAGIC **directly into the raw Volume** — no laptop involved, no 5GB UI upload
# MAGIC limit. That 5GB cap is a Catalog Explorer drag-and-drop UI limit, not a
# MAGIC Volume storage limit — Volumes support local file API access from
# MAGIC cluster/serverless compute, so a plain file write here has no such cap.
# MAGIC
# MAGIC **One-time setup before running this:** put your Kaggle API token in a
# MAGIC Databricks secret instead of typing it into a notebook (never as a widget
# MAGIC value or hardcoded literal — those are visible in job run history):
# MAGIC
# MAGIC ```bash
# MAGIC databricks secrets create-scope kaggle
# MAGIC databricks secrets put-secret kaggle api_token
# MAGIC # paste your token from kaggle.com/settings/api when prompted
# MAGIC ```
# MAGIC
# MAGIC **Test with the small files first** (default below) before trusting this
# MAGIC with `streets.csv` (7.8GB) — confirms serverless compute can actually reach
# MAGIC Kaggle's API before committing to a large transfer.

# COMMAND ----------

# MAGIC %pip install kaggle
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import sys
import zipfile

NOTEBOOK_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(NOTEBOOK_DIR, "..", ".."))
SRC_DIR = os.path.join(REPO_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# COMMAND ----------

dbutils.widgets.text("catalog", "vstone_traffic_dev", "Catalog")
dbutils.widgets.text("schema", "dev_rohitrathodcomp_raw", "Schema")
dbutils.widgets.text("volume", "raw_volume", "Volume")
dbutils.widgets.text("dataset", "xxjcaxx/trafficsimulator", "Kaggle dataset ref")
dbutils.widgets.text(
    "files",
    "node_locations.csv,streets_list.csv",
    "Comma-separated files to download (start small, add cars.csv/telegram.csv/streets.csv once confirmed working)",
)
dbutils.widgets.text("kaggle_secret_scope", "kaggle", "Databricks secret scope holding the Kaggle token")
dbutils.widgets.text("kaggle_secret_key", "api_token", "Secret key name within that scope")
dbutils.widgets.dropdown("force", "false", ["false", "true"], "Re-download even if file already exists")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
dataset = dbutils.widgets.get("dataset")
files = [f.strip() for f in dbutils.widgets.get("files").split(",") if f.strip()]
force = dbutils.widgets.get("force") == "true"

volume_root = f"/Volumes/{catalog}/{schema}/{volume}/incoming"
os.makedirs(volume_root, exist_ok=True)

# COMMAND ----------

from common.logger import get_logger  # noqa: E402

log = get_logger(__name__, catalog=catalog, job_name="Download Raw Data")
log.info(f"Target: {volume_root}")
log.info(f"Files requested: {files}")

# COMMAND ----------

os.environ["KAGGLE_API_TOKEN"] = dbutils.secrets.get(
    scope=dbutils.widgets.get("kaggle_secret_scope"), key=dbutils.widgets.get("kaggle_secret_key")
)

from kaggle.api.kaggle_api_extended import KaggleApi  # noqa: E402

api = KaggleApi()
api.authenticate()
log.info("Kaggle authentication OK")

# COMMAND ----------

results = {"skipped": [], "downloaded": [], "failed": []}

for filename in files:
    target_path = os.path.join(volume_root, filename)

    if not force and os.path.exists(target_path):
        log.info(f"Skipping {filename} — already present in the volume")
        results["skipped"].append(filename)
        continue

    log.info(f"Downloading {filename} -> {target_path}")
    try:
        # dataset_download_file (singular) fetches one named file directly, rather
        # than dataset_download_files' whole-dataset zip -- lets us test a couple
        # of small files before trusting this with the 7.8GB streets.csv.
        api.dataset_download_file(dataset, filename, path=volume_root)

        # Kaggle serves larger files compressed -- the client saves them as
        # <filename>.zip rather than the plain file, even though we asked for
        # filename. Extract and discard the archive so target_path ends up
        # being the plain CSV either way. Smaller files (e.g. node_locations.csv)
        # come back uncompressed already, so this is a no-op for those.
        zip_path = target_path + ".zip"
        if os.path.exists(zip_path):
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(volume_root)
            os.remove(zip_path)
    except Exception as exc:  # noqa: BLE001 - want a clear failure per file, not an aborted loop
        log.error(f"  FAILED: {exc}")
        results["failed"].append(filename)
        continue

    if not os.path.exists(target_path):
        log.error(f"  Download reported success but {target_path} is missing — check the filename matches Kaggle's.")
        results["failed"].append(filename)
        continue

    log.info(f"  done ({os.path.getsize(target_path):,} bytes)")
    results["downloaded"].append(filename)

log.info(f"Summary: {results}")
if results["failed"]:
    raise RuntimeError(f"Failed to download: {results['failed']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanity check
# MAGIC Confirm what actually landed in the volume:

# COMMAND ----------

display(dbutils.fs.ls(volume_root.replace("/Volumes", "dbfs:/Volumes")))
