# Databricks notebook source
# MAGIC %md
# MAGIC # Silver incremental load simulation — MERGE INTO — Day 7, Piece 1
# MAGIC A real, one-time simulation of a late-arriving correction + a genuinely new
# MAGIC reading landing in `silver_environment`. `Fact_Street_Conditions` (Gold) is
# MAGIC a DLT materialized view -- a recomputed reflection of `silver_environment`,
# MAGIC not an independently writable table -- so a correction has to be applied at
# MAGIC its source here; Gold picks it up automatically on its next refresh.
# MAGIC
# MAGIC This is an imperative, one-off script (not a `@dlt.table`) -- `MERGE INTO`
# MAGIC is a direct write and doesn't belong in DLT's declarative pipeline code.
# MAGIC
# MAGIC Uses SQL `MERGE INTO`, not the `DeltaTable.merge()` Python API -- the
# MAGIC latter failed here with a real, confirmed `[DELTA_MISSING_DELTA_TABLE]`
# MAGIC error (`DeltaTable.forName()` doesn't recognize this DLT-managed table as
# MAGIC a Delta table, even though it's genuinely Delta-backed underneath). Plain
# MAGIC SQL `MERGE INTO` resolves the table through the normal SQL catalog path
# MAGIC instead and works fine, same as the ACID demo's SQL-based UPDATE/DELETE/
# MAGIC MERGE against `silver_locations`, another DLT-managed table.

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

import uuid  # noqa: E402
from datetime import datetime, timezone  # noqa: E402

from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from utils.config_loader import get_source_config  # noqa: E402

TABLE = get_source_config("silver_environment", env=env)["target_table"]
print(f"Demo target: {TABLE}")

# The real, existing row this demo corrects -- confirmed present via a real
# query before writing this notebook (street_id=1, an ordinary reading,
# noise=0.0/pollution=0.0/light=0.5295811217256587/raining=0.1502148020932554).
CORRECTION_STREET_ID = 1
CORRECTION_DATE = "2023-08-15 00:00:03.093"

# A street_id+date combination confirmed NOT to exist yet -- a genuinely new
# reading, one day past the last currently-observed date (2024-03-10) for
# this street, not an arbitrary far-future placeholder.
NEW_STREET_ID = 1
NEW_DATE = "2024-03-11 08:00:00.000"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Before state

# COMMAND ----------

before_count = spark.table(TABLE).count()
print(f"Before row count: {before_count}")

before_row = (
    spark.table(TABLE)
    .filter(f"street_id = {CORRECTION_STREET_ID} AND date = '{CORRECTION_DATE}'")
    .collect()
)
if len(before_row) != 1:
    raise RuntimeError(
        f"STOP: expected exactly 1 existing row for street_id={CORRECTION_STREET_ID}, "
        f"date={CORRECTION_DATE}, found {len(before_row)}. Not proceeding against an unexpected state."
    )
before_row = before_row[0].asDict()
print(f"Row to be corrected (before): {before_row}")

new_combo_count = (
    spark.table(TABLE)
    .filter(f"street_id = {NEW_STREET_ID} AND date = '{NEW_DATE}'")
    .count()
)
if new_combo_count != 0:
    raise RuntimeError(
        f"STOP: expected street_id={NEW_STREET_ID}, date={NEW_DATE} to not exist yet, "
        f"found {new_combo_count} row(s). Pick a different simulated 'new reading' key."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Build the simulated new-data drop
# MAGIC One row corrects the existing reading above with obviously different
# MAGIC values; one row is a genuinely new reading. Real audit columns, clearly
# MAGIC identifiable as this demo's own drop (not pretending to be a real
# MAGIC ingestion event) via `source_file`/`run_id`.

# COMMAND ----------

demo_run_id = str(uuid.uuid4())
demo_load_dt = datetime.now(timezone.utc)

new_data_schema = StructType(
    [
        StructField("street_id", IntegerType()),
        StructField("date", TimestampType()),
        StructField("noise", DoubleType()),
        StructField("pollution", DoubleType()),
        StructField("light", DoubleType()),
        StructField("raining", DoubleType()),
        StructField("load_dt", TimestampType()),
        StructField("source_format", StringType()),
        StructField("source_file", StringType()),
        StructField("run_id", StringType()),
    ]
)

new_data_rows = [
    # Correction: same street_id+date as the real row above, obviously
    # different measurement values (was noise=0.0/pollution=0.0/
    # light=0.5295811217256587/raining=0.1502148020932554).
    (
        CORRECTION_STREET_ID,
        datetime.fromisoformat(CORRECTION_DATE),
        15.5,
        8.2,
        45.0,
        2.3,
        demo_load_dt,
        "csv",
        "streets_correction_demo.csv",
        demo_run_id,
    ),
    # New reading: a street_id+date combination that doesn't exist yet.
    (
        NEW_STREET_ID,
        datetime.fromisoformat(NEW_DATE),
        12.0,
        6.5,
        40.0,
        0.5,
        demo_load_dt,
        "csv",
        "streets_correction_demo.csv",
        demo_run_id,
    ),
]

new_data_df = spark.createDataFrame(new_data_rows, new_data_schema)
display(new_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — MERGE INTO
# MAGIC Matches on `street_id` + `date` -- the same natural key
# MAGIC `silver_environment`'s own `dropDuplicates` already dedups on.
# MAGIC
# MAGIC Uses SQL `MERGE INTO`, not the `DeltaTable.merge()` Python API --
# MAGIC `DeltaTable.forName()` failed here with a real, confirmed
# MAGIC `[DELTA_MISSING_DELTA_TABLE]` error against this DLT-managed table, even
# MAGIC though it's genuinely Delta-backed underneath. Plain SQL `MERGE INTO`
# MAGIC resolves the table through the normal SQL catalog path instead and works
# MAGIC fine (same as the ACID demo's SQL-based UPDATE/DELETE/MERGE against
# MAGIC `silver_locations`, another DLT-managed table).

# COMMAND ----------

new_data_df.createOrReplaceTempView("_merge_demo_source")

spark.sql(
    f"""
    MERGE INTO {TABLE} AS target
    USING _merge_demo_source AS source
    ON target.street_id = source.street_id AND target.date = source.date
    WHEN MATCHED THEN UPDATE SET
        target.noise = source.noise,
        target.pollution = source.pollution,
        target.light = source.light,
        target.raining = source.raining,
        target.load_dt = source.load_dt,
        target.source_format = source.source_format,
        target.source_file = source.source_file,
        target.run_id = source.run_id
    WHEN NOT MATCHED THEN INSERT *
    """
)
print("MERGE INTO applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — After state

# COMMAND ----------

after_count = spark.table(TABLE).count()
print(f"After row count: {after_count} (expected {before_count} + 1 = {before_count + 1})")

after_row = (
    spark.table(TABLE)
    .filter(f"street_id = {CORRECTION_STREET_ID} AND date = '{CORRECTION_DATE}'")
    .collect()
)
print(f"Corrected row count for this key (expect exactly 1, no duplicate): {len(after_row)}")
if after_row:
    after_row_dict = after_row[0].asDict()
    print(f"Row after correction: {after_row_dict}")

new_row = (
    spark.table(TABLE)
    .filter(f"street_id = {NEW_STREET_ID} AND date = '{NEW_DATE}'")
    .collect()
)
print(f"New row count for this key (expect exactly 1): {len(new_row)}")
if new_row:
    print(f"New row: {new_row[0].asDict()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Assertions

# COMMAND ----------

row_count_ok = after_count == before_count + 1
no_duplicate_ok = len(after_row) == 1
values_corrected_ok = (
    no_duplicate_ok
    and after_row_dict["noise"] == 15.5
    and after_row_dict["pollution"] == 8.2
    and after_row_dict["light"] == 45.0
    and after_row_dict["raining"] == 2.3
)
new_row_ok = len(new_row) == 1

print(f"{'PASS' if row_count_ok else 'FAIL'} -- row count is exactly before+1")
print(f"{'PASS' if no_duplicate_ok else 'FAIL'} -- corrected key has no duplicate")
print(f"{'PASS' if values_corrected_ok else 'FAIL'} -- corrected row shows the new values, not the old ones")
print(f"{'PASS' if new_row_ok else 'FAIL'} -- new reading exists exactly once")

if not (row_count_ok and no_duplicate_ok and values_corrected_ok and new_row_ok):
    raise RuntimeError(
        "STOP: one or more assertions failed after the MERGE. Do not treat this demo as complete -- "
        "investigate silver_environment's real state before proceeding."
    )

print("\nMERGE INTO simulation complete: 1 row corrected in place, 1 new row inserted, no duplicates.")
