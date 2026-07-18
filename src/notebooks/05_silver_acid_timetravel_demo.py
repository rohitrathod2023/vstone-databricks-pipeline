# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Delta Lake ACID + time travel demo — Day 4-5 catch-up
# MAGIC A previously-missed Day 4-5 checklist item: real evidence of Delta Lake's
# MAGIC ACID transactions and time travel, not a description of what Delta *could*
# MAGIC do. Runs against `silver_locations` (13 rows -- small enough to fully
# MAGIC eyeball every row before and after), a standalone, low-risk demo that never
# MAGIC touches Bronze, Gold, or the Silver pipeline's actual build logic.
# MAGIC
# MAGIC Run each cell top to bottom. Every mutation (UPDATE/DELETE/MERGE) is
# MAGIC followed immediately by its own `DESCRIBE HISTORY` entry as evidence, then
# MAGIC the table is rolled back to the exact pre-demo version at the end (via a
# MAGIC time-travel read + `MERGE`, not `RESTORE TABLE` -- not supported on a DLT
# MAGIC streaming table like this one), re-verified row-by-row. Gold's
# MAGIC `Dim_Location` (built from this table) never sees the intermediate
# MAGIC mutated state, since no Gold pipeline refresh runs during this notebook.

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

from pyspark.sql import functions as F  # noqa: E402

from utils.config_loader import get_source_config  # noqa: E402

TABLE = get_source_config("silver_locations", env=env)["target_table"]
print(f"Demo target: {TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Baseline
# MAGIC Confirm the row count matches Silver's already-verified number (13), and
# MAGIC record the version this baseline corresponds to -- needed later for the
# MAGIC time-travel query and the rollback.

# COMMAND ----------

baseline_count = spark.sql(f"SELECT COUNT(*) AS row_count FROM {TABLE}").collect()[0]["row_count"]
print(f"Baseline row count: {baseline_count}")

if baseline_count != 13:
    raise RuntimeError(
        f"STOP: expected 13 baseline rows (the already-verified Silver number), got {baseline_count}. "
        "Not proceeding against an unexpected table state."
    )

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {TABLE}").orderBy(F.desc("version")))

# COMMAND ----------

baseline_version = spark.sql(f"DESCRIBE HISTORY {TABLE}").agg(F.max("version")).collect()[0][0]
print(f"Baseline version (for the time-travel query and rollback below): {baseline_version}")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {TABLE} ORDER BY location"))

# COMMAND ----------

# Captured now (before any mutation) so Step 7's spot-check compares against
# this run's real baseline value, not a hardcoded number.
original_loc1_latitude = spark.sql(f"SELECT latitude FROM {TABLE} WHERE location = 1").collect()[0]["latitude"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — UPDATE (atomicity/consistency)
# MAGIC Update `location=1`'s coordinates to an obviously different, clearly
# MAGIC marked test value (+1.0 to both).

# COMMAND ----------

spark.sql(f"UPDATE {TABLE} SET latitude = latitude + 1.0, longitude = longitude + 1.0 WHERE location = 1")
print("UPDATE applied to location=1.")

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {TABLE}").orderBy(F.desc("version")).limit(1))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {TABLE} WHERE location = 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — DELETE (atomicity/consistency)
# MAGIC Delete `location=2` (a different row from the one just updated).

# COMMAND ----------

spark.sql(f"DELETE FROM {TABLE} WHERE location = 2")
print("DELETE applied to location=2.")

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {TABLE}").orderBy(F.desc("version")).limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — MERGE (atomicity/consistency)
# MAGIC One inline source row matches an existing `location` (3) -> updates just
# MAGIC its latitude. One source row's `location` (99) doesn't exist -> inserted
# MAGIC as a new row.

# COMMAND ----------

spark.sql(
    f"""
    MERGE INTO {TABLE} AS target
    USING (
        SELECT * FROM VALUES
            (3, CAST(99.999 AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS TIMESTAMP),
                CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING)),
            (99, CAST(40.12345 AS DOUBLE), CAST(-3.12345 AS DOUBLE), current_timestamp(),
                'demo', 'acid_timetravel_demo', 'demo-merge-run')
        AS source(location, latitude, longitude, load_dt, source_format, source_file, run_id)
    ) AS source
    ON target.location = source.location
    WHEN MATCHED THEN UPDATE SET target.latitude = source.latitude
    WHEN NOT MATCHED THEN INSERT (location, latitude, longitude, load_dt, source_format, source_file, run_id)
        VALUES (source.location, source.latitude, source.longitude, source.load_dt,
                 source.source_format, source.source_file, source.run_id)
    """
)
print("MERGE applied: location=3 updated, location=99 inserted.")

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {TABLE}").orderBy(F.desc("version")).limit(1))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {TABLE} WHERE location IN (3, 99) ORDER BY location"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Time travel: query the pre-demo version
# MAGIC Even though the *current* table has location=2 deleted and locations 1/3
# MAGIC changed, querying `VERSION AS OF` the baseline version shows the original
# MAGIC data exactly as it was before any of this notebook ran.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {baseline_version} ORDER BY location"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Durability/rollback (via time travel)
# MAGIC A `MERGE` using the baseline version (read via `VERSION AS OF` -- time
# MAGIC travel) as its source, reconciling the current table back to that exact
# MAGIC point in time in one atomic statement. `WHEN MATCHED` reverts changed
# MAGIC rows (location=1, 3), `WHEN NOT MATCHED` re-inserts anything deleted
# MAGIC (location=2), `WHEN NOT MATCHED BY SOURCE` removes anything inserted
# MAGIC since the baseline (location=99). (Note: `RESTORE TABLE` is the more
# MAGIC direct way to do this on a plain Delta table, but isn't used here since
# MAGIC it isn't supported on a DLT-managed streaming table like this one.)

# COMMAND ----------

spark.sql(
    f"""
    MERGE INTO {TABLE} AS target
    USING (SELECT * FROM {TABLE} VERSION AS OF {baseline_version}) AS source
    ON target.location = source.location
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE
    """
)
print("Rollback MERGE applied.")

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {TABLE}").orderBy(F.desc("version")).limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Final verification
# MAGIC Row count must be exactly 13 again, and every row must match the original
# MAGIC baseline exactly (location=2 back, location=1/3 back to their original
# MAGIC coordinates, location=99 gone).

# COMMAND ----------

final_count = spark.sql(f"SELECT COUNT(*) AS row_count FROM {TABLE}").collect()[0]["row_count"]
print(f"Final row count: {final_count} (expected 13)")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {TABLE} ORDER BY location"))

# COMMAND ----------

if final_count != 13:
    raise RuntimeError(
        f"STOP: final row count is {final_count}, not 13 -- {TABLE} may be left in a mutated state. "
        "Do not treat this demo as complete."
    )

# COMMAND ----------

# Spot-check the 3 rows this notebook touched -- not just the total count.
final_rows = {r["location"]: r for r in spark.sql(f"SELECT * FROM {TABLE}").collect()}

checks = {
    "location=2 (deleted) is back": 2 in final_rows,
    "location=1's coordinates match the original (UPDATE undone)": (
        1 in final_rows and abs(final_rows[1]["latitude"] - original_loc1_latitude) < 0.0001
    ),
    "location=99 (merge-inserted) is gone": 99 not in final_rows,
}

for description, passed in checks.items():
    print(f"{'PASS' if passed else 'FAIL'} -- {description}")

if not all(checks.values()):
    raise RuntimeError(
        f"STOP: one or more spot-checks failed after the rollback -- {TABLE} may be left in a mutated state. "
        "Do not treat this demo as complete."
    )

print("\nDemo complete: silver_locations restored to its exact pre-demo state (13 rows).")
