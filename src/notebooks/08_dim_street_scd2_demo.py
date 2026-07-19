# Databricks notebook source
# MAGIC %md
# MAGIC # Dim_Street SCD2 mechanism exercise — Day 7, Piece 2
# MAGIC Changes one street's `dangerous` value at its real source (`silver_streets`,
# MAGIC the table `_dim_street_snapshot` reads from) to exercise the
# MAGIC `AUTO CDC FROM SNAPSHOT` mechanism already built for `stg_dim_street_scd2`
# MAGIC -- this does NOT use `MERGE INTO`; the SCD2 versioning is entirely
# MAGIC automatic once the snapshot source changes and the Gold pipeline refreshes.
# MAGIC
# MAGIC `street_id=7` ("PalasietA") was picked after checking `Dim_Street`'s real
# MAGIC current data -- an ordinary, unremarkable `dangerous=0.3` (not an edge
# MAGIC case, not already near the 0.5 threshold), bumped to `0.7`, crossing the
# MAGIC safe/dangerous threshold in a clear direction.
# MAGIC
# MAGIC **Every step below checks current real state before acting, so this
# MAGIC notebook is safe to re-run at any point** -- e.g. after the Gold pipeline
# MAGIC has already picked up the source change from an earlier run.
# MAGIC
# MAGIC A second real finding drove this notebook's second half: `AUTO CDC FROM
# MAGIC SNAPSHOT` always stamps a new version's `__START_AT` with wall-clock
# MAGIC processing time (~2026-07-19), years after every real
# MAGIC `silver_environment` fact date (2023-2024). The existing backfill-clamp
# MAGIC logic in `fact_street_conditions.py` (correctly) routes all of that
# MAGIC historical data to the *earliest* known version -- so with no fact data
# MAGIC dated after the change, `gold_street_risk_summary` can never show a real
# MAGIC month-over-month change no matter how many streets get a new SCD2
# MAGIC version. The join logic doesn't need to change -- it already resolves a
# MAGIC fact dated after `__START_AT` to the new version via the normal range
# MAGIC condition. What's missing is simply a fact dated that late. This
# MAGIC notebook adds exactly one (for `street_id=7`, dated after the SCD2
# MAGIC change), which is enough to make the change observable end to end.

# COMMAND ----------

import os
import sys
from datetime import datetime, timedelta, timezone

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

from utils.config_loader import get_source_config  # noqa: E402

STREETS_TABLE = get_source_config("silver_streets", env=env)["target_table"]
ENVIRONMENT_TABLE = get_source_config("silver_environment", env=env)["target_table"]
STREET_ID = 7
OLD_DANGEROUS = 0.3
NEW_DANGEROUS = 0.7

print(f"Streets source: {STREETS_TABLE}")
print(f"Environment source: {ENVIRONMENT_TABLE}")
print(f"street_id={STREET_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Change street_id=7's dangerous value at the source (idempotent)

# COMMAND ----------

current_row = spark.sql(f"SELECT * FROM {STREETS_TABLE} WHERE street_id = {STREET_ID}").collect()
if len(current_row) != 1:
    raise RuntimeError(f"STOP: expected exactly 1 row for street_id={STREET_ID}, found {len(current_row)}.")
current_dangerous = current_row[0]["dangerous"]
print(f"Current dangerous value: {current_dangerous}")

if current_dangerous == OLD_DANGEROUS:
    spark.sql(f"UPDATE {STREETS_TABLE} SET dangerous = {NEW_DANGEROUS} WHERE street_id = {STREET_ID}")
    print(f"UPDATE applied: street_id={STREET_ID} dangerous {OLD_DANGEROUS} -> {NEW_DANGEROUS}")
elif current_dangerous == NEW_DANGEROUS:
    print("Already applied in an earlier run -- no change needed.")
else:
    raise RuntimeError(
        f"STOP: street_id={STREET_ID}'s dangerous value is {current_dangerous}, "
        f"neither the expected old ({OLD_DANGEROUS}) nor new ({NEW_DANGEROUS}) value."
    )

after_row = spark.sql(f"SELECT * FROM {STREETS_TABLE} WHERE street_id = {STREET_ID}").collect()[0].asDict()
print(f"Confirmed current state: {after_row}")
if after_row["dangerous"] != NEW_DANGEROUS:
    raise RuntimeError(f"STOP: expected dangerous={NEW_DANGEROUS}, found {after_row['dangerous']}.")

row_count = spark.table(STREETS_TABLE).count()
if row_count != 36:
    raise RuntimeError(f"STOP: expected silver_streets row count to stay 36, found {row_count}.")
print(f"{STREETS_TABLE} row count (unchanged): {row_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Add one companion fact reading dated after the SCD2 change
# MAGIC Needed so the new SCD2 version has any real fact data to resolve
# MAGIC against at all -- see the notebook-level note above. Uses the actual
# MAGIC `stg_dim_street_scd2` `__START_AT` for street_id=7's newest version (if
# MAGIC it already exists from an earlier Gold refresh) to guarantee this
# MAGIC reading's date is genuinely after it; falls back to "now" otherwise.

# COMMAND ----------

gold_schema = STREETS_TABLE.rsplit(".", 2)[1]
stg_table = f"{catalog}.{gold_schema}.stg_dim_street_scd2"

new_version_start_at = None
try:
    rows = spark.sql(
        f"""
        SELECT MAX(__START_AT) AS latest_start_at FROM {stg_table} WHERE street_id = {STREET_ID}
        """
    ).collect()
    if rows and rows[0]["latest_start_at"] is not None:
        new_version_start_at = rows[0]["latest_start_at"]
except Exception:  # noqa: BLE001 -- stg table may not exist yet if Gold hasn't been refreshed at all
    pass

if new_version_start_at is not None:
    reading_date = new_version_start_at + timedelta(hours=1)
    print(f"Using stg_dim_street_scd2's latest __START_AT + 1h: {reading_date}")
else:
    reading_date = datetime.now(timezone.utc) + timedelta(hours=1)
    print(f"stg_dim_street_scd2 not available yet -- using now + 1h instead: {reading_date}")

reading_date_str = reading_date.strftime("%Y-%m-%d %H:%M:%S.%f")

existing_new_reading = spark.sql(
    f"""
    SELECT COUNT(*) AS c FROM {ENVIRONMENT_TABLE}
    WHERE street_id = {STREET_ID} AND date > '{new_version_start_at or reading_date_str}'
    """
).collect()[0]["c"]

if existing_new_reading > 0:
    print(f"A reading after the SCD2 change already exists for street_id={STREET_ID} -- no insert needed.")
else:
    demo_run_id = str(uuid.uuid4())
    spark.sql(
        f"""
        INSERT INTO {ENVIRONMENT_TABLE}
        (street_id, date, noise, pollution, light, raining, load_dt, source_format, source_file, run_id)
        VALUES (
            {STREET_ID}, TIMESTAMP '{reading_date_str}', 5.0, 3.0, 20.0, 0.2,
            current_timestamp(), 'csv', 'street_scd2_demo_reading.csv', '{demo_run_id}'
        )
        """
    )
    print(f"Inserted 1 new silver_environment reading: street_id={STREET_ID}, date={reading_date_str}")

print(
    "\nStep 1+2 complete. Next: (re-)refresh the Gold pipeline (a normal run is sufficient -- see "
    "docs/day7_incremental_load_evidence.md), then verify stg_dim_street_scd2 (37 rows), Dim_Street "
    "(street_key 1-37 contiguous), and gold_street_risk_summary (risk_changed_flag=true for street_id=7 "
    "in the month this new reading falls in)."
)
