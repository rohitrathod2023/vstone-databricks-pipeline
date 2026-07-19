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

# Load configuration
STREETS_TABLE = get_source_config("silver_streets", env=env)["target_table"]
ENVIRONMENT_TABLE = get_source_config("silver_environment", env=env)["target_table"]
STREET_ID = 7
OLD_DANGEROUS = 0.3
NEW_DANGEROUS = 0.7

print("=" * 80)
print("SCD2 DEMO CONFIGURATION")
print("=" * 80)
print(f"\nCatalog: {catalog}")
print(f"Environment: {env}\n")
print("Source tables:")
print(f"  Streets: {STREETS_TABLE}")
print(f"  Environment: {ENVIRONMENT_TABLE}\n")
print("Target street:")
print(f"  street_id = {STREET_ID} (PalasietA)")
print(f"  Current dangerous: {OLD_DANGEROUS}")
print(f"  New dangerous: {NEW_DANGEROUS}")
print(f"  Threshold crossed: {OLD_DANGEROUS} < 0.5 -> {NEW_DANGEROUS} > 0.5 (safe to dangerous)")
print("=" * 80 + "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Change street_id=7's dangerous value at the source (idempotent)

# COMMAND ----------

print("=" * 80)
print("STEP 1: SOURCE TABLE UPDATE (IDEMPOTENT)")
print("=" * 80)
print(f"\nTarget table: {STREETS_TABLE}")
print(f"Target street: street_id = {STREET_ID} (PalasietA)")
print(f"Expected change: dangerous {OLD_DANGEROUS} -> {NEW_DANGEROUS}\n")

# Check current state
current_row = spark.sql(f"SELECT * FROM {STREETS_TABLE} WHERE street_id = {STREET_ID}").collect()
if len(current_row) != 1:
    raise RuntimeError(f"STOP: expected exactly 1 row for street_id={STREET_ID}, found {len(current_row)}.")
current_dangerous = current_row[0]["dangerous"]

print("Current state:")
print("-" * 80)
current_df = spark.createDataFrame([current_row[0].asDict()])
display(current_df)

# Apply or confirm update
if current_dangerous == OLD_DANGEROUS:
    print(f"\nCurrent dangerous value: {current_dangerous}")
    print("Status: UPDATE required\n")
    print("Executing UPDATE...")
    spark.sql(f"UPDATE {STREETS_TABLE} SET dangerous = {NEW_DANGEROUS} WHERE street_id = {STREET_ID}")
    print(f"UPDATE completed: dangerous {OLD_DANGEROUS} -> {NEW_DANGEROUS}")
elif current_dangerous == NEW_DANGEROUS:
    print(f"\nCurrent dangerous value: {current_dangerous}")
    print("Status: Already applied in an earlier run -- no change needed")
else:
    raise RuntimeError(
        f"STOP: street_id={STREET_ID}'s dangerous value is {current_dangerous}, "
        f"neither the expected old ({OLD_DANGEROUS}) nor new ({NEW_DANGEROUS}) value."
    )

# Verify final state
after_row = spark.sql(f"SELECT * FROM {STREETS_TABLE} WHERE street_id = {STREET_ID}").collect()[0].asDict()
print("\nFinal state after UPDATE:")
print("-" * 80)
after_df = spark.createDataFrame([after_row])
display(after_df)

if after_row["dangerous"] != NEW_DANGEROUS:
    raise RuntimeError(f"STOP: expected dangerous={NEW_DANGEROUS}, found {after_row['dangerous']}.")

# Verify table row count unchanged
row_count = spark.table(STREETS_TABLE).count()
if row_count != 36:
    raise RuntimeError(f"STOP: expected silver_streets row count to stay 36, found {row_count}.")

print(f"\nValidation: Table row count unchanged at {row_count}")
print("=" * 80 + "\n")

# COMMAND ----------

# DBTITLE 1,Before/After Comparison
# Create comparison summary
print("\n" + "=" * 80)
print("CHANGE SUMMARY: DANGEROUS VALUE")
print("=" * 80)
print()

comparison_data = [
    ("Before", OLD_DANGEROUS, "Safe" if OLD_DANGEROUS < 0.5 else "Dangerous"),
    ("After", NEW_DANGEROUS, "Safe" if NEW_DANGEROUS < 0.5 else "Dangerous"),
    ("Change", NEW_DANGEROUS - OLD_DANGEROUS, "Threshold crossed: safe -> dangerous" if OLD_DANGEROUS < 0.5 <= NEW_DANGEROUS else "")
]

comparison_df = spark.createDataFrame(
    comparison_data,
    ["State", "Dangerous Value", "Classification"]
)

print(f"Street: street_id = {STREET_ID} (PalasietA)")
print(f"Safe/Dangerous threshold: 0.5\n")
display(comparison_df)
print("\n" + "=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Add one companion fact reading dated after the SCD2 change
# MAGIC Needed so the new SCD2 version has any real fact data to resolve
# MAGIC against at all -- see the notebook-level note above. Uses the actual
# MAGIC `stg_dim_street_scd2` `__START_AT` for street_id=7's newest version (if
# MAGIC it already exists from an earlier Gold refresh) to guarantee this
# MAGIC reading's date is genuinely after it; falls back to "now" otherwise.

# COMMAND ----------

print("=" * 80)
print("STEP 2: ADD COMPANION FACT READING")
print("=" * 80)
print(f"\nTarget table: {ENVIRONMENT_TABLE}")
print(f"Purpose: Create fact data dated after the SCD2 change")
print(f"Reason: AUTO CDC FROM SNAPSHOT stamps new versions with wall-clock time")
print("        (~2026-07-19), but all historical facts are 2023-2024\n")

# Determine appropriate reading date
gold_schema = STREETS_TABLE.rsplit(".", 2)[1]
stg_table = f"{catalog}.{gold_schema}.stg_dim_street_scd2"

print(f"Checking SCD2 staging table: {stg_table}")
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
    print(f"Found latest __START_AT: {new_version_start_at}")
    print(f"Setting reading date to: {reading_date} (1 hour after)\n")
else:
    reading_date = datetime.now(timezone.utc) + timedelta(hours=1)
    print("SCD2 staging table not available yet")
    print(f"Setting reading date to: {reading_date} (now + 1 hour)\n")

reading_date_str = reading_date.strftime("%Y-%m-%d %H:%M:%S.%f")

# Check if reading already exists
print("Checking for existing post-SCD2 readings...")
existing_new_reading = spark.sql(
    f"""
    SELECT COUNT(*) AS c FROM {ENVIRONMENT_TABLE}
    WHERE street_id = {STREET_ID} AND date > '{new_version_start_at or reading_date_str}'
    """
).collect()[0]["c"]

if existing_new_reading > 0:
    print(f"Status: Reading already exists (from earlier run)")
    print(f"Found {existing_new_reading} reading(s) after SCD2 change")
    print("No insert needed\n")
else:
    print("Status: No post-SCD2 reading found")
    print("Inserting new reading...\n")
    
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
    
    # Display inserted reading details
    inserted_reading = spark.sql(
        f"""
        SELECT street_id, date, noise, pollution, light, raining, source_file, run_id
        FROM {ENVIRONMENT_TABLE}
        WHERE street_id = {STREET_ID} AND run_id = '{demo_run_id}'
        """
    )
    
    print("Inserted reading details:")
    print("-" * 80)
    display(inserted_reading)
    print(f"\nInsert completed: 1 new reading for street_id={STREET_ID}")

print("\n" + "=" * 80)
print("DEMO SETUP COMPLETE")
print("=" * 80)
print("\nNext steps:")
print("  1. Refresh the Gold pipeline (standard run)")
print("  2. Verify stg_dim_street_scd2 has 37 rows")
print("  3. Verify Dim_Street has street_key 1-37 (contiguous)")
print(f"  4. Verify gold_street_risk_summary shows risk_changed_flag=true")
print(f"     for street_id={STREET_ID} in the month of the new reading")
print("\nRefer to: docs/day7_incremental_load_evidence.md")
print("=" * 80)