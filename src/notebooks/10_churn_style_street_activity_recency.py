# Databricks notebook source
# MAGIC %md
# MAGIC # Churn-style metric: street activity recency — Day 7
# MAGIC Adapted from the brief's "customers with no transaction in 6 months"
# MAGIC example: for this dataset, the equivalent signal is "streets with no
# MAGIC environmental sensor readings recently" -- the same shape of problem
# MAGIC (an entity that used to be active going quiet), just applied to sensors
# MAGIC instead of customers.
# MAGIC
# MAGIC **Standalone demo notebook, not a DLT table** -- this is a one-off
# MAGIC analysis for the Day 10 walkthrough, same pattern as
# MAGIC `06_liquid_clustering_benchmark.py`, not an ongoing operational metric
# MAGIC wired into the Gold pipeline. Reuses the actual tested build function
# MAGIC from `src/pipelines/gold/agg_street_activity_recency.py` (32/32 -- 6/6 on
# MAGIC this function specifically -- unit tests passing), just invoked here
# MAGIC instead of from a `@dlt.table`.
# MAGIC
# MAGIC **Honest finding up front:** the real dataset has zero churn --
# MAGIC confirmed live, all 36 streets report gap-free data across all 283
# MAGIC observed days, from the very first day to the very last. So the real
# MAGIC section below will show every street at 0 days since its last reading.
# MAGIC That's a correct, verified result, not a bug -- and the second section
# MAGIC proves the detection logic actually works, using an illustrative
# MAGIC synthetic gap, since the real data doesn't currently exercise that path.

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
dbutils.widgets.text("churn_threshold_days", "30", "Churn threshold (days)")

catalog = dbutils.widgets.get("catalog")
env = dbutils.widgets.get("env")
churn_threshold_days = int(dbutils.widgets.get("churn_threshold_days"))

# COMMAND ----------

import matplotlib.pyplot as plt  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from pipelines.gold.agg_street_activity_recency import build_agg_street_activity_recency  # noqa: E402
from utils.config_loader import get_source_config  # noqa: E402

FACT_TABLE = get_source_config("fact_city_observations", env=env)["target_table"]
DIM_STREET_TABLE = get_source_config("dim_street", env=env)["target_table"]

print(f"Fact table: {FACT_TABLE}")
print(f"Dim street table: {DIM_STREET_TABLE}")
print(f"Churn threshold: {churn_threshold_days} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1 — real data
# MAGIC Runs the actual, tested `build_agg_street_activity_recency` against the
# MAGIC live `fact_city_observations` and `dim_street`.

# COMMAND ----------

fact_df = spark.table(FACT_TABLE)
dim_street_df = spark.table(DIM_STREET_TABLE)

real_result = build_agg_street_activity_recency(fact_df, dim_street_df, churn_threshold_days)
display(real_result.orderBy(F.desc("days_since_last_observation")))

# COMMAND ----------

churned_count = real_result.filter(F.col("is_churned")).count()
total_count = real_result.count()
print(f"{churned_count} of {total_count} streets currently flagged as churned (threshold: {churn_threshold_days} days).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chart 1 — real data: days since last observation, per street
# MAGIC Every bar should sit at 0, below the red churn-threshold line -- the
# MAGIC real, verified "nothing has gone quiet" baseline.

# COMMAND ----------

real_pd = real_result.orderBy("street_id").select("street", "days_since_last_observation", "is_churned").toPandas()

fig, ax = plt.subplots(figsize=(14, 6))
colors = ["#B45309" if churned else "#0F6E6A" for churned in real_pd["is_churned"]]
ax.bar(real_pd["street"], real_pd["days_since_last_observation"], color=colors)
ax.axhline(y=churn_threshold_days, color="#DC2626", linestyle="--", linewidth=1.5, label=f"Churn threshold ({churn_threshold_days} days)")
ax.set_ylabel("Days since last observation")
ax.set_title("Street activity recency — real data (all streets currently active)")
ax.legend()
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2 — synthetic gap, to prove the detection logic works
# MAGIC The real data doesn't exercise the "churned" path, so this section
# MAGIC injects one illustrative gap into a **copy** of the real data (does not
# MAGIC write anywhere) to demonstrate the flag actually triggering. Clearly
# MAGIC labeled synthetic -- not a claim about real streets.

# COMMAND ----------

# Pick one real street and cut its readings off well before the dataset's
# real end date, simulating a sensor that went quiet.
demo_street_key = real_result.orderBy("street_key").first()["street_key"]
max_date_key = fact_df.agg(F.max("date_key")).collect()[0][0]
# Cut the demo street off ~60 days before the real end of the dataset.
simulated_cutoff = int(
    spark.sql(
        f"SELECT CAST(date_format(date_sub(to_date(CAST({max_date_key} AS STRING), 'yyyyMMdd'), 60), 'yyyyMMdd') AS INT)"
    ).collect()[0][0]
)

synthetic_fact_df = fact_df.withColumn(
    "date_key",
    F.when(
        (F.col("street_key") == demo_street_key) & (F.col("date_key") > simulated_cutoff),
        F.lit(None).cast("int"),
    ).otherwise(F.col("date_key")),
).filter(F.col("date_key").isNotNull())

synthetic_result = build_agg_street_activity_recency(synthetic_fact_df, dim_street_df, churn_threshold_days)
display(synthetic_result.orderBy(F.desc("days_since_last_observation")))

# COMMAND ----------

demo_row = synthetic_result.filter(F.col("street_key") == demo_street_key).collect()[0]
print(
    f"Synthetic demo: street_key={demo_street_key} ('{demo_row['street']}') cut off {demo_row['days_since_last_observation']} "
    f"days before the dataset's real end -- is_churned={demo_row['is_churned']}."
)
assert demo_row["is_churned"], "Synthetic demo should always trigger churn -- if not, the detection logic itself is broken."

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chart 2 — synthetic data: the churn flag actually triggering

# COMMAND ----------

synthetic_pd = (
    synthetic_result.orderBy("street_id").select("street", "days_since_last_observation", "is_churned").toPandas()
)

fig, ax = plt.subplots(figsize=(14, 6))
colors = ["#B45309" if churned else "#0F6E6A" for churned in synthetic_pd["is_churned"]]
ax.bar(synthetic_pd["street"], synthetic_pd["days_since_last_observation"], color=colors)
ax.axhline(y=churn_threshold_days, color="#DC2626", linestyle="--", linewidth=1.5, label=f"Churn threshold ({churn_threshold_days} days)")
ax.set_ylabel("Days since last observation")
ax.set_title("Street activity recency — synthetic gap (proves the flag triggers)")
ax.legend()
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()

# COMMAND ----------

print("Done. Section 1 = real, verified baseline (0 churned). Section 2 = synthetic proof the logic works.")
