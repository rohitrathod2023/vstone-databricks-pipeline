# Databricks notebook source
# MAGIC %md
# MAGIC # Liquid Clustering vs. partitioning + Z-ordering benchmark — Day 7
# MAGIC Real, measured comparison on `fact_city_observations` (112.6M rows) --
# MAGIC the one unified fact table in Gold (this project's fact tables were later
# MAGIC merged from separate `fact_street_conditions`/`fact_traffic_counts` tables
# MAGIC into one `fact_city_observations` with an `observation_type` discriminator
# MAGIC -- see `docs/gold_data_model.md`). Superseded an earlier version of this
# MAGIC notebook that targeted the old, now-retired `fact_street_conditions`.
# MAGIC
# MAGIC **Read `docs/liquid_clustering_benchmark.md`'s "Important caveat" section
# MAGIC before trusting these numbers at face value** -- the deployed
# MAGIC `fact_city_observations` currently clusters on `date_key` alone (not
# MAGIC `street_key`), so this run does not test Liquid Clustering and Z-ordering
# MAGIC on equal footing for the `street_key`-filtered queries below. It's a real,
# MAGIC useful result, just not the one the naive reading suggests.
# MAGIC
# MAGIC Liquid Clustering itself is applied directly to `fact_city_observations`
# MAGIC via `cluster_by=[...]` on its `@dlt.table` decorator (see
# MAGIC `dlt_gold_tables.py`) -- `ALTER TABLE ... CLUSTER BY` cannot be applied
# MAGIC post-hoc to this table (confirmed live:
# MAGIC `EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE`, the same limitation already hit
# MAGIC with PK/FK constraints), so `cluster_by` is declared at table-creation
# MAGIC time instead, same pattern as `schema=`.
# MAGIC
# MAGIC This notebook builds the comparison side: a plain (non-DLT) Delta table,
# MAGIC `fact_city_observations_zorder_benchmark`, partitioned by year/month and
# MAGIC `OPTIMIZE ... ZORDER BY (street_key)`'d -- a benchmark artifact only, not
# MAGIC part of the documented Gold data model / ER diagram / PK-FK set. Then it
# MAGIC runs the same representative queries against both tables and records
# MAGIC real wall-clock timings.

# COMMAND ----------

import json
import os
import sys
import time

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

LIQUID_TABLE = get_source_config("fact_city_observations", env=env)["target_table"]
GOLD_SCHEMA = LIQUID_TABLE.rsplit(".", 2)[1]
BENCHMARK_TABLE = f"{catalog}.{GOLD_SCHEMA}.fact_city_observations_zorder_benchmark"

print(f"Liquid Clustering table: {LIQUID_TABLE}")
print(f"Partition+Z-order benchmark table: {BENCHMARK_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the comparison table
# MAGIC Same source data as `fact_city_observations`, plus derived `year`/`month`
# MAGIC columns (not present on the real table) so the table can be partitioned
# MAGIC by them -- a plain `CREATE OR REPLACE TABLE`, not a DLT table, so it's
# MAGIC unambiguously a one-off benchmark artifact.

# COMMAND ----------

liquid_df = spark.table(LIQUID_TABLE)

with_partition_cols = liquid_df.withColumn(
    "year", (F.col("date_key") / 10000).cast("int")
).withColumn(
    "month", ((F.col("date_key") / 100) % 100).cast("int")
)

(
    with_partition_cols.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("year", "month")
    .saveAsTable(BENCHMARK_TABLE)
)
print(f"Wrote {BENCHMARK_TABLE}, partitioned by year/month.")

# COMMAND ----------

spark.sql(f"OPTIMIZE {BENCHMARK_TABLE} ZORDER BY (street_key)")
print(f"OPTIMIZE ... ZORDER BY (street_key) complete on {BENCHMARK_TABLE}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row-count parity check
# MAGIC The benchmark table must be a faithful copy -- same row count as
# MAGIC `fact_city_observations`'s already-verified row count (112,586,955).

# COMMAND ----------

liquid_count = spark.table(LIQUID_TABLE).count()
benchmark_count = spark.table(BENCHMARK_TABLE).count()
print(f"fact_city_observations row count: {liquid_count}")
print(f"fact_city_observations_zorder_benchmark row count: {benchmark_count}")

if liquid_count != 112_586_955 or benchmark_count != 112_586_955:
    raise RuntimeError(
        f"STOP: expected both tables at 112,586,955 rows (the already-verified fact_city_observations "
        f"row count), got liquid={liquid_count}, benchmark={benchmark_count}."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pick a real, non-edge-case street_key + month
# MAGIC August 2023 is fully inside the observed date range (2023-06-02 to
# MAGIC 2024-03-10) for every street, so no street is missing data for it.

# COMMAND ----------

sample_month_count = (
    spark.table(LIQUID_TABLE)
    .filter("street_key = 1 AND date_key BETWEEN 20230801 AND 20230831")
    .count()
)
print(f"street_key=1, August 2023 row count: {sample_month_count} (confirms a real, non-trivial slice)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benchmark queries
# MAGIC Each table is queried using its own natural column set for the access
# MAGIC pattern -- `fact_city_observations` filters on `street_key`/`date_key`
# MAGIC directly, the benchmark table filters on the partition columns
# MAGIC `year`/`month` plus `street_key`. Filtering the partitioned table on
# MAGIC `date_key` alone -- ignoring the partition columns it was actually
# MAGIC organized by -- would be an unfair, unrealistic test (no partition
# MAGIC pruning could occur at all), not a like-for-like comparison of each
# MAGIC strategy queried the way it's meant to be queried.
# MAGIC
# MAGIC **Caveat (see docs/liquid_clustering_benchmark.md for the full writeup):**
# MAGIC the deployed `fact_city_observations` currently clusters on `date_key`
# MAGIC alone, not `(street_key, date_key)` -- so Liquid Clustering gets no
# MAGIC data-skipping benefit at all from the `street_key` predicate these
# MAGIC queries use, while the benchmark table is deliberately `ZORDER BY
# MAGIC (street_key)`. This is not an equal-footing test of the two
# MAGIC technologies; it mostly demonstrates that a clustering key has to
# MAGIC actually match the query's filter columns to help.
# MAGIC
# MAGIC 3 runs each, first run discarded as cold cache, remaining runs averaged.

# COMMAND ----------


def timed_runs(label, query_fn, runs=3):
    times = []
    for i in range(runs):
        start = time.perf_counter()
        result = query_fn()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        tag = "cold (discarded)" if i == 0 else "warm"
        print(f"{label} — run {i + 1} ({tag}): {elapsed:.3f}s — result={result}")
    warm_times = times[1:]
    avg_warm = sum(warm_times) / len(warm_times)
    print(f"{label} — avg of warm runs: {avg_warm:.3f}s\n")
    return {"all_times": times, "warm_times": warm_times, "avg_warm": avg_warm}


# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1 — point lookup: avg pollution, one street, one month

# COMMAND ----------

def q1_liquid():
    row = (
        spark.sql(
            f"""
            SELECT AVG(pollution) AS avg_pollution, COUNT(*) AS n
            FROM {LIQUID_TABLE}
            WHERE street_key = 1 AND date_key BETWEEN 20230801 AND 20230831
            """
        )
        .collect()[0]
    )
    return {"avg_pollution": row["avg_pollution"], "n": row["n"]}


def q1_benchmark():
    row = (
        spark.sql(
            f"""
            SELECT AVG(pollution) AS avg_pollution, COUNT(*) AS n
            FROM {BENCHMARK_TABLE}
            WHERE street_key = 1 AND year = 2023 AND month = 8
            """
        )
        .collect()[0]
    )
    return {"avg_pollution": row["avg_pollution"], "n": row["n"]}


q1_liquid_results = timed_runs("Q1 Liquid Clustering", q1_liquid)
q1_benchmark_results = timed_runs("Q1 Partition+Z-order", q1_benchmark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2 — range scan: all readings for one street, full date range

# COMMAND ----------

def q2_liquid():
    row = (
        spark.sql(
            f"""
            SELECT COUNT(*) AS n, AVG(noise) AS avg_noise, AVG(pollution) AS avg_pollution
            FROM {LIQUID_TABLE}
            WHERE street_key = 1
            """
        )
        .collect()[0]
    )
    return {"n": row["n"], "avg_noise": row["avg_noise"], "avg_pollution": row["avg_pollution"]}


def q2_benchmark():
    row = (
        spark.sql(
            f"""
            SELECT COUNT(*) AS n, AVG(noise) AS avg_noise, AVG(pollution) AS avg_pollution
            FROM {BENCHMARK_TABLE}
            WHERE street_key = 1
            """
        )
        .collect()[0]
    )
    return {"n": row["n"], "avg_noise": row["avg_noise"], "avg_pollution": row["avg_pollution"]}


q2_liquid_results = timed_runs("Q2 Liquid Clustering", q2_liquid)
q2_benchmark_results = timed_runs("Q2 Partition+Z-order", q2_benchmark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------


def pct_diff(a, b):
    """% difference of b relative to a, signed -- positive means b is slower."""
    return (b - a) / a * 100


summary_rows = [
    ("Q1: avg pollution, street=1, Aug 2023", q1_liquid_results["avg_warm"], q1_benchmark_results["avg_warm"]),
    ("Q2: all readings, street=1, full range", q2_liquid_results["avg_warm"], q2_benchmark_results["avg_warm"]),
]

print(f"{'Query':45} {'Liquid (s)':>12} {'Partition+Z (s)':>16} {'Delta (s)':>10} {'% diff':>8}")
for label, liquid_avg, benchmark_avg in summary_rows:
    delta = benchmark_avg - liquid_avg
    diff = pct_diff(liquid_avg, benchmark_avg)
    print(f"{label:45} {liquid_avg:>12.3f} {benchmark_avg:>16.3f} {delta:>10.3f} {diff:>7.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chart — grouped bar comparison, Liquid Clustering vs. Partition+Z-order
# MAGIC **Read this alongside the caveat above**, not in isolation: the deployed
# MAGIC `fact_city_observations` clusters on `date_key` only, so Liquid
# MAGIC Clustering gets no data-skipping benefit at all from these queries'
# MAGIC `street_key` predicate -- this chart shows a real measured result, not
# MAGIC an apples-to-apples verdict on the two technologies.

# COMMAND ----------

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

labels = [label for label, _, _ in summary_rows]
liquid_times = [liquid_avg for _, liquid_avg, _ in summary_rows]
partz_times = [benchmark_avg for _, _, benchmark_avg in summary_rows]

x = np.arange(len(labels))
width = 0.35

fig, ax = plt.subplots(figsize=(10, 6))
bars1 = ax.bar(x - width / 2, liquid_times, width, label="Liquid Clustering (date_key only)", color="#0F6E6A")
bars2 = ax.bar(x + width / 2, partz_times, width, label="Partition + Z-order (on street_key)", color="#B45309")

ax.set_ylabel("Avg warm query time (s)")
ax.set_title("Liquid Clustering vs. Partition+Z-order — real measured times")
ax.set_xticks(x)
ax.set_xticklabels(labels, rotation=10, ha="right")
ax.legend()
ax.bar_label(bars1, fmt="%.2fs", padding=3)
ax.bar_label(bars2, fmt="%.2fs", padding=3)
plt.tight_layout()
plt.show()

# COMMAND ----------

evidence = {
    "liquid_table": LIQUID_TABLE,
    "benchmark_table": BENCHMARK_TABLE,
    "liquid_row_count": liquid_count,
    "benchmark_row_count": benchmark_count,
    "sample_month_row_count": sample_month_count,
    "q1_liquid": q1_liquid_results,
    "q1_benchmark": q1_benchmark_results,
    "q2_liquid": q2_liquid_results,
    "q2_benchmark": q2_benchmark_results,
    "summary": [
        {
            "query": label,
            "liquid_avg_warm_s": liquid_avg,
            "partition_zorder_avg_warm_s": benchmark_avg,
            "delta_s": benchmark_avg - liquid_avg,
            "pct_diff": pct_diff(liquid_avg, benchmark_avg),
        }
        for label, liquid_avg, benchmark_avg in summary_rows
    ],
}
dbutils.notebook.exit(json.dumps(evidence, default=str))
