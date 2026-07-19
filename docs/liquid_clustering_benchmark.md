# Day 7: Liquid Clustering vs. partitioning + Z-ordering benchmark

A real, measured comparison between Delta Lake Liquid Clustering and
traditional partitioning + `OPTIMIZE ... ZORDER BY`, run against
`fact_street_conditions` (~87.8M rows) on `feature/gold-layer`. Not merged
to `dev` -- the incremental-load work continues on this same branch.

## Why `fact_street_conditions`, not `fact_traffic_counts`

`fact_street_conditions` (87,776,721 rows) is actually the larger of the two
fact tables -- `fact_traffic_counts` has 24,681,794 rows, smaller by more
than 3x. Row count aside, `fact_traffic_counts` also has a simpler, less
clustering-sensitive access pattern: it's mostly aggregated straight by
`location_key`/month (`gold_monthly_traffic_summary` already does this).
`fact_street_conditions` is both the biggest real dataset in Gold and the
table most likely to be filtered/joined on `street_key` and `date_key` in
real queries -- e.g. "average pollution for street X in a given month,"
exactly the kind of query this benchmark runs. That access pattern is what
clustering strategies are actually built to help with, so it's the more
meaningful table for this exercise.

## Applying Liquid Clustering — a real platform limitation hit again

The first attempt was `ALTER TABLE fact_street_conditions CLUSTER BY
(street_key, date_key)` directly against the existing table. This failed:

```
[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'ALTER TABLE ... CLUSTER BY'
expects a table but `vstone_traffic_dev`.`dev_rohitrathodcomp_gold`.`fact_street_conditions` is a view.
```

The exact same limitation already documented for PK/FK constraints in
`docs/gold_data_model.md`: Unity Catalog registers a DLT materialized view
as a `VIEW` object, so a post-hoc `ALTER TABLE` can't be applied to it
regardless of what the ALTER is trying to do.

The fix follows the same pattern that worked for PK/FK: declare `cluster_by`
directly in the `@dlt.table` decorator, at table-creation time, instead of
altering the table after the fact:

```python
@dlt.table(
    name="fact_street_conditions",
    comment=_FACT_STREET_CONDITIONS_CFG["description"],
    schema=FACT_STREET_CONDITIONS_SCHEMA,
    cluster_by=["street_key", "date_key"],
)
def fact_street_conditions():
    ...
```

Deployed and full-refreshed. Confirmed live via `DESCRIBE TABLE EXTENDED`:

```
Table Properties: [clusteringColumns=[["street_key"],["date_key"]], ...]
```

Row count after the full-refresh: **87,776,721** -- unchanged, exact match
to the already-verified `silver_environment` accepted count. `table_type`
in `information_schema.tables` still reports `MATERIALIZED_VIEW` -- applying
clustering this way doesn't change the table's management model, same as
the PK/FK constraints didn't.

## The comparison table

`fact_street_conditions_zorder_benchmark` -- a **plain Delta table**, not a
DLT table, built by a one-off script
(`src/notebooks/06_liquid_clustering_benchmark.py`), not part of the Gold
DLT pipeline. Deliberately **not** added to `docs/gold_data_model.md`, the
ER diagram, `sources.yml`, or the PK/FK constraint set -- it's a benchmark
artifact, not a real table in the design.

Built from the exact same source data as `fact_street_conditions` (a
straight `spark.table(...)` read, not a re-derivation from Silver), with
two derived columns added for partitioning purposes (not present on the
real table):

```python
with_partition_cols = (
    spark.table("fact_street_conditions")
    .withColumn("year", (F.col("date_key") / 10000).cast("int"))
    .withColumn("month", ((F.col("date_key") / 100) % 100).cast("int"))
)
with_partition_cols.write.format("delta").partitionBy("year", "month").saveAsTable(
    "fact_street_conditions_zorder_benchmark"
)
```

...followed by `OPTIMIZE fact_street_conditions_zorder_benchmark ZORDER BY
(street_key)`.

**Row-count parity, confirmed:**

| Table | Row count |
|---|---|
| `fact_street_conditions` (Liquid Clustering) | 87,776,721 |
| `fact_street_conditions_zorder_benchmark` (partition + Z-order) | 87,776,721 |

Both exactly match `silver_environment`'s already-verified accepted-row
count -- the benchmark table is a faithful copy, not a sample.

## Benchmark queries

A real, non-edge-case slice was confirmed before benchmarking:
`street_key=1`, August 2023 (fully inside the observed 2023-06-02 to
2024-03-10 date range for every street) has **267,840 rows** -- a real,
meaningful chunk, not a near-empty edge case.

Each table is queried using its own natural column set for the access
pattern -- `fact_street_conditions` filters on `date_key` directly (relying
on Liquid Clustering over `street_key`/`date_key`); the benchmark table
filters on its partition columns `year`/`month` plus `street_key` (relying
on partition pruning + Z-order). Filtering the partitioned table on
`date_key` alone -- ignoring the very columns it was partitioned by -- would
get no partition pruning at all and would be an unfair, unrealistic test,
not a like-for-like comparison of each strategy queried the way it's
actually meant to be queried.

**Query 1 — point lookup:** average pollution, one street, one month.

```sql
-- fact_street_conditions (Liquid Clustering)
SELECT AVG(pollution), COUNT(*) FROM fact_street_conditions
WHERE street_key = 1 AND date_key BETWEEN 20230801 AND 20230831

-- fact_street_conditions_zorder_benchmark (partition + Z-order)
SELECT AVG(pollution), COUNT(*) FROM fact_street_conditions_zorder_benchmark
WHERE street_key = 1 AND year = 2023 AND month = 8
```

**Query 2 — range scan:** all readings for one street across the full
observed date range.

```sql
SELECT COUNT(*), AVG(noise), AVG(pollution) FROM <table> WHERE street_key = 1
```

3 runs each, first run discarded as cold cache, remaining 2 runs averaged.

## Real results

| Query | Liquid Clustering (avg warm, s) | Partition + Z-order (avg warm, s) | Delta (s) | % difference |
|---|---|---|---|---|
| Q1: avg pollution, street=1, Aug 2023 | 1.579 | 1.440 | -0.139 | -8.8% |
| Q2: all readings, street=1, full range | 1.257 | 1.265 | +0.008 | +0.6% |

(% difference = partition+Z-order relative to Liquid Clustering; negative
means partition+Z-order was faster.)

**Honest finding: the difference is not dramatic at this data volume.**
Partition+Z-order came out modestly faster on the point-lookup query (Q1,
~9%) and effectively tied on the range-scan query (Q2, <1% apart, well
within normal run-to-run noise). At ~88M rows on Databricks Free Edition's
serverless compute, neither clustering strategy shows a decisive advantage
for either query shape tested here -- the two approaches perform within
single-digit percentage points of each other. This is a legitimate,
reportable result, same as the Phase 5 `raining`-range lesson and every
other real-verification finding in this project: report what actually
happened, not what "should" happen in theory.

A plausible reason this table doesn't show a bigger gap: `street_key` only
has 36 distinct values, so even a Z-ordered/partitioned physical layout and
Liquid Clustering's own layout both end up co-locating a given street's rows
fairly effectively at this cardinality and row count -- the theoretical
advantage of Liquid Clustering (better behavior under high-cardinality,
frequently-changing clustering keys, and no need to manually re-run
`OPTIMIZE`) would likely show up more clearly at higher data volumes, more
skewed access patterns, or over many incremental writes where Z-order's
one-time file layout degrades and Liquid Clustering's incremental
maintenance keeps re-optimizing automatically -- none of which this
single-snapshot, 88M-row benchmark exercises.

## Verification

- Both tables confirmed at exactly 87,776,721 rows, matching
  `silver_environment`'s already-verified accepted-row count.
- `fact_street_conditions`'s `table_type` confirmed still `MATERIALIZED_VIEW`
  in `information_schema.tables` after adding `cluster_by` -- the
  materialized-view design is unaffected.
- `flake8` clean (`src/notebooks/` is excluded from lint per `setup.cfg`,
  same convention as every other notebook file in this project -- injected
  `spark`/`dbutils` globals aren't real names outside a Databricks
  notebook).
- Full unit test suite re-run after the `cluster_by` change to
  `dlt_gold_tables.py` -- unaffected, since `build_fact_street_conditions`'s
  actual transformation logic wasn't touched, only the DLT table
  declaration.
