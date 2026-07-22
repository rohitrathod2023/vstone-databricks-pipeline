# Day 7: Liquid Clustering vs. partitioning + Z-ordering benchmark

A real, measured comparison between Delta Lake Liquid Clustering and
traditional partitioning + `OPTIMIZE ... ZORDER BY`, run live against the
current `fact_city_observations` (112,586,955 rows,
`vstone_traffic_dev.dev_rohitrathodcomp_gold`) — superseding an earlier
version of this benchmark that ran against `fact_street_conditions`, a
table from before this project's fact tables were unified into one
(`fact_street_conditions` + `fact_traffic_counts` → `fact_city_observations`).

## Why `fact_city_observations`

There's only one fact table now — the unification already documented in
`docs/gold_data_model.md` replaced the two separate fact tables this
benchmark originally targeted. `fact_city_observations` is also the biggest
table in Gold (112.6M rows, vs. the largest individual `agg_*` table's tens
of thousands) and the one most likely to be filtered on `street_key`/
`date_key` in real queries — the same reasoning that picked
`fact_street_conditions` over `fact_traffic_counts` originally still holds,
just at the unified-table level.

## Important caveat, found while running this — read before the numbers

**The currently deployed `fact_city_observations` is clustered on
`date_key` alone** (`cluster_by=["date_key"]` — confirmed live via
`DESCRIBE TABLE EXTENDED`: `clusteringColumns=[["date_key"]]`), not on
`(street_key, date_key)` the way the original `fact_street_conditions`
benchmark's table was. This is a side effect of a separate, still-pending
design decision on this project (removing the `observation_type` column
reduced the clustering key from `["observation_type", "date_key"]` down to
`["date_key"]` — see `docs/fact_table_without_discriminator_alternative.md`).

That matters a lot for this benchmark: **the test queries below filter on
`street_key`, but Liquid Clustering here isn't actually organized around
`street_key` at all.** The partition+Z-order comparison table, by contrast,
*is* deliberately `ZORDER BY (street_key)`. So this run is not a clean
apples-to-apples "which strategy handles the same access pattern better" —
it's closer to "a table clustered on the wrong column vs. a table
explicitly organized around the column being filtered." The result below
should be read with that in mind, not as a general verdict on Liquid
Clustering vs. Z-ordering.

## Applying Liquid Clustering — same platform limitation as before

`fact_city_observations`'s `cluster_by` is declared directly in its
`@dlt.table` decorator in `dlt_gold_tables.py`, at table-creation time —
`ALTER TABLE ... CLUSTER BY` still cannot be applied post-hoc to a DLT
materialized view (`EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE`, the same
limitation documented for PK/FK constraints and the original version of
this benchmark).

## The comparison table

`fact_city_observations_zorder_benchmark` — a **plain Delta table**, built
live via `CREATE TABLE ... AS SELECT` (not a DLT table, not part of the Gold
pipeline). Deliberately **not** added to `docs/gold_data_model.md`, the ER
diagram, `sources.yml`, or the PK/FK constraint set — a benchmark artifact,
not a real table in the design, same convention as before.

```sql
CREATE OR REPLACE TABLE fact_city_observations_zorder_benchmark
USING DELTA
PARTITIONED BY (year, month)
AS SELECT *,
   CAST(date_key / 10000 AS INT) AS year,
   CAST(MOD(CAST(date_key / 100 AS INT), 100) AS INT) AS month
FROM fact_city_observations;

OPTIMIZE fact_city_observations_zorder_benchmark ZORDER BY (street_key);
```

`OPTIMIZE` real stats from this run: 10 partitions optimized, 10 files
merged down to 26 output files, ~80s total task execution time across 8
parallel tasks.

**Row-count parity, confirmed live:**

| Table | Row count |
|---|---|
| `fact_city_observations` (Liquid Clustering, `date_key` only) | 112,586,955 |
| `fact_city_observations_zorder_benchmark` (partition + Z-order on `street_key`) | 112,586,955 |

## Benchmark queries

A real, non-edge-case slice confirmed live: `street_key=1`, August 2023 has
**267,840 rows** — same number as the original benchmark (same underlying
environmental data, just carried through the fact-table unification).

Each table queried using its own natural column set for the access pattern
— `fact_city_observations` filters on `street_key`/`date_key` directly; the
benchmark table filters on partition columns `year`/`month` plus
`street_key`. 3 runs each, first run discarded as cold cache, remaining 2
runs averaged — same methodology as the original benchmark.

**Query 1 — point lookup:** average pollution, one street, one month.

```sql
-- fact_city_observations (Liquid Clustering, date_key only)
SELECT AVG(pollution), COUNT(*) FROM fact_city_observations
WHERE street_key = 1 AND date_key BETWEEN 20230801 AND 20230831

-- fact_city_observations_zorder_benchmark (partition + Z-order on street_key)
SELECT AVG(pollution), COUNT(*) FROM fact_city_observations_zorder_benchmark
WHERE street_key = 1 AND year = 2023 AND month = 8
```

Both returned identical results (`avg_pollution=0.16446831459001987`,
`n=267840`) — confirms the benchmark table is a faithful copy, not a
divergent one.

**Query 2 — range scan:** all readings for one street across the full
observed date range.

```sql
SELECT COUNT(*), AVG(noise), AVG(pollution) FROM <table> WHERE street_key = 1
```

Both returned `n=2,438,246` and matching (to floating-point rounding)
`avg_noise`/`avg_pollution` values.

## Real results

| Query | Liquid Clustering, `date_key` only (avg warm, s) | Partition + Z-order on `street_key` (avg warm, s) | Delta (s) | % difference |
|---|---|---|---|---|
| Q1: avg pollution, street=1, Aug 2023 | 2.475 | 2.305 | -0.170 | -6.8% |
| Q2: all readings, street=1, full range | 2.692 | 2.225 | -0.467 | -17.4% |

(% difference = partition+Z-order relative to Liquid Clustering; negative
means partition+Z-order was faster.)

**Honest finding, with the caveat front and center: partition+Z-order won
both queries this time, by a real and non-trivial margin — but this is not
evidence that Z-ordering is better than Liquid Clustering in general.** As
flagged above, the deployed `fact_city_observations` is clustered on
`date_key` alone, so it gets **zero** data-skipping benefit from Liquid
Clustering for the `street_key` predicate these queries actually filter on
— all the pruning it manages comes from the `date_key` half of each query.
The partition+Z-order table, by contrast, was deliberately organized
*specifically* around `street_key` via `ZORDER BY`. This result mostly
demonstrates that **a clustering key must actually match the query's filter
columns to help** — a table clustered on the wrong column loses to one
explicitly tuned for the access pattern, regardless of which clustering
technology either one uses. That's a real, useful finding, just a different
one than "Z-order beats Liquid Clustering."

**For a genuinely fair rematch**, `fact_city_observations` would need
`cluster_by=["street_key", "date_key"]` (matching what the original
`fact_street_conditions` benchmark used, and what `dev`'s current
`fact_city_observations` schema still declares in `table_schemas.py`'s FK
section, even though the actually-deployed table here only clusters on
`date_key`). That's a separate, real design question — not something to
change just to make this benchmark come out differently — but worth
re-running once the `observation_type` design question (see
`docs/fact_table_without_discriminator_alternative.md`) is settled and the
fact table's final `cluster_by` is locked in.

## Verification

- Both tables confirmed at exactly 112,586,955 rows.
- Query results identical (to floating-point rounding) between both tables
  for both Q1 and Q2 — the benchmark table is a faithful copy.
- `fact_city_observations`'s clustering confirmed live via
  `DESCRIBE TABLE EXTENDED`: `clusteringColumns=[["date_key"]]`.
- All queries run live against the real deployed Databricks SQL warehouse
  (`vstone_traffic_dev.dev_rohitrathodcomp_gold`), not estimated or
  simulated.
