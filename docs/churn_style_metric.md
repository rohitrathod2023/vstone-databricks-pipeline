# Day 7: churn-style metric — street activity recency

Adapted from the brief's example ("customers with no transaction in the
last 6 months"): this dataset has no customers, but it does have streets
that should be producing a steady stream of environmental sensor readings.
The equivalent signal is **"which streets have gone quiet"** — the same
shape of problem (an entity that used to be active going silent), applied
to sensors instead of purchases.

## Where it lives

**A standalone demo notebook**
(`src/notebooks/10_churn_style_street_activity_recency.py`), not a DLT
table — same pattern already used for the Liquid Clustering benchmark
(`06_liquid_clustering_benchmark.py`): a one-off analysis for the Day 10
walkthrough, not an ongoing pipeline table. Reuses the real, fully tested
build function (`src/pipelines/gold/agg_street_activity_recency.py`, 6/6
unit tests passing), just invoked from the notebook instead of a
`@dlt.table` decorator.

An earlier version of this work wired it into the Gold DLT pipeline as a
real `agg_*` table (queryable by Genie, auto-refreshed with every Gold
run) — reverted in favor of the notebook approach on request, trading
Genie-queryability and auto-refresh for a lighter, faster-to-demo artifact
that doesn't add a table to the live pipeline.

## What it computes

One row per street: `last_observation_date`, `days_since_last_observation`
(measured against the true end of the observed dataset — see below, not
today's real-world date), and `is_churned` (`days_since_last_observation
>= CHURN_THRESHOLD_DAYS`, default 30).

## Honest finding — checked live, not assumed

**The real dataset has zero churn.** Confirmed via a direct query before
building anything: all 36 streets (and, checked the same way, all 13
locations) have complete, gap-free data across all 283 observed days, from
the very first day (2023-06-02) to the very last (2024-03-10). So running
the real build function today reports `is_churned=False` for every street —
a correct, verified baseline, not a bug or a sign the logic doesn't work.

## Proving the logic actually works, given real data can't

Since the real data never exercises the "churned" path, the notebook has a
second section that injects one illustrative gap into a **copy** of the
real data (nothing is written anywhere) — cutting one real street's
readings off 60 days before the dataset's true end date — and confirms the
function correctly flags it as churned. Both sections render as a grouped
bar chart with a threshold line at the churn cutoff, so the "before" (real,
all quiet) and "after" (synthetic, one street crosses the line) are visibly
different, not just two rows in a table.

## Design notes

- **`CHURN_THRESHOLD_DAYS = 30`**, a named constant (not hardcoded inline,
  per the brief's "No HARD CODING" standard), passed as a real parameter —
  proven configurable by a dedicated unit test that runs the same synthetic
  gap through both a 30-day and a 10-day threshold and gets different
  `is_churned` results.
- **The "as of" reference date** is `MAX(date_key)` across the *whole* fact
  table (all branches, not just environmental readings) — this dataset is a
  fixed historical snapshot, not a live feed, so "days since" has to be
  measured against where the data collection actually stopped, not the
  calendar date the notebook happens to run on. A dedicated unit test
  confirms this reference date is correctly drawn from the whole table,
  including branches with no `street_key` at all (e.g. traffic-only rows),
  not just the environmental subset being aggregated.
- **No `observation_type` column to filter on** — environmental rows are
  identified via `noise.isNotNull()`, the same measure-nullness convention
  used by every other `agg_*` table in this design (see
  `docs/fact_table_without_discriminator_alternative.md`).
