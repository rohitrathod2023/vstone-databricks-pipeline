# Requirement Documentation & Assumptions

## Source
`project-vstone-v7-20260504.pdf` — "Internal Project - Databricks", Version 7.0, May 01 2026.

## Scope
End-to-end Bronze → Silver → Gold medallion pipeline on Databricks Free
Edition (serverless only — no managed clusters, no DBX trial), ingesting
multi-format data (CSV, JSON, XML), governed by Unity Catalog, deployed via
Databricks Asset Bundles, with Git branching + CI/CD. Individual project — no
shared codebase; groups exist only for common connects/evaluations. All
dataset files except metadata files must be ingested through to Gold.

## Known discrepancy — resolved
The onboarding email named the Kaggle dataset "traffic simulator"
(`xxjcaxx/trafficsimulator`) as the primary dataset. The shared Google Drive
zip instead contained an unrelated airline flight-delay dataset
(`AirLineData.csv`).

**Decision: proceeding with the real traffic-simulator dataset**, not the
airline data. The airline data and its pre-made chunks/utility scripts are
not part of the final build — they were only formatting examples (see
`csv_splitter.py` and related scripts, reused for the *mechanism*, not the
*data*).

## Key design decisions and rationale

**Two independent domains, modeled as a fact constellation.** `cars.csv` /
`node_locations.csv` and `streets.csv` / `streets_list.csv` don't share a
row-level key. Rather than forcing a join that doesn't exist, Gold models two
separate fact tables (`Fact_Traffic_Counts`, `Fact_Street_Conditions`)
bridged only through a shared `Dim_Date`.

**Chronological, not equal, chunking (40/30/20/10).** `cars.csv` is split by
`date` order — earliest 40% first — rather than four equal quarters, because
it mirrors a real pipeline's growth pattern (one big historical backfill,
then progressively smaller incremental drops) and lets each Bronze technique
map naturally to the batch size it's best suited for (COPY INTO for bulk,
Auto Loader/DLT for smaller incremental, PySpark for the riskiest/smallest
XML write path). Full rationale in the project context notes.

**`streets.csv` (7.8GB) is not chunked or format-converted.** It's ingested
directly as CSV via COPY INTO and deliberately held back whole — it's the
large table reserved for the Day 7 Liquid Clustering vs. partitioning/Z-order
benchmark, where table size is the point.

**Data quality issue, flagged not silently dropped.** `node_locations.csv`
row `location=7` has invalid coordinates `(0,0)`. This is surfaced and
quarantined in Silver, not fixed by guessing real coordinates.

**`telegram.csv` has no key-based relationship** to any other table — only a
loose date/time association. It's treated as a supporting reference/log
table, not formalized into the fact/dimension model (bonus/stretch scope
only).

## Assumptions

- Databricks Free Edition provides Unity Catalog, Volumes, Delta Lake, Jobs,
  and serverless SQL/compute — confirmed sufficient for every requirement in
  the brief without a paid tier.
- "dev / test / prod" targets in `databricks.yml` map to three Unity Catalog
  catalogs (`vstone_traffic_dev/_test/_prod`) within the single Free Edition
  workspace, not three separate workspaces, since Free Edition is single-workspace.
- Job tasks omit an explicit cluster/environment spec to run on serverless
  compute by default, per Free Edition's serverless-only constraint — to be
  confirmed on first `databricks bundle deploy`, and adjusted with an explicit
  `environments:` block if the workspace requires one.
- Email failure notifications go to the project owner's email
  (rohitrathodpersonal@gmail.com); Slack notification integration is out of
  scope unless time permits.
