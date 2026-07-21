# Day 8-9: Resource Usage Analysis dashboard

Closes the "no visibility into what the pipeline actually costs to run" gap.
Done directly on `dev`, no new feature branch.

## Step 0 — is real DBU data even queryable on Free Edition?

The prompt itself was explicitly uncertain whether `system.billing.usage` (or
an equivalent) would be accessible on a Free Edition workspace. Rather than
assume either way, ran real queries first via the project's established
scratch-notebook-as-a-job pattern (the SQL Statement API's OAuth is broken in
this environment -- confirmed again below, still broken, unrelated to this
work):

- `SELECT * FROM system.billing.usage LIMIT 10` -- **real, populated**, 1,082
  historical rows.
- `system.access.audit`, `system.lakeflow.job_run_timeline` -- also real and
  populated.
- `system.compute.node_timeline` -- 0 rows, expected: this project is 100%
  serverless, there are no traditional compute-cluster nodes to report.

This directly contradicts the prompt's own stated uncertainty. No proxy/
fallback metric was needed -- the dashboard below is built entirely on real
`system.billing.usage` data, not job-run-history as a stand-in for cost.

## Step 0b — a real attribution problem, found before building anything

`DESCRIBE TABLE system.billing.usage` confirmed the real shape of
`usage_metadata` (a struct with `job_id`, `job_name`, `job_run_id`,
`notebook_path`, `dlt_pipeline_id`, and more). A first pass grouping by
`usage_metadata.job_name` looked plausible -- it surfaced `Data Chunking`,
`Bronze COPY INTO`, and `Bronze Auto Loader / PySpark / DLT Ingestion` with
real DBU numbers -- but **`Silver Layer` and `Gold Layer` never appeared**.

Cross-checked the real job IDs from `databricks jobs list` against
`system.billing.usage` directly and found the cause: `silver_job` and
`gold_job` are each a single `pipeline_task` wrapping a DLT pipeline. The
actual compute for a DLT pipeline refresh is billed under
`usage_metadata.dlt_pipeline_id`, with `job_id`/`job_name` **null** on those
billing rows -- even though the pipeline was triggered by a job. A dashboard
built on `job_name` alone would have silently under-counted total project DBU
by roughly two-thirds (job-attributed: ~16.5 DBU vs. pipeline-attributed:
~30.8 DBU, in the sample pulled during this investigation).

Also found: `system.lakeflow.pipelines` shows **3 different
`dlt_pipeline_id` values all named `"[dev rohitrathodcomp] Silver Layer"`**
-- the DLT pipeline gets recreated (new ID) across some redeploys while
keeping the same display name. Hardcoding a `dlt_pipeline_id` would silently
go stale the next time that happens.

**Fix:** every dataset query joins `system.billing.usage` against both
`system.lakeflow.jobs` (on `job_id`) and `system.lakeflow.pipelines` (on
`dlt_pipeline_id`) by **name**, not by hardcoded ID, then `UNION ALL`s the two
attribution paths together. This also naturally scopes the dashboard to only
this project's bundle-deployed resources: real jobs/pipelines get the
bundle-injected `[<target> <user>]` name prefix (confirmed via
`databricks jobs list`), while this session's own ad-hoc scratch/verification
jobs (`silver-profiling-scratch`, `check_date_range_run`, dozens of others
visible in the raw billing data) do not, and are excluded by the
`name LIKE '[%] %'` filter on the jobs/pipelines side. Unrelated workspace
activity in this shared Free Edition account (a `Street Safety RLS-CLS
Implementation` notebook, a serverless model-serving endpoint burning ~220
DBU on its own) is excluded the same way.

## Real finding: this project has exactly one SKU

`GROUP BY sku_name` scoped to just this project's jobs/pipelines returns a
**single row**: `PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO`. Every task in
every job/pipeline here runs on serverless jobs compute -- there is no
all-purpose cluster, no SQL warehouse compute, and no GPU/model-serving usage
attributable to this pipeline. The dashboard's SKU panel reports this
honestly (one row) rather than fabricating variety; the wider 7-SKU mix seen
in the unscoped, whole-workspace query belongs to unrelated activity in the
same shared account, not to this pipeline.

## What the dashboard shows

`resources/dashboards/resource_usage_dashboard.yml` +
`resources/dashboards/resource_usage_dashboard.lvdash.json` -- deployed as a
DAB resource the same way as every job/pipeline in this project
(`databricks bundle deploy -t dev`). No hardcoded catalog/schema in the SQL
(the system tables queried are account/workspace-level, not per-catalog, so
`${var.catalog}` substitution doesn't apply here); the one environment-
specific value is `warehouse_id`, pulled from a new `sql_warehouse_id`
bundle variable (defaults to this Free Edition workspace's only warehouse,
`Serverless Starter Warehouse`).

One page, six widgets, all backed by the same validated join/attribution
logic described above:

1. **Title/methodology text widget** -- states plainly, in the dashboard
   itself (not just this doc), that this is real DBU data scoped to project
   resources via name-prefix join, that Silver/Gold billing comes through the
   `dlt_pipeline_id` path, and that all project usage falls under one SKU.
2. **Counter** -- grand total DBU across the whole project.
3. **Bar chart** -- total DBU by layer (Chunking / Bronze / Silver / Gold).
4. **Table** -- total DBU by individual job/pipeline component, with run
   counts.
5. **Line chart** -- daily DBU trend, one line per layer.
6. **Table** -- DBU by SKU (the one-row finding above).

## Verification

- `databricks bundle validate -t dev` -- passes.
- `databricks bundle deploy -t dev` -- deploys cleanly; confirmed via
  `databricks lakeview get <dashboard_id>` that the dashboard is
  `lifecycle_state: ACTIVE` and that all 5 dataset queries were parsed
  without error into `queryLines` (proves the hand-authored JSON and SQL are
  both syntactically valid).
- Every dataset's exact SQL text was independently run and confirmed to
  return real, non-empty, non-fabricated data via the project's established
  scratch-notebook-as-a-job pattern against the same underlying
  `system.billing.usage` / `system.lakeflow.jobs` / `system.lakeflow.pipelines`
  tables the dashboard itself queries.
- **Known limitation, not introduced by this work:** the SQL Statement
  Execution API's OAuth is broken in this environment (`token refresh:
  ... Refresh token is invalid`), so the dashboard's queries could not be
  triggered directly through the warehouse via CLI/API to screenshot actual
  rendered panels. The SQL was instead verified by running the identical
  query text against the identical real tables via a notebook job (the
  project's standing workaround for this same OAuth issue). Visually
  confirming the rendered dashboard in the browser is the one remaining step
  a human needs to do:
  `https://dbc-fb8578aa-62d7.cloud.databricks.com/dashboardsv3/01f183ca29a61c33ad3cd140785853ed/published?w=7474652998320895`
- All scratch investigation notebooks (`_investigate_billing_schema`,
  `_investigate_dlt_billing`, `_validate_dashboard_queries`) deleted from the
  workspace after use.
- No catalog/DBU numbers in this doc or the dashboard are fabricated --
  every figure came from a real, timestamped query result during this
  investigation.
