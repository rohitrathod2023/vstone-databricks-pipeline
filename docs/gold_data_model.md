# Gold data model — dimensions, facts, and PK/FK relationships

## Star schema overview

```
Dim_Date ──────────┐
                    ├──< Fact_Traffic_Counts
Dim_Location ───────┘

Dim_Date ──────────┐
                    ├──< Fact_Street_Conditions
Dim_Street ─────────┘   (SCD2 -- range join on __START_AT/__END_AT)
```

Two independent fact tables, not one combined fact — Kimball grain-purity:
`Dim_Location` and `Dim_Street` share no key and no real-world overlap (a real
query confirmed zero coordinate overlap between them), so combining their
facts into a single table would force a false relationship between two
unrelated measurement processes.

## Tables and surrogate keys

| Table | Type | Surrogate key (PK) | Natural key | Notes |
|---|---|---|---|---|
| `Dim_Date` | Materialized view | `date_key` (int, `YYYYMMDD`) | `full_date` | Generated, no source file |
| `Dim_Location` | Materialized view | `location_key` (int) | `location` | 13 rows — `location=7` excluded (bad coordinates, rejected at Silver) |
| `stg_dim_street_scd2` | Streaming table | — (internal CDC plumbing, not a public dimension) | `street_id` | `AUTO CDC FROM SNAPSHOT` target; not part of this model |
| `Dim_Street` | Materialized view | `street_key` (int) | `street_id` + `__START_AT` | SCD2 — full version history, `is_current` flag |
| `Fact_Traffic_Counts` | Materialized view | — (no surrogate; natural grain is `id`+`location_key`+`date_key`) | — | 24,681,794 rows, one per `silver_traffic` row |
| `Fact_Street_Conditions` | Materialized view | — (grain is `street_key`+`date_key`, many rows per pair) | — | 87,776,721 rows, one per accepted `silver_environment` row |
| `gold_monthly_traffic_summary` | Materialized view | — (grain is `location_key`+`year`+`month`) | — | 140 rows (14 location groups incl. NULL x 10 months) |
| `gold_street_risk_summary` | Materialized view | — (grain is `street_id`+`year`+`month`) | — | 360 rows (36 streets x 10 months) |

## Foreign keys (intended relationships)

| From | Column | To | Column |
|---|---|---|---|
| `Fact_Traffic_Counts` | `location_key` | `Dim_Location` | `location_key` |
| `Fact_Traffic_Counts` | `date_key` | `Dim_Date` | `date_key` |
| `Fact_Street_Conditions` | `street_key` | `Dim_Street` | `street_key` |
| `Fact_Street_Conditions` | `date_key` | `Dim_Date` | `date_key` |

## Known, expected orphan: `Fact_Traffic_Counts.location_key`

2,373,327 of 24,681,794 rows (all real `location=7` traffic readings) have a
`NULL` `location_key` — `location=7`'s coordinates were quarantined at
Silver (`silver_locations_rejected`), so it was correctly excluded from
`Dim_Location`, but traffic sensor readings for that location still exist
(a separate domain). This is surfaced deliberately via a `LEFT JOIN`
(an `INNER JOIN` would have silently dropped these rows instead), not a bug.

## Business aggregates

Two materialized views roll the fact tables up to a monthly grain:

- **`gold_monthly_traffic_summary`** — `Fact_Traffic_Counts` grouped by
  `location_key` x calendar month, with `busiest_rank_in_month` (a
  descending rank of `total_traffic_volume` within each month) making
  "top-10 busiest intersections" a trivial `WHERE busiest_rank_in_month <=
  10` filter. Verified: 140 rows = 14 location groups (13 real locations +
  1 `NULL` group carrying `location=7`'s orphaned readings, same orphan
  documented above) x 10 months in the observed date range. Real result,
  checked live: `location_key=6` is the busiest intersection in 9 of the
  10 observed months (peaking at 13,695,050 in August 2023); `location_key=3`
  is the only other location to break into the overall top 10 (rank 2,
  August 2023).
- **`gold_street_risk_summary`** — `Fact_Street_Conditions` grouped by
  `street_id` x calendar month, joined back to `Dim_Street`'s full SCD2
  history to pick up the `dangerous` rating in effect that month
  (`dangerous_rating_this_month` uses `F.first()`, not `F.avg()` — an
  average over millions of rows of a value that's constant per SCD2
  version introduced floating-point summation drift on the order of
  1e-14, enough to make a genuinely unchanged rating misreport as
  "increased"/"decreased"; `F.first()` reads the stored value directly
  with no accumulation error). `risk_changed_flag` (crosses the 0.5
  safe/dangerous threshold) and `risk_direction` (`increased`/
  `decreased`/`stable`) compare each street's rating against the prior
  calendar month via `LAG`. Verified: 360 rows = 36 streets x 10 months,
  `risk_changed_flag = true` for 0 rows and `risk_direction = "stable"`
  for all 360 — expected on this first build, since every street still has
  exactly one `Dim_Street` SCD2 version and there is no real rating change
  yet to detect. This becomes a meaningful signal once a street's
  `dangerous` score actually changes across a `Dim_Street` refresh.

Both aggregates are wired into their own DABs job, `gold_aggregates_job`
(`resources/jobs/gold_aggregates_job.yml`), named by function rather than
by build day. Since every Gold table lives in the one `gold_dlt_pipeline`
DLT pipeline, this job's `pipeline_task` targets the same pipeline as
`gold_job` — DLT's own dependency graph and incremental engine ensure nothing
already up to date gets wastefully recomputed, and the job exists as its own
named, function-scoped entry point for scheduling the aggregate refresh
independently of the raw dimension/fact build.

## Constraint enforcement — a real Unity Catalog limitation, not just "not enforced"

Databricks documents Unity Catalog primary/foreign keys as **informational
only** (not enforced like an RDBMS — "it is the user's responsibility to
check whether a constraint is satisfied"). But the actual limitation found
building this model is stronger than that framing suggests: **`ALTER TABLE
... ADD CONSTRAINT` cannot be attached to a DLT materialized view at all.**
Confirmed live against every table above:

```
[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'ALTER TABLE ... ADD CONSTRAINT'
expects a table but `...`.`dim_location` is a view.
```

Unity Catalog registers a DLT materialized view as a `VIEW` object, not a
plain table, regardless of the real Delta table backing it internally — so
this isn't a matter of the constraint being unenforced once added, it's that
no constraint can be added in the first place while these tables stay
materialized views. Since every Gold table here is a materialized view by
design (the already-agreed default, with `stg_dim_street_scd2` the sole
streaming-table exception for CDC plumbing), the PK/FK relationships above
are documented as the model's real, intended design — not registered as
literal Unity Catalog constraints. Converting any of these to plain
Delta tables outside DLT purely to attach a constraint would abandon the
materialized-view design already agreed for Gold, so this was deliberately
not done.
