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

## Audit column lineage: carried through from Bronze, not regenerated per layer

`load_dt`/`source_format`/`source_file`/`run_id` originate once, at Bronze
ingestion, and are carried through Silver and into every Gold table
unchanged -- Gold no longer calls `add_audit_columns()` to stamp its own
fresh values. Two categories of exception:

- **Dimensions and facts carry through their upstream source's audit
  columns.** `Dim_Location`/`Dim_Street` select them straight from
  `silver_locations`/`stg_dim_street_scd2` (itself carried through
  `AUTO CDC FROM SNAPSHOT` from `silver_streets` -- confirmed live that
  untracked source columns pass through the CDC flow unchanged).
  `Fact_Traffic_Counts`/`Fact_Street_Conditions` carry through their
  *fact-side* table's columns specifically (`silver_traffic`/
  `silver_environment`), not `Dim_Location`/`Dim_Street`/`Dim_Date`'s --
  those are pure lookups in the join, and a fact row's real lineage is the
  fact record it came from, not whichever dimension row it resolved to.
- **`Dim_Date` and the two aggregate tables keep their own "generated"
  stamp.** `Dim_Date` has no Bronze file behind it at all (a calendar is
  computed, not ingested). `gold_monthly_traffic_summary`/
  `gold_street_risk_summary` are `GROUP BY` aggregates over potentially
  millions of source rows each -- there is no single row's lineage to carry
  through an aggregation, so these keep `source_format="generated"` with a
  fresh `load_dt`/`run_id` reflecting when the aggregate was actually
  computed.

Verified live post-full-refresh: every carried-through table's `load_dt`
reflects the real original Bronze/Silver ingestion time (`2026-07-13`), not
the Gold refresh's own run time, and `Fact_Traffic_Counts.source_file`
correctly varies across all 4 real Bronze files (`chunk1.csv`/`chunk2.csv`/
`chunk3.json`/`chunk4.xml`) depending on which technique a given row came
from -- confirming per-row lineage survives the join, not just a single
fixed value at the table level.

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

## Constraint enforcement — real, registered UC constraints, declared at table-creation time

**Update:** an earlier version of this doc concluded that PK/FK constraints
could not be attached to Gold tables at all, because `ALTER TABLE ... ADD
CONSTRAINT` fails against every one of them:

```
[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'ALTER TABLE ... ADD CONSTRAINT'
expects a table but `...`.`dim_location` is a view.
```

That's still true, but it turned out to be a limitation of `ALTER TABLE`
specifically (a *post-hoc* DDL statement issued against an already-created,
view-registered object), not a limitation of Unity Catalog constraints on
materialized views in general. Lakeflow Declarative Pipelines supports
declaring `PRIMARY KEY`/`FOREIGN KEY` constraints **inline in the `schema=`
argument of `@dlt.table`**, at the moment the table is created by the
pipeline itself — a different code path that Unity Catalog does accept for
materialized views. Every `@dlt.table(...)` in
`src/pipelines/gold/dlt_gold_tables.py` now declares its full column schema
plus its constraints this way, e.g.:

```python
@dlt.table(
    name="dim_location",
    comment=_DIM_LOCATION_CFG["description"],
    schema="""
        location_key    INT     NOT NULL,
        location        INT,
        latitude        DOUBLE,
        longitude       DOUBLE,
        load_dt         TIMESTAMP,
        source_format   STRING,
        source_file     STRING,
        run_id          STRING,
        CONSTRAINT dim_location_pk PRIMARY KEY (location_key)
    """,
)
```

Confirmed live, post-full-refresh deploy, by querying
`information_schema.table_constraints` and `information_schema.key_column_usage`
in `vstone_traffic_dev`: every constraint below is really registered against
its table, with the correct column(s) --

| Table | Constraint | Type | Column(s) |
|---|---|---|---|
| `dim_date` | `dim_date_pk` | PRIMARY KEY | `date_key` |
| `dim_location` | `dim_location_pk` | PRIMARY KEY | `location_key` |
| `dim_street` | `dim_street_pk` | PRIMARY KEY | `street_key` |
| `fact_traffic_counts` | `fact_traffic_counts_location_fk` | FOREIGN KEY | `location_key` -> `dim_location.location_key` |
| `fact_traffic_counts` | `fact_traffic_counts_date_fk` | FOREIGN KEY | `date_key` -> `dim_date.date_key` |
| `fact_street_conditions` | `fact_street_conditions_street_fk` | FOREIGN KEY | `street_key` -> `dim_street.street_key` |
| `fact_street_conditions` | `fact_street_conditions_date_fk` | FOREIGN KEY | `date_key` -> `dim_date.date_key` |
| `gold_monthly_traffic_summary` | `gold_monthly_traffic_summary_location_fk` | FOREIGN KEY | `location_key` -> `dim_location.location_key` |
| `gold_street_risk_summary` | `gold_street_risk_summary_pk` | PRIMARY KEY | `street_id`, `year`, `month` (composite) |

`information_schema.tables.table_type` still reports `MATERIALIZED_VIEW` for
every one of these (confirmed unchanged) -- attaching a constraint this way
does not convert the table to a plain Delta table, and the materialized-view
design is fully preserved. Databricks still documents UC PK/FK as
**informational only** (not enforced like an RDBMS foreign key at write
time), so these constraints exist for catalog lineage/documentation/BI-tool
discovery purposes, not to reject bad writes.

Two intentional gaps in the table above, both load-bearing design decisions:

- **`gold_monthly_traffic_summary` has no `PRIMARY KEY`.** Its grain is
  `(location_key, year, month)`, but `location_key` is legitimately `NULL`
  for 10 of its 140 rows (the `location=7` orphan, see above) -- a primary
  key member can't be `NULL`, so no PK is declared. Its `FOREIGN KEY` on
  `location_key` is unaffected, since FK columns are allowed to be `NULL`.
- **`gold_street_risk_summary` has no `FOREIGN KEY` to `Dim_Street`.**
  `Dim_Street`'s primary key is the surrogate `street_key`, not `street_id`
  -- `street_id` repeats across SCD2 versions by design, so it isn't unique
  in `Dim_Street` and can't be a valid FK target there.

Two real column-type bugs were caught and fixed while writing these schema
strings, both against the actual Silver source schemas
(`src/config/silver_schemas.py`): `Dim_Street.long` (street length in
meters) is `INT`, not `DOUBLE` -- a first draft of this schema declared it
as `DOUBLE`, which would have mismatched `silver_streets.long`'s real
`IntegerType`. Same for `Fact_Traffic_Counts.id`, which is `INT`
(`silver_traffic.id` is `IntegerType`), not `STRING`. Verified post-deploy
via `dtypes` on the live tables.

All full-refreshed and re-verified after this change: row counts unchanged
and still exact (`fact_traffic_counts` 24,681,794, `fact_street_conditions`
87,776,721, `gold_monthly_traffic_summary` 140, `gold_street_risk_summary`
360).
