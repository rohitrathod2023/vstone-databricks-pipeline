# Gold data model — dimensions, one unified fact, and PK/FK relationships

## Star schema overview

```
Dim_Date ──────────┐
Dim_Location ───────┤
Dim_Street ─────────┼──< Fact_City_Observations >── gold_daily_summary
Dim_Technique ──────┤                            └── gold_location_summary
Dim_Audit ──────────┘
```

One fact table, not several — `fact_city_observations` uses an
`observation_type` discriminator (`'environmental'` / `'traffic'` /
`'telegram'`) to carry all three measurement domains at one row-per-event
grain, instead of one fact table per domain. Each branch populates only its
own measures and leaves the others `NULL`.

## Tables and surrogate keys

| Table | Type | Surrogate key (PK) | Natural key | Notes |
|---|---|---|---|---|
| `dim_date` | Materialized view | `date_key` (int, `YYYYMMDD`) | `full_date` | Generated, no source file |
| `dim_location` | Materialized view | `location_key` (int) | `location` | 13 rows — `location=7` excluded (bad coordinates, rejected at Silver) |
| `stg_dim_street_scd2` | Streaming table | — (internal CDC plumbing, not a public dimension) | `street_id` | `AUTO CDC FROM SNAPSHOT` target |
| `dim_street` | Materialized view | `street_key` (int) | `street_id` + `__START_AT` | SCD2 — full version history, `is_current` flag, 36 streets |
| `dim_technique` | Materialized view | `technique_key` (int) | `technique_name` | Static, 4 rows — `autoloader`/`copyinto`/`dlt`/`pyspark`, the real techniques used anywhere in this pipeline (matches `pipelines.silver.traffic.SOURCE_TECHNIQUES` exactly) |
| `dim_audit` | Materialized view | `audit_key` (int) | `(load_dt, source_format, source_file, run_id)` | Junk dimension consolidating lineage metadata — 6 distinct combinations on the real dataset |
| `dim_time` | Materialized view | `time_key` (int, `HHMMSS`) | `(hour, minute, second)` | Generated, one row per second of day — 86,400 rows |
| `fact_city_observations` | Materialized view | — (grain is `observation_type` + the fact-side natural key) | — | 112,586,955 rows (87,776,721 environmental + 24,681,794 traffic + 128,440 telegram) |
| `gold_daily_summary` | Materialized view | `date_key` | — | 283 rows (one per real date, June 2023 – March 2024) |
| `gold_location_summary` | Materialized view | — (`location_key` nullable, see orphan below) | — | 14 rows (13 real locations + 1 `NULL` group) |

## Fact/dimension categorization

Facts hold foreign keys, additive measures, and degenerate identifiers.
Dimensions hold descriptive/classifying attributes — including numeric ones
(`dim_time.hour`, `dim_street.dangerous`) that are never aggregated, only
filtered/grouped on or resolved to the version active at a point in time.

- **`fact_city_observations`** measures: `noise`, `pollution`, `light`,
  `raining` (environmental); `enter`, `exit` (traffic); `message_count`,
  `message_length` (telegram). FKs: `street_key`, `location_key`, `date_key`,
  `time_key`, `technique_key`, `audit_key`. `observation_type` is a
  degenerate discriminator (a tiny 3-value tag, not normalized into its own
  dimension — a deliberate simplicity trade-off); `observation_id` is a
  degenerate dimension (identifier, no attributes of its own).
- **`dim_technique.technique_key`** replaces what used to be a raw
  `source_technique STRING` sitting directly in a fact table — `enter`/`exit`
  rows resolve their real, per-row-varying technique via a join on
  `traffic.source_technique == dim_technique.technique_name`; environmental
  and telegram rows resolve to the single `"copyinto"` technique (verified:
  both real Bronze sources behind them use `copy_into`), via a `crossJoin`
  against a one-row lookup rather than a per-row equi-join.
- **`dim_audit.audit_key`** replaces the 4 raw audit columns
  (`load_dt`/`source_format`/`source_file`/`run_id`) that used to sit
  directly on every fact/dimension row with a single FK.

## Audit column lineage — two patterns, by design

- **`dim_date`, `dim_location`, `dim_street`** carry the standard 4 audit
  columns, selected straight through from their Silver source (or, for
  `dim_date`, stamped fresh as `source_format="generated"` since there's no
  Bronze file behind a calendar).
- **`dim_technique` and `dim_time`** carry **no** audit columns at all —
  both are pure generated/static reference tables with nothing meaningful to
  stamp (no source file, no per-row lineage), one step further than
  `dim_date`'s "generated" exception.
- **`fact_city_observations`** carries lineage via `audit_key` (an FK into
  `dim_audit`), not raw columns — this is the one place lineage is
  normalized rather than duplicated per row.
- **`gold_daily_summary` / `gold_location_summary`** keep their own raw
  audit columns stamped fresh as `source_format="generated"` — both are
  `GROUP BY` aggregates over millions of rows, so there's no single row's
  lineage to carry through, same reasoning as any aggregate table in this
  project.

## Foreign keys (intended relationships)

| From | Column | To | Column |
|---|---|---|---|
| `fact_city_observations` | `street_key` | `dim_street` | `street_key` |
| `fact_city_observations` | `location_key` | `dim_location` | `location_key` |
| `fact_city_observations` | `date_key` | `dim_date` | `date_key` |
| `fact_city_observations` | `time_key` | `dim_time` | `time_key` |
| `fact_city_observations` | `technique_key` | `dim_technique` | `technique_key` |
| `fact_city_observations` | `audit_key` | `dim_audit` | `audit_key` |
| `gold_daily_summary` | `date_key` | `dim_date` | `date_key` |
| `gold_location_summary` | `location_key` | `dim_location` | `location_key` |

`date_key`/`time_key`/`technique_key`/`audit_key` are declared `NOT NULL` on
`fact_city_observations` and verified live at **0 NULLs** across all three
observation types, post-full-refresh.

## Known, expected orphan: `location_key`

Traffic rows for `location=7` (2,373,327 of 24,681,794 traffic rows) have a
`NULL` `location_key` — `location=7`'s coordinates were quarantined at
Silver (`silver_locations_rejected`), so it's correctly excluded from
`dim_location`, but its sensor readings still exist as a real, separate
domain. `gold_location_summary` surfaces this as a `NULL`-keyed 14th group
(13 real locations + this orphan) rather than silently dropping it —
verified live.

## Business aggregates

- **`gold_daily_summary`** — one row per date. `total_vehicles_entered`,
  `total_vehicles_exited`, `net_traffic_flow` (traffic rows only);
  `avg_noise`, `avg_pollution`, `avg_light`, `avg_raining` (environmental
  rows only); `telegram_message_count` (telegram rows only);
  `total_observations` (all three combined). Each measure is a conditional
  `SUM`/`AVG` scoped to its own `observation_type` via `F.when(...)` inside
  a single `GROUP BY date_key` — no joins needed, since the unified fact
  already puts all three domains on the same table. Verified live: 283
  rows, 0 NULLs across every measure, real date range 2023-06-02 to
  2024-03-10.
- **`gold_location_summary`** — one row per location, filtered to
  `observation_type == 'traffic'` first. `total_vehicles_entered`,
  `total_vehicles_exited`, `total_traffic_volume`, `total_traffic_readings`.
  Answers "busiest intersections": verified live, `location_key=6` is the
  single busiest location (123,752,573 total volume), `location_key=3` is
  second (113,936,208) — consistent with this project's independently
  documented finding that location 6 dominates traffic volume.

Every Gold table (dimensions, fact, and both aggregates) lives in the one
`gold_dlt_pipeline` DLT pipeline, run via `gold_job`.

## Constraint enforcement — real, registered UC constraints

Lakeflow Declarative Pipelines accepts `PRIMARY KEY`/`FOREIGN KEY`
constraints declared inline in the `schema=` argument of `@dlt.table`, at
the moment the table is created — see `src/pipelines/gold/table_schemas.py`
for every table's full column schema plus constraints. Confirmed live via
`information_schema.table_constraints`/`key_column_usage`: every constraint
is really registered, and `information_schema.tables.table_type` still
reports `MATERIALIZED_VIEW` throughout. Databricks documents UC PK/FK as
**informational only** (not enforced like an RDBMS constraint at write
time) — these exist for catalog lineage/documentation/BI-tool discovery,
not to reject bad writes. Plain column-level `NOT NULL` (distinct from
PK/FK) **is** enforced at write time by Delta — confirmed live when
`dim_technique.technique_key`'s inferred `LongType` was rejected against
its declared `INT NOT NULL`.

`gold_location_summary` has no `PRIMARY KEY`: its grain is `location_key`,
but `location_key` is legitimately `NULL` for the orphan group above — a
`PRIMARY KEY` member can't be `NULL`. Its `FOREIGN KEY` is unaffected, since
FK columns are allowed to be `NULL`.

## Real bugs caught and fixed while building this model

- **Timestamp/date join mismatch**: joining `environment.date`/`traffic.date`
  (TIMESTAMP, real time-of-day) directly against `dim_date.full_date`
  (DATE) without `F.to_date()` silently resolved `date_key` to `NULL` for
  almost every real row. Fixed by casting to `DATE` before the join.
- **Hardcoded `technique_key`**: originally stamped as a constant for every
  traffic row, discarding the real per-row variation across all 4
  ingestion techniques. Fixed with a real join on `source_technique`.
- **`Int`/`Long` schema mismatch**: `spark.createDataFrame()` infers
  Python int literals as `LongType`, but `dim_technique.technique_key` is
  declared `INT` — Delta rejected the write (`DELTA_MERGE_INCOMPATIBLE_DATATYPE`)
  until the column was explicitly cast.
- **Eager `.collect()` breaking DLT flow resolution**: an early version of
  the technique lookup called `.collect()` to resolve a scalar, which forced
  execution during DLT's graph-analysis pass — before `dim_technique` (a
  sibling table in the *same* pipeline run) had actually been computed.
  Fixed by keeping the lookup fully lazy (a `crossJoin` against a one-row
  DataFrame, resolved at real execution time in the correct dependency
  order) rather than collecting a Python scalar.
- **Unqualified column after `withColumn()` on an aliased DataFrame**:
  `telegram_df.alias("telegram").withColumn("time_key", ...)` — the new
  `time_key` column doesn't carry the `"telegram"` alias, so
  `F.col("telegram.time_key")` failed to resolve. Fixed by referencing the
  column unqualified.

All verified full-refreshed and re-verified live in `vstone_traffic_dev`.
