# Day 7: incremental load simulation + MERGE INTO evidence

Two independent pieces, both on `feature/gold-layer` (same branch as the
Liquid Clustering benchmark). Neither touches `Fact_Street_Conditions`
directly -- it's a DLT materialized view, a recomputed reflection of
`silver_environment`, not an independently writable table. Both pieces
apply their change at the real source and let Gold pick it up on its next
refresh.

## Piece 1: `MERGE INTO` on `silver_environment`

Script: `src/notebooks/07_silver_environment_merge_demo.py`.

**Real finding along the way:** `DeltaTable.merge()` (the Python Delta API)
failed against `silver_environment` with `[DELTA_MISSING_DELTA_TABLE]` --
`DeltaTable.forName()` doesn't recognize this DLT-managed table as a Delta
table, even though it's genuinely Delta-backed underneath. Switched to SQL
`MERGE INTO` instead (resolves through the normal SQL catalog path), which
worked immediately -- same as the ACID demo's SQL-based UPDATE/DELETE/MERGE
against `silver_locations`, another DLT-managed table.

### Before / after

| | Row count |
|---|---|
| Before | 87,776,721 (already-verified `silver_environment` count) |
| After | 87,776,722 (`before + 1`, exactly one new row) |

The corrected row (`street_id=1`, `date=2023-08-15 00:00:03.093`):

| Column | Before | After |
|---|---|---|
| `noise` | 0.0 | 15.5 |
| `pollution` | 0.0 | 8.2 |
| `light` | 0.5295811217256587 | 45.0 |
| `raining` | 0.1502148020932554 | 2.3 |

Confirmed no duplicate (exactly 1 row for this key after the merge), and
the new reading (`street_id=1`, `date=2024-03-11 08:00:00`, one day past
the last previously-observed date for this street) exists exactly once.

### Gold picked it up with a normal run -- no full-refresh needed

Ran `databricks bundle run gold_job -t dev` (normal, not `--full-refresh`)
after the merge. `Fact_Street_Conditions` is a materialized view, so it
fully recomputes from current source data on every refresh regardless --
confirmed live:

- `Fact_Street_Conditions` row count: 87,776,722 (matches `silver_environment`)
- The corrected values (`noise=15.5, pollution=8.2, light=45.0, raining=2.3`)
  appear exactly once for `street_key=1` on `date_key=20230815`
- The new reading appears exactly once for `street_key=1` on `date_key=20240311`
- The pre-correction values (`noise=0.0, pollution=0.0, light=0.5295811217256587,
  raining=0.1502148020932554`) are completely gone -- 0 rows found

## Piece 2: exercising `Dim_Street`'s SCD2 mechanism

Script: `src/notebooks/08_dim_street_scd2_demo.py`. No `MERGE INTO` here --
the SCD2 versioning is entirely automatic once `silver_streets` (the table
`_dim_street_snapshot` reads from) changes and the Gold pipeline refreshes.

`street_id=7` ("PalasietA") was picked after checking `Dim_Street`'s real
current data: an ordinary `dangerous=0.3`, not an edge case, not already
near the 0.5 threshold. Changed to `0.7`.

### A normal run was sufficient -- confirmed, not assumed

Ran `databricks bundle run gold_job -t dev` (normal run) after the source
change. `AUTO CDC FROM SNAPSHOT` picked up the new snapshot and created a
new SCD2 version without any full-refresh:

- `stg_dim_street_scd2` row count: 37 (36 streets, one with 2 versions)
- Old version: `street_key=7`, `dangerous=0.3`, `__START_AT=2026-07-19
  00:40:26.764`, `__END_AT=2026-07-19 12:59:49.119`, `is_current=false`
- New version: `street_key=8`, `dangerous=0.7`, `__START_AT=2026-07-19
  12:59:49.119`, `__END_AT=NULL`, `is_current=true`
- `Dim_Street` row count: 37, `street_key` values 1-37, confirmed
  contiguous with no gaps or duplicates

**Real, notable side effect:** adding a new SCD2 version shifted every
subsequently-ordered street's `street_key` by one (`street_key` is
`row_number() OVER (ORDER BY street_id, __START_AT)`, and the new version
sorts in the middle of the sequence, not the end). E.g. `street_id=8`'s
surrogate key changed from 8 to 9, and so on through `street_id=36`
(36 -> 37). This is safe within this pipeline -- every Gold table fully
recomputes together on each refresh, so nothing downstream is left
pointing at a stale key -- but it would be a real problem for any external
system caching `street_key` values between refreshes.

### A real, structural finding: the backfill clamp needed a companion fact

The first refresh after the SCD2 change showed `risk_changed_flag=false`
for every one of `gold_street_risk_summary`'s existing rows, including
`street_id=7`. Investigated rather than assumed:

- `Fact_Street_Conditions` rows resolved to the new version
  (`street_key=8`, `dangerous=0.7`): **0**
- `Fact_Street_Conditions` rows resolved to the old version
  (`street_key=7`, `dangerous=0.3`): 2,438,240 (unchanged)

Root cause: `AUTO CDC FROM SNAPSHOT` always stamps a new version's
`__START_AT` with wall-clock processing time (`2026-07-19`), never a
backdated business-effective date. Every real `silver_environment` fact is
dated 2023-2024 -- years before *any* SCD2 version's `__START_AT` exists.
The backfill-clamp logic in `fact_street_conditions.py` (built earlier,
specifically for this reason) correctly routes all such historical facts to
the *earliest* known version. Once a second version exists, that logic
doesn't change -- it's still correct -- but it means **no new SCD2 version
can ever receive real historical fact data**, no matter how many streets
change, because `__START_AT` is always "now" and "now" is always after
every real fact date in this dataset.

This was not a bug requiring a join-logic redesign. The range-join already
handles a fact dated *after* `__START_AT` correctly via its normal
condition -- there simply wasn't one. The fix: add one companion
`silver_environment` reading for `street_id=7`, dated after the SCD2
change's `__START_AT`. `Dim_Date` recomputes its min/max date range from
live data on every refresh, so it naturally expanded to cover the new
date once the reading existed (289 -> 1,150 rows, range
2023-05-30 to 2026-07-22) -- no separate fix needed there either.

### Final result

After adding the companion reading (`street_id=7`, `date=2026-07-19
14:29:10`) and refreshing Gold again:

- That reading resolves to `street_key=8` (the **new**, `dangerous=0.7`
  version) via the normal range condition -- exactly 1 row
- All 2,438,240 historical readings still resolve to `street_key=7` (the
  old version) via the backfill clamp -- unchanged
- `gold_street_risk_summary` gained exactly one new row, and it is the
  **only** row in the entire 361-row table with `risk_changed_flag=true`:

| street_id | year | month | dangerous_rating_prior_month | dangerous_rating_this_month | risk_changed_flag | risk_direction |
|---|---|---|---|---|---|---|
| 7 | 2026 | 7 | 0.3 | 0.7 | true | increased |

Every other row for `street_id=7` (2023-06 through 2024-03) still shows
`dangerous_rating_this_month=0.3`, `risk_changed_flag=false`,
`risk_direction="stable"` -- correct, since none of that historical data
predates or postdates the change in a way that should show a difference.

## Why this is a one-time demo, not the production design

Both pieces here are deliberate, one-time, manually-triggered scripts
against Silver source tables. That's a scoped demo, not how this would
actually run in production. This project's Silver ingestion is
**append-only by design** -- Bronze lands new files, Silver reads them via
`spark.readStream`, and nothing currently re-checks or corrects rows
already landed. A real production system handling late-arriving
corrections and new readings continuously, rather than as a manual
one-off, would need one of:

- **`AUTO CDC` on a real change-data-feed source** -- if the upstream
  system producing `streets_list`/`street_conditions` could emit its own
  change events (inserts/updates/deletes), Silver could consume that feed
  directly the same way `stg_dim_street_scd2` already consumes
  `AUTO CDC FROM SNAPSHOT` for `Dim_Street`.
- **Structured Streaming `foreachBatch` running `MERGE INTO` on every
  micro-batch** -- the standard Databricks pattern for continuous
  upserts: each micro-batch of newly-arrived Bronze data gets merged into
  Silver (matching on the same natural key used here) instead of being
  blindly appended.

Neither was built here. Redesigning Silver's ingestion shape from
append-only to continuously-upserting is a real architectural change
touching every Silver table's DLT pipeline definition, not a one-time
demo script -- not justified this close to the project deadline, and out
of scope for what Day 7 asked for (demonstrating the mechanisms exist and
work, not rebuilding the pipeline around them). This is scoped,
intentional debt, not an oversight.

## Verification summary

- Piece 1: real before/after row counts (87,776,721 -> 87,776,722, exactly
  `+1`), corrected row's exact new values confirmed, no duplicates,
  propagated to Gold via a normal (non-full-refresh) run.
- Piece 2: real version count (37), `risk_changed_flag=true` /
  `risk_direction="increased"` confirmed for the specific
  `street_id=7, year=2026, month=7` row -- the only such row in the table.
- `flake8` clean on both new notebooks.
- Full local `pytest` suite passing (no regressions from touching
  `silver_environment`/`silver_streets`).
