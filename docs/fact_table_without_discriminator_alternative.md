# Alternative Gold fact table design: no `observation_type`/`observation_id`

**Status: experimental, pending trainer review — not merged to `dev`.**
Branch: `experiment/fact-table-without-discriminator-columns`. This document
exists to present both options to a trainer/reviewer for a decision; it is
not a recommendation to adopt.

## What changed on this branch

`fact_city_observations` currently (on `dev`) carries two extra columns
that this branch removes entirely:

| Column | Type | Role on `dev` |
|---|---|---|
| `observation_type` | `STRING` | Discriminator tagging each row `'environmental'` / `'traffic'` / `'telegram'` |
| `observation_id` | `STRING` | Synthetic per-row identifier (e.g. `'traffic_3_20240102'`) |

See `docs/observation_type_and_observation_id.md` for the full rationale
behind the `dev`-branch design (why they're `STRING`, Kimball/Databricks/
Microsoft sources, etc.). This document is the other side of that
conversation: what happens if you remove them, and whether that's actually
a good idea.

## Why this alternative is technically valid, not just "different"

Removing `observation_type` requires every consumer that used to write
`WHERE observation_type = 'traffic'` to instead infer branch membership
from which measures are populated. That's only safe if each branch's own
measures are reliably non-null when that branch's rows exist. This was
checked against real data, branch by branch, not assumed:

| Branch | Inference used instead | Why it's safe |
|---|---|---|
| `telegram` | `message_count IS NOT NULL` | Safe **by construction** — `fact_city_observations.py`'s telegram branch always stamps `message_count` as `F.lit(1)`, never conditionally null |
| `traffic` | `enter IS NOT NULL` | Confirmed by the original dimensional-model profiling: zero nulls in `silver_traffic`'s key columns, including `enter`/`exit` |
| `environmental` | `noise IS NOT NULL` | Confirmed **live**, this session: queried `silver_environment` via `databricks api post /api/2.0/sql/statements` — **87,776,721 total rows, 0 nulls** across `noise`/`pollution`/`light`/`raining`, individually and all four at once |

All 32 Gold-layer unit tests pass on this branch (`fact_city_observations`,
all 4 `agg_*` tables, `dim_street`), confirming the inference approach
produces identical results to the `dev`-branch discriminator approach on
every existing test case.

## Side-by-side comparison

| | `dev` (current, with discriminator) | This branch (without) |
|---|---|---|
| `fact_city_observations` columns | +2 (`observation_type`, `observation_id`) | 2 fewer columns |
| Filtering a branch | `WHERE observation_type = 'traffic'` — explicit, self-documenting | `WHERE enter IS NOT NULL` — requires knowing which measure to check per branch |
| `CLUSTER BY` | `["observation_type", "date_key"]` | `["date_key"]` only |
| Ad-hoc/Genie queries against the raw fact table | Read `observation_type` off the schema directly | Must already know the inference convention — not discoverable from the schema alone |
| Storage/query performance | Negligible difference — a 3-value string column dictionary-encodes almost for free in Parquet | No measurable win |
| Risk if a future data source is added or a branch's measures ever go fully null | None — the tag is independent of the measures | Silent misclassification: a row could vanish from every `agg_*` table's aggregation without any error, if a new branch or edge case defeats the nullness assumption |
| Code footprint | N/A (already built) | 1 fact builder + `table_schemas.py` + `dlt_gold_tables.py` (`cluster_by`) + all 4 `agg_*.py` filters + 5 test files rewritten |

## Recommendation

Keep the `dev`-branch design (with `observation_type`/`observation_id`).
This alternative is **technically sound and fully tested** — it is not
broken or wrong — but it trades a one-time rewrite for an ongoing cost with
no offsetting benefit:

- **No performance or storage win** — already established the difference
  is negligible at this data volume.
- **A recurring tax on every future consumer** — anyone querying
  `fact_city_observations` directly (a new teammate, an ad-hoc analyst, a
  Genie natural-language question) has to already know "check
  `noise IS NOT NULL` to mean environmental" instead of reading
  `observation_type = 'environmental'` straight off the schema. That
  knowledge has to be independently rediscovered or documented every time,
  forever, versus once.
- **A latent correctness risk, not a proven one** — the nullness inference
  is verified true for *today's* data across all 3 branches, but it's an
  assumption about data shape, not a schema guarantee. A future sensor
  type, a partial-reading edge case, or a new data source could silently
  break it with no error — just rows quietly missing from an aggregate.

That said, this is a legitimate judgment call, not a right/wrong technical
question — both designs are correct, tested, and functional. Worth a
trainer's second opinion precisely because reasonable engineers could land
on either side.

## Files changed on this branch

- `src/pipelines/gold/fact_city_observations.py` — both columns removed
  from all 3 branch builders
- `src/pipelines/gold/table_schemas.py` — removed from
  `FACT_CITY_OBSERVATIONS_SCHEMA`
- `src/pipelines/gold/dlt_gold_tables.py` — `cluster_by` reduced to
  `["date_key"]`
- `src/pipelines/gold/agg_daily_street_conditions.py`,
  `agg_daily_location_traffic.py`, `agg_monthly_street_summary.py`,
  `agg_hourly_telegram_activity.py` — filters rewritten to measure-nullness
- `tests/unit/test_fact_city_observations.py` + all 4
  `tests/unit/test_agg_*.py` — rewritten and passing (32/32 Gold tests green)

`docs/gold_data_model.md` and `docs/gold_layer_dimensional_model.png` are
**not** updated on this branch — they still describe the `dev` design.
Regenerating them is deferred until/unless this alternative is actually
approved, to avoid throwaway work on a branch that may not be adopted.
