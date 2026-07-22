# `observation_type` and `observation_id` — design rationale

`fact_city_observations` unions three structurally different event types —
traffic counts, environmental sensor readings, and telegram messages — into
one physical table (a **heterogeneous fact table**, in dimensional-modeling
terms) instead of three separate fact tables. `observation_type` and
`observation_id` are the two columns that make that design actually work.
Both are declared `STRING`, not numeric — this doc explains what each does,
why string is the correct type (not an oversight), what breaks without
them, and what the numeric alternative would look like.

## What each column is

| Column | Role | Kimball term |
|---|---|---|
| `observation_type` | Tags each row with which of the 3 source domains it came from | Degenerate discriminator |
| `observation_id` | A synthetic, human-readable identifier built per row | Degenerate dimension |

Neither is a **measure** (a quantity to `SUM`/`AVG`) and neither is a
**foreign key** (a join to a dimension table). They're facts *about the
row itself* — the third category a fact table holds, alongside FKs and
measures.

## Real example rows

One real row from each Silver source, after being unioned in
`fact_city_observations`:

| Column | Row A — from `silver_traffic` | Row B — from `silver_environment` | Row C — from `silver_telegram` |
|---|---|---|---|
| `observation_type` | `'traffic'` | `'environmental'` | `'telegram'` |
| `street_key` | `NULL` | `12` | `NULL` |
| `location_key` | `3` | `NULL` | `NULL` |
| `enter` / `exit` | `12` / `8` | `NULL` / `NULL` | `NULL` / `NULL` |
| `noise` / `pollution` | `NULL` / `NULL` | `4.047` / `3.920` | `NULL` / `NULL` |
| `vehicle_plate_id` | `530` | `NULL` | `NULL` |
| `message_count` | `NULL` | `NULL` | `1` |
| `observation_id` | `'traffic_3_20240102'` | `'env_12_20230602'` | `'telegram_20230602_144053'` |

Source code, `src/pipelines/gold/fact_city_observations.py`:

```python
# Environmental branch (~line 157, 177)
F.lit("environmental").alias("observation_type")
F.concat(F.lit("env_"), F.col("env.street_id"), F.lit("_"), F.col("date.date_key")).alias("observation_id")

# Traffic branch (~line 239, 275)
F.lit("traffic").alias("observation_type")
F.concat(F.lit("traffic_"), F.col("traffic.location"), F.lit("_"), F.col("date.date_key")).alias("observation_id")

# Telegram branch (~line 317, 340)
F.lit("telegram").alias("observation_type")
F.concat(F.lit("telegram_"), F.col("date.date_key"), F.lit("_"), F.col("time_key")).alias("observation_id")
```

## Why `STRING`, not numeric

Apply the same test used throughout this project's dimensional model
analysis: **can you meaningfully aggregate it?**

- `SUM(observation_type)` / `AVG(observation_type)` — meaningless. It
  answers "what kind of row is this," not "how much happened."
- `SUM(observation_id)` / `AVG(observation_id)` — meaningless. It answers
  "which specific row is this," the same way an invoice number or order
  confirmation code would.

Neither column measures an event — they classify and identify it. That's
exactly the definition of a **degenerate dimension**: an attribute kept
directly on the fact table because it has no other descriptive attributes
that would justify its own dimension table.

## Impact if these columns didn't exist — concrete, not hypothetical

Checked directly against this codebase, not a guess:

1. **The whole aggregate layer stops compiling.** All 4 tables under
   `src/pipelines/gold/agg_*.py` filter on `observation_type`, e.g.:
   ```python
   env_facts = fact_city_observations_df.filter(F.col("observation_type") == "environmental")
   ```
   Remove the column and this throws `UNRESOLVED_COLUMN` — a hard failure,
   not degraded behavior.

2. **We'd lose the single most effective clustering key** — not a hard
   failure, a real performance cost. `dlt_gold_tables.py` clusters the
   table on this column:
   ```python
   cluster_by=["observation_type", "date_key"]
   ```
   To be precise: `CLUSTER BY` is flexible (Databricks allows any 1-4
   columns), so deleting `observation_type` wouldn't break the table build
   — we'd just re-cluster on something else, e.g. `date_key` alone. The
   real argument is that `observation_type` is the single most common
   filter predicate in this project (every `agg_*` table's first line is
   `WHERE observation_type = '...'`), and Liquid Clustering's entire value
   is data-skipping on columns queries actually filter by — so *given* the
   column exists, clustering on it is clearly the right choice. That's a
   "why it's a good clustering key" argument, not a "table can't be built
   without it" one.

3. **You'd be forced to infer type from column emptiness** (e.g.
   `WHERE enter IS NOT NULL` as a proxy for "traffic row"). This project has
   a real, live example of why that's an unsafe habit: `location_key` is
   `NULL` for 2,373,327 real traffic rows (the `location=7` orphan), for a
   reason that has nothing to do with row type. Inferring "is this traffic"
   from "is some other column populated" already breaks once in this
   dataset; there's no reason to trust it elsewhere.

   To be precise about what actually happened at Silver, since this is
   easy to misread: only the **`location=7` dimension row** (its GPS
   coordinates, `latitude=longitude=0`, in `src/pipelines/silver/locations.py`)
   was quarantined into `silver_locations_rejected` — that's a 1-row table.
   The 2.37M **traffic count readings** recorded at location 7, in
   `silver_traffic`, were never rejected; they're real, valid vehicle
   counts. We keep them (with a `NULL` FK, since there's no valid
   `dim_location` row to point at) rather than deleting real measured data
   just because that location's coordinate metadata was bad — dropping them
   too would silently undercount total city traffic volume by ~10%.

### Could we actually remove both columns? — checked live, not assumed

Short answer: **yes, technically, with zero data-correctness risk today.**
`observation_type` could be inferred from measure-nullness instead of an
explicit column, *if* every branch's measures are reliably non-null when
that branch's rows are populated. Checked each branch against real data
rather than assuming:

| Branch | Safe to infer from measure-nullness? | Evidence |
|---|---|---|
| `telegram` | Yes, by construction | `message_count` is `F.lit(1)`, a hardcoded literal — never null, structurally |
| `traffic` | Yes, confirmed | Original dimensional-model profiling: zero nulls in `silver_traffic`'s key columns, including `enter`/`exit` |
| `environmental` | Yes, confirmed live | `ENVIRONMENT_QUARANTINE_RULES` only rejects negative values, not nulls — so this needed checking, not assuming. Queried live via `databricks api post /api/2.0/sql/statements` against `silver_environment`: **87,776,721 total rows, 0 nulls in `noise`, `pollution`, `light`, or `raining` — individually or all four at once.** |

So `observation_id` is genuinely optional (nothing downstream consumes it
at all), and `observation_type` *could* be removed without data risk —
though not for free: it would mean rewriting the `observation_type ==`
filters in all 4 `agg_*.py` files and the `cluster_by` line, trading one
explicit, self-documenting column for an implicit assumption ("no branch's
measures are ever entirely null") that's verified true *today* but isn't a
schema-enforced guarantee — a future data source change could silently
break it without any signal. That's a real, narrow trade-off to weigh, not
a case of the columns being technically necessary.

## Alternative considered: `INT` via a lookup dimension

A numeric alternative is real and already has a precedent in this exact
codebase: `dim_technique.technique_key INT` replaced a raw
`source_technique STRING` for the same normalization reason. The same
pattern could apply here — a `dim_observation_type` lookup
(`1=environmental, 2=traffic, 3=telegram`) with `observation_type_key INT`
on the fact table instead.

| | `STRING` (current choice) | `INT` via lookup dimension |
|---|---|---|
| Storage cost | Negligible — Parquet dictionary-encodes a 3-value string column almost for free | Marginally smaller; difference is noise at this scale |
| Query speed | Photon compares dictionary-encoded low-cardinality strings about as fast as an int | Technically fastest, not meaningfully faster here |
| Readability | `observation_type = 'traffic'` is self-explanatory in any ad-hoc query, log, or Genie answer | `observation_type_key = 2` needs a join or memorized mapping |
| Complexity | Zero extra objects | One more dimension + FK, for a value that will essentially never grow |

**Decision: kept as `STRING`.** For a 3-value, effectively-static
discriminator, the join/dimension overhead isn't worth it — and since this
Gold layer's purpose is BI dashboards and Genie natural-language queries, a
self-describing value (`'traffic'`) is worth more here than the small,
mostly theoretical efficiency gain of an int code. If `observation_type`
ever needed more attributes of its own (a description, an icon, an owning
team), promoting it to a real dimension — the same move already made for
`dim_technique` — would become the right call.

`observation_id` could similarly become a `BIGINT` via `xxhash64(...)`
instead of a concatenated string, but that would defeat its actual purpose:
being human-readable for debugging. `'traffic_3_20240102'` tells you what
happened at a glance; a hash doesn't.

## Why not a Databricks Unity Catalog tag instead?

Unity Catalog tags are **object-level metadata** — a key-value label
attached to a whole catalog, schema, table, or column (e.g. `pii=true`,
`domain=finance`) for governance and discovery. They don't vary per row —
there's no mechanism to tag one row of `fact_city_observations`
differently from another. `observation_type` solves a row-level
classification problem, which is a data problem, not a metadata/governance
problem, so it has to live in the table as an actual column.

## Independent validation: a colleague's redesign proposal agrees

A separate redesign notebook (`08_gold_layer_star_schema_redesign.py`,
written against an older copy of this project — it migrates *from*
`fact_street_conditions`/`fact_traffic_counts`, the two-fact-table design
this project already moved past) arrives at the same core pattern
independently: a unified `fact_city_observations`, an `observation_type`
discriminator, and `observation_id` explicitly labeled a "degenerate
dimension kept in fact." Two people reaching the same design independently
is a good signal it's the right call — worth raising in conversation.

That said, **this project's current implementation should be kept as-is,
not replaced by that proposal**, for three concrete reasons found in it:

1. **It drops `telegram` entirely** — its `fact_city_observations` only
   handles `'environmental'`/`'traffic'` (see its table comment and the
   fact that it only has 2 `INSERT` branches). This project's fact table
   unifies all 3 domains, including `message_count`. Adopting it as written
   would be a regression.
2. **It combines `PARTITIONED BY (date_key)` with `CLUSTER BY (...)`** on
   the same table — two largely incompatible optimization strategies.
   Relevant to flag now, since Liquid Clustering vs. partitioning is the
   project's next work item.
3. **It uses `ALTER TABLE ... ADD CONSTRAINT`** for PK/FK, which this
   project already confirmed, live, fails against DLT materialized views
   (Unity Catalog registers them as `VIEW` objects) — constraints have to
   be declared inline in `schema=` at creation time instead (see
   `docs/gold_data_model.md`'s "Constraint enforcement" section). The
   proposal is written as plain imperative SQL (`CREATE TABLE`/
   `INSERT INTO`), not a DLT pipeline — a different execution model from
   what's actually deployed here.

## References

**Official vendor documentation (Databricks / Microsoft):**
- [Implementing a Dimensional Data Warehouse with Databricks SQL: Part 1](https://www.databricks.com/blog/implementing-dimensional-data-warehouse-databricks-sql-part-1) — Databricks' own blog explicitly describes a fact-table identifier column as a degenerate dimension: *"unique identifiers for transactional records (or other descriptive attributes in a nearly one-to-one relationship with the fact records)"* — this is `observation_id`, in Databricks' own words
- [Understand star schema and the importance for Power BI](https://learn.microsoft.com/en-us/power-bi/guidance/star-schema) — official Microsoft Learn documentation; its "Degenerate dimensions" section uses an order-number-on-the-fact-table example structurally identical to `observation_id`, and its "Surrogate keys" section explains why Type 2 SCDs (`dim_street`) require one
- [Apply tags to Unity Catalog securable objects](https://docs.databricks.com/aws/en/database-objects/tags) — confirms tags are object-level metadata, not per-row, which is why a Unity Catalog tag can't substitute for `observation_type`
- [Delta Lake generated columns / IDENTITY columns](https://docs.databricks.com/aws/en/tables/features/generated-columns) — the numeric surrogate-key alternative evaluated (and rejected) for this project's dimension tables, same reasoning class as this doc's `INT` alternative section

**Dimensional modeling theory (Kimball Group — the origin of this terminology):**
- [Degenerate Dimensions](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/degenerate-dimension/) — the term for `observation_type` and `observation_id`
- [Design Tip #46: Another Look At Degenerate Dimensions](https://www.kimballgroup.com/2003/06/design-tip-46-another-look-at-degenerate-dimensions/)
- [Junk Dimension](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/junk-dimension/) — the pattern `dim_audit` uses, referenced above for contrast
- [Fact Tables](https://www.kimballgroup.com/2008/11/fact-tables/) — general fact table grain/design background
