# Dataset Profiling — `xxjcaxx/trafficsimulator` (Kaggle)

Simulated traffic dataset for a real neighborhood in Valencia, Spain,
spanning ~9 months (2023-06-02 to 2024-03-11).

## Files, sizes, and columns

| File | Rows | Size | Columns | Grain |
|---|---|---|---|---|
| `cars.csv` | 24,681,794 | 900 MB | `enter` (int), `exit` (int), `date` (timestamp, ~10s intervals), `id` (int, 999 distinct), `location` (int, 14 distinct) | One reading per intersection per ~10 sec |
| `streets.csv` | 87,820,725 | 7.8 GB | `noise` (float), `pollution` (float), `date` (timestamp), `light` (float), `raining` (float), `street_id` (int, 36 distinct) | One reading per street per ~10 sec |
| `node_locations.csv` | 14 | tiny | `latitude`, `longitude`, `location` (PK) | One row per intersection |
| `streets_list.csv` | 36 | tiny | `street` (name), `long` (length), `latitude`, `longitude`, `dangerous` (float 0-0.8), `street_id` (PK) | One row per street |
| `telegram.csv` | 128,440 | small | `message` (free text), `date` (text, inconsistent format), `hour` (text) | Free-text citizen/incident reports |

## Confirmed entity behavior

`cars.id` is a real recurring entity, not a rotating counter — e.g. id=530
appears 39,044 times across the full 9-month span. Confirms this represents
a real (or realistically simulated) recurring vehicle/sensor, not a
per-reading throwaway ID.

## Confirmed relationships

- `cars.location` ↔ `node_locations.location` — 100% overlap, 14/14.
- `streets.street_id` ↔ `streets_list.street_id` — expected 36/36 overlap
  (pattern confirmed via sampling; full re-verification pending a
  memory-safe DuckDB query that avoids loading `light`/`raining` in full).
- `telegram.csv` — no key-based relationship to any other table, only a
  loose date/time association.

## Data quality issues found

- `node_locations.csv` row `location=7` has invalid coordinates `(0,0)`.
  Flagged for quarantine in Silver, not silently corrected.
- `telegram.csv` dates are inconsistently formatted and irregularly spaced
  (min gap 0, max gap 29 days) — requires explicit parsing/normalization in
  `silver_telegram`, not a straight cast.

## Open items (not yet fully verified)

- Re-confirm `streets.street_id` ↔ `streets_list.street_id` overlap
  explicitly via a memory-safe DuckDB query.
- Optionally confirm whether a given `cars.id` appears across multiple
  `location`s over time (would confirm a moving vehicle rather than a
  per-location fixed sensor) — informs whether `id` should be modeled as a
  degenerate dimension only, or eventually its own `Dim_Vehicle`.
