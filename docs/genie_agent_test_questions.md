# Genie Agent — setup + test questions (Day 6: "Genie space created, using the aggregate table as source")

**Note:** this doc previously referenced `gold_monthly_traffic_summary` and
`gold_street_risk_summary` — tables from an earlier design iteration that no
longer exist in this codebase. Rewritten against the real, currently
deployed aggregate tables, with every "expected answer" queried live against
`vstone_traffic_dev.dev_rohitrathodcomp_gold` (not assumed).

## Setup steps (Databricks workspace UI)

1. Catalog → **Genie** → **New Agent** (renamed from "Genie space" in Databricks' recent
   Genie One / Genie Agents update — same underlying capability the Day 6 checklist asks for).
2. Register all 4 aggregate tables as source datasets:
   - `agg_daily_street_conditions` — daily noise/pollution/light/rain per street
   - `agg_daily_location_traffic` — daily traffic volume per location
   - `agg_monthly_street_summary` — monthly environmental trend per street (long-term/seasonal view)
   - `agg_hourly_telegram_activity` — hourly incident-report volume
3. Add a short instruction/description for the agent, e.g. "Answers questions about
   traffic volume, street environmental conditions, and citizen incident reports for the
   VStone traffic simulation project, using daily/monthly/hourly aggregate data." Genie
   uses this plus column comments to ground its answers — the column `comment=` values
   already added during the PK/FK work should help here.
4. Save, then move to testing below.

## Test questions — run each, record the real answer, compare against the known-correct result

| # | Question to ask | Source table | Expected real answer (queried live) | Genie's actual answer | Match? |
|---|---|---|---|---|---|
| 1 | "Which location had the highest total traffic volume overall?" | `agg_daily_location_traffic` | `location_key=6`, total volume **123,752,573** (summed across all 283 days); `location_key=3` second at **113,936,208** | | |
| 2 | "Which street has the highest average noise level?" | `agg_monthly_street_summary` | `street_id=35` ("N-340B") — consistently highest across months, e.g. **13.11** avg noise in June 2023 | | |
| 3 | "How many months of monthly street data do we have, and for how many streets?" | `agg_monthly_street_summary` | **360 rows** = 36 streets × 10 months each | | |
| 4 | "What hour of the day gets the most telegram incident reports?" | `agg_hourly_telegram_activity` | Fairly even distribution across all 24 hours — hour 22 is slightly highest at **8,191** messages, but hours 6 and 2 are nearly tied (8,167 / 8,165). Good test of whether Genie overstates a "peak hour" that isn't actually dramatic in the real data. | | |
| 5 | "Which street had the highest rain intensity on a single day, and when?" | `agg_daily_street_conditions` | `street_id=31` ("Academic Maravall") on **2024-02-04**, `rain_intensity_sum=323905.45` — the top 3 streets that day are all from the same date, suggesting a real city-wide rain event worth cross-checking against `agg_daily_street_conditions` for other streets on 2024-02-04 too | | |
| 6 | "What's the average pollution for street 12 last month in the data?" (swap in a real recent month from `agg_monthly_street_summary`) | `agg_monthly_street_summary` | Should return a single numeric value matching that row's `avg_pollution` — verify against a direct `SELECT avg_pollution FROM agg_monthly_street_summary WHERE street_id=12 AND year=... AND month=...` | | |

## After testing

- Fill in the "Genie's actual answer" and "Match?" columns with real results — this table
  becomes part of the Day 10 demo material.
- Take screenshots or a short recording of a couple of these exchanges for the Day 10 walkthrough.
- Report back here once done — if any answer doesn't match the known-correct result, that's worth
  flagging and digging into (could be a Genie grounding issue, e.g. missing column descriptions,
  rather than a data problem) rather than silently accepting a wrong answer.
- Question #4 is deliberately chosen to test whether Genie invents a more dramatic "peak hour"
  story than the data actually supports — a good check on whether it's grounding answers in the
  real numbers or pattern-matching to what a plausible-sounding answer "should" look like.
