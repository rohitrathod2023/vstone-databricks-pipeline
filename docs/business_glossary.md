# Business Glossary — VStone Traffic Analytics

Plain-language definitions for anyone using the Gold layer tables,
dashboards, or the Genie agent — no engineering background assumed. For the
technical schema (column types, keys, joins), see `docs/gold_data_model.md`
and the ER diagram (`docs/gold_layer_dimensional_model.png`) instead; this
document is about what the data *means*, not how it's built.

## Core entities

| Term | Plain-language meaning |
|---|---|
| **Street** | A monitored street segment in the city, each with a fixed length, GPS location, and a danger rating (see below). 36 streets are tracked. Streets can be renamed or have their danger rating re-assessed over time — this system keeps a full history of those changes, so a report about "last year's noise on Main Street" still uses the name Main Street had back then, even if it's since been renamed. |
| **Location (intersection)** | A fixed traffic monitoring point — an intersection or road node where vehicle counts are recorded. 13 locations are tracked. Unlike streets, locations don't change over time. |
| **Date** | A calendar day. Trends can be viewed by day, month, quarter, or day-of-week (e.g., "are weekends busier?"). |
| **Time of day** | The specific second a reading was recorded, down to hour/minute/second — supports questions like "is traffic worse during rush hour?" |
| **Ingestion technique** | Which method loaded a given piece of data into the system (there are 4: Auto Loader, COPY INTO, Delta Live Tables, PySpark). This is an internal data-engineering detail, included for troubleshooting data-quality questions ("is data loaded one way less reliable than another?"), not a business question about the city itself. |
| **Data load record** | Internal record of when, from what file, and from which pipeline run a piece of data arrived — used to trace any number back to its original source file if its accuracy is ever questioned. |
| **Observation** | A single measurement event — one sensor reading, one traffic count, or one citizen-submitted report. Every row in the main data table is one observation. |

## The three kinds of observations

Every observation is one of exactly three kinds. A given row only has values filled in for its own kind — the others are blank.

| Kind | What it captures | Key numbers recorded |
|---|---|---|
| **Environmental reading** | A sensor reading on a street — noise, pollution, light level, and rain | `noise`, `pollution`, `light`, `raining` |
| **Traffic count** | Vehicles entering/exiting at an intersection | `enter`, `exit`, and the anonymized car ID |
| **Citizen report** | A social-media-style message submitted by a resident about an incident | Just a count — the message text itself isn't structured data |

## Measures (the numbers you can total or average)

| Measure | Meaning | Business-relevant notes |
|---|---|---|
| **Noise** | Ambient noise level measured on a street | Higher = louder. No fixed upper bound in the data. |
| **Pollution** | Air pollution level measured on a street | Higher = more polluted. |
| **Light** | Ambient light level, 0–100 scale | Affected by both time of day and rain. |
| **Raining** | Rain intensity as a percentage, 0–100 | **Important:** a value of **-1 means "not raining,"** not a real negative reading — this is a known placeholder in the source data, already handled correctly in all official reports (`rain_intensity_sum` and similar figures exclude -1 automatically). If you ever query the raw number yourself, treat -1 as "no rain," not as data. |
| **Vehicles entered / exited** | Count of vehicles passing a monitoring point in each direction | Summed to get total traffic volume; entered minus exited gives net flow (are more cars arriving than leaving?). |
| **Message count** | Number of citizen incident reports | Always counted as 1 per report — message length or content isn't scored. |
| **Danger rating** | A 0.1–0.9 risk score assigned to each street | Higher = more dangerous. Can be re-assessed over time (e.g., after a safety improvement); history of past ratings is preserved. |

## Known data-quality notes, in plain terms

- **"Not raining" shows as -1, not 0 or blank.** Already excluded correctly in every official report — flagged here only so a -1 in a raw export isn't mistaken for a real reading.
- **One traffic monitoring location (location 7) has unusable GPS coordinates** and was excluded from the location list, but its actual vehicle-count readings are still real and included in traffic totals — they just can't be mapped to a specific spot on a map. Roughly 2.4 million of the ~24.7 million traffic readings fall into this "location unknown but real" bucket.
- **Every street has complete data for the entire observation period** (June 2023 – March 2024) — no street has ever gone quiet or missed reporting, confirmed by direct check. (A "has this street gone silent?" monitoring check exists and is ready to flag it the moment any street ever does.)

## Reporting tables — what each one answers

| Table | Answers | Grain |
|---|---|---|
| **Daily street conditions** | "How did noise/pollution/rain trend on this street, day by day?" | One row per street, per day |
| **Daily location traffic** | "How did traffic volume trend at this intersection, day by day?" | One row per location, per day |
| **Monthly street summary** | "What's the long-term/seasonal trend for this street?" | One row per street, per month |
| **Hourly incident activity** | "What time of day do most citizen reports come in?" | One row per date, per hour |

All four feed the Genie agent, so these questions (and variations on them) can be asked directly in plain English rather than written as queries.

## Terms you might see used loosely, defined precisely

- **"Churned" street** — a street that has gone quiet (no new readings) for 30+ days. Currently, no street meets this definition — every street has reported continuously through the entire observation period. This is monitored via a dedicated check, not a live dashboard table.
- **"Current" version of a street** — since street names and danger ratings can change over time, "current" always means the most recent version as of today, unless a report specifically asks about a past date.
- **"Busiest" location/street** — ranked by total traffic volume (entered + exited) unless otherwise specified.
