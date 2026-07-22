# VStone Databricks Capstone Retry — Project Context
**Purpose of this doc:** Full context for the VStone Databricks capstone retry so every future chat in this project has full context without re-explaining. Last updated: July 16, 2026 (Silver Phases 2 & 3 — header standardization, and dimension staging `silver_locations`/`silver_streets` + quarantine tables — BUILT AND VERIFIED by Rohit via local Claude Code. Exact expected counts confirmed: `silver_locations` 13/`silver_locations_rejected` 1, `silver_streets` 36/`silver_streets_rejected` 0. Moving to Phase 4: `silver_traffic`).

**⚠️ Companion doc:** `claude/vstone-day-by-day-checklist.md` is the authoritative per-day, per-branch compliance checklist, pulled verbatim from the brief PDF.

**⚠️ Sourcing note**: Rohit develops via his own local Claude Code in a folder NOT the same as this session's device-bridge-connected folder (stale/separate copy — don't use it to verify code state). Git/deploy status comes from Rohit's own status reports.

**⚠️ Naming convention rule**: no deployed DABs resource name should reference which day it was built on. Name by function.

**⚠️ MAJOR WORKING-STYLE PIVOT (July 15, applies from Silver layer onward — read this first in any new session)**: After Day 4 evaluation, Rohit reported fumbling trainer questions on parts of the project he hadn't personally understood before code was generated for it. Explicit decision: **from Silver onward, understand the concept first, confirm understanding, THEN generate code** — not the other way around. He will still use local Claude Code to actually write the repeatable code, but only for things he can already explain himself; this session's job is teaching + planning, phase by phase, not bulk code generation. Bronze is done/tested/deployed and will NOT be redone to fit this philosophy — it stays as-is; this pivot is for Silver, Gold, and everything after. Silver has been broken into 8 phases (Section 6B) — go one phase at a time, and don't move to the next phase until Rohit can explain the current one back unprompted.

**⚠️ Working method, now explicit and standing**: for open design questions, check (1) official Databricks docs, (2) our own project brief/day-by-day checklist directly (not a summary of it), and (3) broader vendor-neutral industry practice where relevant — then synthesize an actual recommendation from all three, not defer to any single source as automatically final. Then, where possible, close the loop with a real query against the actual data rather than stopping at "the design is agreed" — Phase 1 of Silver (Section 6A/6B) is the model example of this whole method end to end. Also cite real Databricks frameworks by name when relevant and confirmed (e.g. the Well-Architected Framework's 7 pillars, used in Phase 2 to justify defensive/insurance design) rather than vague "best practice" language — but always verify the framework's actual content first rather than assume Rohit's recollection or a training-data guess is exactly right.

**⚠️ STANDING RULE (established July 13-14, must never be violated again): never generate any file/prompt/deliverable without Rohit's explicit permission.** Always discuss the plan/approach first, reach explicit agreement, THEN generate. This rule exists because skipping discussion cost real rework on Bronze (materialized-view→streaming-table conversion, schema redo). Now reinforced further by the July 15 pivot: even after permission to generate, understanding must come first, phase by phase (see above).

**⚠️ STANDING RULE (added July 14): never assume a value/type/fact about the actual data — always check the real data first, then implement.** Applies to anything that looks like it "should" be a certain way (a column's data type, a value's range, whether a format is uniform) — verify against the real data before writing it into a schema, config, or design doc.

**⚠️ Mindset correction (carry forward)**: Rohit wants this treated as an enterprise-level system, not a day-wise checklist — reasoned "why" for every strategy, proactive edge-case thinking (failure/drift/scale/concurrency — Section 14), business-use-case reasoning applied to ambiguous brief instructions (see Section 5A) rather than waiting on the trainer to resolve every ambiguity. Also: don't let "the brief doesn't mention it" be a reason to skip a genuine industry best practice — research the standard/official recommendation on its own merits and bring it forward even if the brief is silent.

**⚠️ Open, not blocking**: trainer's answers to the 3 original understanding-check-call doubts still not known. Capture here once known. Day 4 evaluation happened July 15 — detailed trainer feedback beyond "review is done" + the fumbling observation still not reported back.

---

## 1. Background & stakes
Retry/redemption capstone, individual project. **Deadline: July 22, 2026.**
**Correction**: the "not clear on UC/cluster policies/DABs" trainer feedback was on a *different, earlier* project — being deliberately imported as a lesson for VStone, not evidence of VStone's own history.

---

## 2. The project brief (source: uploaded `projectvstonev720260504.pdf`, v7.0)
Bronze→Silver→Gold medallion, Databricks Free Edition (serverless), Unity Catalog governed, DABs deployment, Git+CI/CD. Mandatory: RLS/CLS/masking, Liquid Clustering benchmark, SCD2 Gold dims, audit columns, no hardcoding, `assertDataFrameEqual`/`assertSchemaEqual`, sanity checks, branch flow `feature/data-profiling`→`feature/bronze-layer`→`feature/silver-layer`→`feature/gold-layer`→`dev`→`main`.

Confirmed directly from full text: Day 6.A requires DLT for Gold; Day 4 Silver requires a Pandas/Python UDF for header standardization — **confirmed technically imprecise: a Spark UDF (incl. Pandas UDF) transforms column values, not column names; renaming is a schema operation via `withColumnRenamed`/`toDF`, checked directly against Databricks' pandas UDF docs (Section 6B, Phase 2)**; Day 8-9 dashboard should use Databricks system billing tables for DBU usage; brief has an internal Day1/Day3-detail vs. Day4-summary-table inconsistency; brief is a generic multi-employee template requiring deliberate domain translation (see Section 5A). SCD2 is a Day 6/Gold-only requirement. Neither the Day 4-5 Silver nor Day 6 Gold checklist items use the literal word "join" anywhere.

---

## 3-4. Dataset — `xxjcaxx/trafficsimulator` (Kaggle), fully verified, with real profiled statistics (July 16)

Two independent fact domains, each with its own real dimension join:

| File | Rows | Columns (verified) | Join key |
|---|---|---|---|
| `cars.csv` | 24,681,794 | `enter`(int, 0–35), `exit`(int, 0–35), `date`(timestamp, ISO8601), `id`(int, **0–998 — NOT a unique row identifier alone**), `location`(int, 1–14) | `location` → `node_locations.location` |
| `node_locations.csv` | 14 | `latitude`/`longitude`(double, 6-decimal precision); `location=7` has bad `(0,0)` coords — confirmed | dimension for `cars.csv` (traffic domain) |
| `streets.csv` | 87,820,725 | `noise`/`pollution`(double, 0–~46.9), `light`(double, 5.14E-8–83.7), `raining`(double, **-0.9999999834 to 100.9999 — out-of-range anomaly**), `street_id`(int, 1–36), `date`(timestamp, ISO8601) | `street_id` → `streets_list.street_id` |
| `streets_list.csv` | 36 | `street`(string, already clean — confirmed no messy values found), `long`(int, street length in meters — NOT longitude), `latitude`/`longitude`(double), `dangerous`(double, **0.1–0.9**), `street_id`(int, 1–36, unique) | dimension for `streets.csv` (environment domain) |
| `telegram.csv` | ~128,440 | `message`(string, free text — **real found issue: multiple raw samples have leading whitespace, e.g. `" The car does not respect..."`, `"  This morning, from 8:20..."`**), `date`(string, **confirmed `DD/MM/YYYY` via hard evidence**), `hour`(string, plain `HH:mm:ss` time-of-day, NOT a real timestamp) | loosely connected by date only |

**Real findings from Phase 1 profiling (July 16), each shapes the Silver build directly — Phase 1 essentially closed, only `raining` bound sign-off outstanding:**

1. **`cars.csv`'s `id` is not a unique row identifier alone. RESOLVED**: `id` + `location` + `date` together, verified unique via a real `GROUP BY ... HAVING COUNT(*) > 1` query against the full 4-table union — zero duplicate groups. `silver_traffic`'s confirmed dedup key.
2. **`streets.csv`'s `raining` has a real out-of-bounds anomaly** (negative to ~101). Second quarantine test case for `silver_environment`. **Proposed bound**: `raining < 0 OR raining > 100` — awaiting Rohit's explicit yes.
3. **`telegram.csv`'s `hour` column is not a real timestamp** — bare `HH:mm:ss` string; an earlier `inferSchema=True` pass had silently invented today's date to attach to it. Bronze itself never had this issue (explicit STRING typing). Silver builds one real `event_timestamp` from `date` + `hour` together.
4. **`telegram.csv`'s `date` format resolved with hard evidence**: rows like `30/07/2023`, `18/10/2023` prove `DD/MM/YYYY` (no month can be 30 or 18). Parse explicitly with `to_date(date, 'dd/MM/yyyy')`.
5. **Referential integrity — CONFIRMED via real anti-join queries, both returned zero rows**: every `location` across all 4 Bronze traffic tables exists in `node_locations`; every `street_id` in `street_conditions` exists in `streets_list`. Closes the no-joins-at-Silver decision on hard evidence.

### 5A. Reasoning through the brief's "metadata files" exemption for this specific dataset (July 13)
`node_locations`/`streets_list` are reference/lookup data (no measurements of their own), the textbook shape of a **dimension table**, not a fact table. They reach Gold as `Dim_Location`/`Dim_Street`. Sayable answer: *"I treated the small reference files as dimension sources, not excluded files — they don't have their own metrics to aggregate, but the facts need them to be meaningful."*

---

## 5. Data model (Gold layer)
`Dim_Location`, `Dim_Street` (SCD2), `Dim_Date`. `Fact_Traffic_Counts`, `Fact_Street_Conditions`. `telegram.csv` = supporting reference. Gold must use DLT (brief-confirmed) — SCD2's officially-supported mechanism (`AUTO CDC`, `STORED AS SCD TYPE 2`) is exclusively a DLT capability, confirming the Gold DLT requirement.

**Fact-to-dimension joins happen at Gold, not Silver.** Backed by four independent checks: brief's Silver/Gold task split, Databricks' medallion docs, Kimball's "surrogate key pipeline" (join is *"the last step of fact table processing"*, after dimensions load), and real anti-join evidence confirming no referential-integrity problem exists at Silver.

---

## 6. Bronze layer — ✅ FULLY COMPLETE, GIT CLOSED OUT (July 13). Not being redone for the understand-first pivot — that applies going forward only.

Chunking: `cars.csv` chronological 40/30/20/10 → COPY INTO/DLT/Auto Loader/PySpark; `streets.csv` whole (Day 7 benchmark).

**8 Bronze tables, all live, all explicit-STRING-typed**: `bronze.traffic_counts_copyinto/_dlt(STREAMING_TABLE)/_autoloader/_pyspark` + `bronze.street_conditions/node_locations/streets_list/telegram`.

**3 jobs + 1 DLT pipeline**: `data_chunking_job`, `bronze_copy_into_job` (5 parallel tasks), `bronze_autoloader_pyspark_dlt_job` (3 tasks), `bronze_dlt_pipeline`.

**4 post-Day-3 fixes, all confirmed live, all merged**: (1) chunking idempotency + orchestration, (2) audit-schema bug fix, (3) Bronze explicit STRING schemas (PR#7) — also what silently prevented the `inferSchema`-invents-a-date bug from ever reaching Bronze's `telegram.hour` column — (4) PySpark XML fingerprint idempotency (PR#10), (5) DLT materialized-view → `STREAMING_TABLE` (PR#9).

**Git, fully closed out (July 13)**: PR#10 merged. `main` promotion from `dev` — still not explicitly reconfirmed.

**Schemas deployed**: `dev_rohitrathodcomp_raw`, `_bronze`, `_audit`, `_ops`. Tests: 55/55 passing, flake8 clean.

**Deep code-level walkthroughs done (Rohit can trace these end-to-end for a trainer)**: `databricks.yml`, `chunking.py`, COPY INTO's full call chain, `pyspark_xml_ingest.py` in full. Reusable 5-point tracing template given.

**Schema-creation answer for trainer**: UC schemas/namespaces declared as DABs resources (`resources/catalog.yml`). Table column schemas are explicit `StructType`, never inferred.

---

## 6A. Silver layer — architecture agreed and evidence-closed on the joins question; build proceeding phase-by-phase (Section 6B)

**Technology: Delta Live Tables, not plain PySpark.** Reached through explicit discussion on real technical merits (quarantine pattern is DLT-native, incremental handling via streaming tables, UDFs confirmed DLT-compatible, testability preserved via thin-wrapper pattern). All 5 Silver tables are streaming tables.

**Corrected 8-Bronze → 5-Silver table mapping**: `node_locations` (traffic domain dimension) and `streets_list` (environment domain dimension) are two independent chains.

- `silver_traffic` — UNION of 4 Bronze `traffic_counts_*` tables, deduped on confirmed composite key `id`+`location`+`date`, cleaned.
- `silver_locations` — from `bronze.node_locations`, `location=7` bad-coordinate row is the quarantine test case. **BUILT + VERIFIED**: 13 good / 1 rejected, exact match.
- `silver_environment` — from `bronze.street_conditions`, quarantine rule: `raining < 0 OR raining > 100` (awaiting sign-off).
- `silver_streets` — from `bronze.streets_list`, `dangerous` range corrected to 0.1–0.9. **BUILT + VERIFIED**: 36 good / 0 rejected, exact match.
- `silver_telegram` — `event_timestamp` from confirmed `dd/MM/yyyy` `date` + `HH:mm:ss` `hour`; parse failures are the quarantine rule.

**No fact-to-dimension joins at Silver — fully evidence-closed.** Closes the decision on four independent legs: brief silence + Databricks docs + Kimball methodology + real anti-join evidence (both empty).

**Build prompt**: `silver_layer_build_prompt.md` — the original agreed target architecture; being superseded phase-by-phase by the individual phase prompts as each phase completes and verifies (Phases 2 and 3 now both done).

---

## 6B. Silver build — phase breakdown for the understand-first pivot

1. **Data profiling & schema decisions** — ESSENTIALLY DONE (July 16). All 5 tables profiled with real evidence, dedup key confirmed, referential integrity confirmed. Only remaining: Rohit's explicit sign-off on the `raining` quarantine bound.
2. **Header-standardization UDF — BUILT AND VERIFIED (July 16)**: `silver_phase2_header_udf_prompt.md` run by Rohit via local Claude Code. Delivered: plain Python `standardize_column_name` function (5 rules: lowercase, strip whitespace, collapse non-alphanumeric runs to underscore, collapse repeated underscores, strip leading/trailing underscore) + rename-loop helper, applied across all 5 Silver tables; one genuine Pandas UDF scoped to `streets_list.street` only (whitespace trim/collapse), framed as Well-Architected Framework **Reliability pillar** insurance. `telegram.message`'s real found whitespace issue was a deliberate, explicit scope exclusion by Rohit's own choice (not overlooked) — worth remembering if it comes up with the trainer.
3. **Dimension staging: `silver_locations` & `silver_streets` — BUILT AND VERIFIED (July 16)**: `silver_phase3_dimension_staging_prompt.md` run by Rohit via local Claude Code. Both tables reuse Phase 2's header function (and the street Pandas UDF for `silver_streets`), both are DLT streaming tables, no joins. Quarantine pattern taught and now demonstrated concretely: two downstream table definitions from the same upstream source (passing condition vs. inverted/failing condition), each `_rejected` table carrying a `rejection_reason` STRING column. **Exact counts confirmed matching prediction**: `silver_locations` 13 good / 1 rejected (the known `location=7` bad-coordinate row); `silver_streets` 36 good / 0 rejected (both rules are insurance-only, no current violations) — this is real, demoable evidence for the trainer, not a hypothetical design.
4. **`silver_traffic`** — up next. Union of the 4 Bronze `traffic_counts_*` tables + dedup on the confirmed composite key (`id`+`location`+`date`). First phase involving a genuine UNION ALL across 4 sources and a real dedup operation, rather than a single-source pass-through.
5. **`silver_environment`** — not started.
6. **`silver_telegram`** — not started (date/hour parsing logic fully resolved with evidence, ready to implement once this phase starts).
7. **Delta time-travel demo** — not started.
8. **Testing, verification, git close-out** — not started.

---

## 7. Assets available
`XML_Data_Read_and_Write.py`/`.ipynb`, `csv_splitter.py`, `csv_to_json.py`/`csv_to_xml.py` (reference only, reimplemented independently, not copied unseen).

---

## 8. Architecture diagram
Final form delivered as SVG + synced to "vstone-architecture" Cowork artifact. **Known unresolved gaps, not yet fixed**: CI/CD box still shows `catalog: vstone_uc` (should be `vstone_traffic_dev/_test/_prod`); Bronze node still says "4 tables" (should say 8); should reflect 5 Silver tables once Silver is live, not 3.

---

## 12. Immediate next step
1. Confirm `main` was actually promoted from `dev` after PR#10.
2. **Phase 4 next**: `silver_traffic` — union + dedup of the 4 Bronze traffic tables on the confirmed composite key (`id`+`location`+`date`). Understand-first: teach why UNION not JOIN here, why dedup is "insurance" given the non-overlapping chunking guarantee, before generating the build prompt.
3. Also still open: Rohit's explicit sign-off on the `raining < 0 OR > 100` quarantine bound (low-risk, can close alongside Phase 4).
4. **Day 6+ Gold layer**: DLT-mandated, confirmed only path for SCD2. Fact-to-dimension joins happen here. SCD2 mechanics + real PK/FK constraints need explicit design. Apply the same understand-first phase breakdown used for Silver once Silver is done.
5. **Day 6 — flagged, needs Free Edition availability check when we get there**: Unity Catalog Metrics (maps to brief's Day 6 churn-style aggregate requirement) and Unity Catalog Business Glossary (maps to brief's Day 6 glossary requirement) — verify availability before committing. Also check Genie's GA changes against the Day 6 "Genie space" requirement.
6. **Day 8-9**: close the cross-job ordering gap; build the DBU dashboard off `system.billing` tables.
7. Capture trainer's actual Day 4 evaluation feedback in detail once Rohit reports it.
8. Ongoing: keep applying the Section 14 mindset — bring forward genuine best practices even when the brief is silent, always check real data before assuming, never generate code before the concept is understood, check official docs + our own brief + broader industry practice together, and close design decisions with a real query against real data wherever possible.

---

## 14. Engineering mindset — beyond the day-wise checklist

Framework: **Failure** / **Drift** / **Scale** / **Concurrency** — applied to every pipeline.

**Bronze**: Failure — well handled. Drift — STRING-typed Bronze exists precisely to catch drift explicitly at Silver; also silently prevented the `telegram.hour` inferSchema bug from ever reaching Bronze. Scale — untested past current volume, honestly framed as such. Concurrency — cross-job ordering is the real identified gap, deferred to Day 8-9.

**Silver (Phases 1-3 done and verified)**: quarantine table design resolved and now demonstrated with real, exact, matching counts — `silver_locations` 13/1, `silver_streets` 36/0 — genuine evidence for the trainer, not a hypothetical design. Header standardization correctly split into a plain rename function (schema-level, all 5 tables, Reliability-pillar insurance) and one genuine Pandas UDF (value-level, `streets_list.street`) — a real technical correction to the brief's imprecise wording, confirmed against official docs rather than assumed. `telegram.message` has a real found whitespace issue that was deliberately left out of scope by Rohit's own choice, not overlooked — worth remembering this was a conscious tradeoff if it comes up later. No-joins-at-Silver decision backed by four independent legs. **Meta-lesson from Day 4**: understanding has to precede generation, not just design — Phase 2/3's clean first-try exact-count results are early evidence the new working method is paying off.

**Gold (upcoming)**: DLT-for-Gold confirmed, only officially-supported SCD2 path. Fact-to-dimension joins happen here. SCD2 mechanics + real PK/FK constraints need explicit design. Two new-feature candidates flagged for Day 6 — verify Free Edition availability before committing. Apply the same understand-first phase breakdown used for Silver.

**Cross-cutting**: Free Edition's serverless-only constraint removes cluster-policy surface — frame as stricter cost discipline, not an absence of one.

**Observability gap**: job-failure alerts catch crashes, not silent data-quality problems. Current sanity checks catch structural breakage, not statistical anomalies.

---

## 15. Day 4 evaluation — completed July 15 (checkpoint gate, not a new git branch)

`day4_demo_script.md` delivered pre-evaluation. **Outcome**: evaluation happened; Rohit fumbled some trainer questions on parts he hadn't personally understood — directly triggered the July 15 working-style pivot. Detailed trainer feedback not yet captured — add once reported. **Note**: demo script predates the DLT/5-table Silver decision — needs a refresh before reuse at a future checkpoint.