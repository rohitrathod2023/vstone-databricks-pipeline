# Day 8-9: cross-job dependency enforcement

Closes the "cross-job ordering" gap flagged since Bronze: Bronze, Silver, and
Gold used to run as independent jobs, triggered manually/separately, with
nothing enforcing that Bronze finishes before Silver starts, or Silver
before Gold. Done directly on `dev`, no new feature branch.

## Step 0 — real job/pipeline inventory

Confirmed directly from `resources/jobs/*.yml` and `resources/pipelines/*.yml`
rather than assumed from memory:

| Job | Display name | Tasks | Task type(s) |
|---|---|---|---|
| `data_chunking_job` | "Data Chunking" | 2, already chained internally (`chunk_cars_csv` depends on `download_raw_data`) | `notebook_task` |
| `bronze_copy_into_job` | "Bronze COPY INTO" | 5, fully independent/parallel | `notebook_task` |
| `bronze_autoloader_pyspark_dlt_job` | "Bronze Auto Loader / PySpark / DLT Ingestion" | 3, fully independent/parallel | 2x `notebook_task` + 1x `pipeline_task` (`bronze_dlt_pipeline`) |
| `silver_job` | "Silver Layer" | 1 | `pipeline_task` (`silver_dlt_pipeline`) |
| `gold_job` | "Gold Layer" | 1 | `pipeline_task` (`gold_dlt_pipeline`) |

Key finding: **Bronze is not one job, it's three.** `data_chunking_job`
produces the raw/chunked files everything downstream reads; both
`bronze_copy_into_job` and `bronze_autoloader_pyspark_dlt_job` consume that
output but are otherwise independent of each other (they load different
Bronze tables). `silver_job` and `gold_job` are each already a single
`pipeline_task` wrapping their respective DLT pipeline -- no restructuring
needed there, they're clean units to depend on as-is.

(`gold_aggregates_job`, the duplicate removed in Day 7, was confirmed absent
from `resources/jobs/` before starting this work -- not reintroduced.)

## Design: one orchestrating job, not job-to-job triggers

`medallion_pipeline_job` (`resources/jobs/medallion_pipeline_job.yml`) --
named by function, not by day, per this project's standing rule.

**Why not job-to-job triggering:** Databricks has no native "trigger this
job when that job succeeds" mechanism separate from putting a
`run_job_task` inside a job's own task list. Since `silver_job` has a real
**fan-in** dependency -- it must wait for *both* `bronze_copy_into_job` and
`bronze_autoloader_pyspark_dlt_job`, which run in parallel -- job-to-job
triggering would mean adding a `run_job_task` to the tail of *two* separate
job definitions, each independently trying to trigger Silver, with no clean
way to guarantee Silver runs exactly once, after both finish (a real race:
whichever Bronze job finishes second would trigger Silver while the other
might still be running, or Silver could be triggered twice). A single
orchestrating job with `depends_on` (which natively supports depending on
multiple upstream tasks) handles this fan-in correctly and keeps the entire
dependency chain visible in one file instead of scattered across five.

Every task in `medallion_pipeline_job` uses `run_job_task` against the
**existing** job resources, not `pipeline_task` direct to the DLT pipelines
-- this orchestrator is purely a sequencing layer on top of what already
exists. If a task is later added inside e.g. `bronze_copy_into_job`, this
file doesn't need to change to pick it up, and no existing job's display
name, tasks, or internal structure was touched (`data_chunking_job` keeps
its exact required "Data Chunking" name).

```
data_chunking
  |
  +--> bronze_copy_into --------+
  |                             |
  +--> bronze_autoloader_pyspark_dlt --+
                                 |     |
                                 v     v
                               silver (waits for both)
                                 |
                                 v
                               gold
```

## Verification

### Success path — real per-task timing, not just eventual success

Ran `databricks bundle run medallion_pipeline_job -t dev` end to end. Every
task succeeded, and the real start/end timestamps (UTC) confirm the actual
run order, not just that all 5 eventually finished:

| Task | Start | End | Duration | Result |
|---|---|---|---|---|
| `data_chunking` | 21:39:32 | 21:42:46 | 194.7s | SUCCESS |
| `bronze_autoloader_pyspark_dlt` | 21:42:47 | 21:44:10 | 83.7s | SUCCESS |
| `bronze_copy_into` | 21:42:47 | 21:45:21 | 154.0s | SUCCESS |
| `silver` | 21:45:21 | 21:49:38 | 256.4s | SUCCESS |
| `gold` | 21:49:38 | 21:51:44 | 126.0s | SUCCESS |

Both Bronze tasks started within a second of `data_chunking` finishing, and
in parallel with each other (not waiting on one another). `silver` started
within a second of `bronze_copy_into` finishing -- the *later*-finishing of
the two Bronze tasks, confirming it waited for both, not just one. `gold`
started within a second of `silver` finishing. Real proof of enforced
ordering, not documentation of intent.

### Failure path — the actual proof the dependency is enforced

Deliberately broke `bronze_copy_into_job`'s `copy_into_chunk1_csv` task by
pointing its `source_key` at a value that doesn't exist in `sources.yml`,
deployed, and re-ran `medallion_pipeline_job`:

| Task | Result state | Life cycle state |
|---|---|---|
| `data_chunking` | SUCCESS | TERMINATED |
| `bronze_autoloader_pyspark_dlt` | SUCCESS | TERMINATED |
| `bronze_copy_into` | **FAILED** | TERMINATED |
| `silver` | `UPSTREAM_FAILED` | **SKIPPED** |
| `gold` | `UPSTREAM_FAILED` | **SKIPPED** |

`silver` and `gold` never ran at all (`SKIPPED`, not just failed) once their
upstream dependency failed -- this is the real proof, not the happy-path
ordering alone, which only shows things ran in the right sequence when
everything succeeds. Reverted the deliberate breakage back to the correct
`source_key: chunk1_csv` and redeployed/re-ran to confirm the pipeline is
back to a fully working state.

## Verification checklist

- `databricks bundle validate -t dev` passes.
- Full success-path run: all 5 tasks SUCCESS, real timestamps confirm
  enforced ordering.
- Deliberate-failure run: Bronze task FAILED, Silver and Gold both SKIPPED
  with `UPSTREAM_FAILED` -- confirms the dependency is enforced, not just
  documented.
- No existing job's display name, tasks, or structure changed.
  `gold_aggregates_job` not reintroduced.
- Full local `pytest` suite unaffected (no Python/pipeline logic touched,
  only job orchestration YAML).
- `flake8` clean (no new Python code -- this is YAML-only).
