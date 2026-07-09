# vstone-databricks-pipeline

End-to-end Bronze → Silver → Gold medallion pipeline for the VStone Databricks
capstone (retry), built on the `xxjcaxx/trafficsimulator` Kaggle dataset —
simulated traffic sensor data for a neighborhood in Valencia, Spain. Databricks
Free Edition, serverless only, Unity Catalog governed, deployed via Databricks
Asset Bundles (DABs).

See `docs/requirements_and_assumptions.md` and `docs/dataset_profiling.md` for
the full write-up. This README is the "how do I run this" reference.

## Repo layout

```
databricks.yml              DABs bundle: targets dev/test/prod -> vstone_traffic_dev/test/prod catalogs
resources/jobs/*.yml         One file per Databricks Job, included by databricks.yml
src/
  notebooks/                 Thin Databricks Job entrypoints only — no real logic here
  pipelines/                 Actual pipeline logic, organized by layer (ingestion/, silver/, gold/ as they're added)
  common/                    Shared, reusable code: logger.py, config_loader.py, audit.py, io_readers.py
  config/                    sources.yml (per-dataset config) + env.yml (per-environment config)
tests/
  unit/                      pytest, runs in GitHub Actions on every PR — no Databricks cluster needed
  integration/                (added once Bronze/Silver/Gold exist and there's something to integration-test)
docs/                        Requirement/assumptions doc, dataset profiling notes
```

## Design principles this repo follows (so every day's work reuses the last)

- **Config-driven, not hardcoded.** Every dataset (raw file, chunk, Bronze/Silver/Gold
  table) is one entry in `src/config/sources.yml` — path, format, technique, target
  table. Notebooks call `generic_ingest("<key>")`-style functions; adding a new
  source is a config edit, not new code.
- **One reusable ingestion path across formats.** `src/common/io_readers.py` is a
  strategy-pattern dispatch (`READERS = {"csv": ..., "json": ..., "xml": ...}`) —
  the same `read_source()`/`write_source()` calls work for every format Day 1-3
  touches, and adding Parquet or another format later is one function + one
  registry entry.
- **Every table gets the same audit columns.** `src/common/audit.py` adds
  `load_dt`, `source_format`, `source_file`, `run_id` identically in Bronze,
  Silver, and Gold — required by the brief, and it's what makes `run_id`
  traceable end-to-end for the Day 7 late-arriving-data / MERGE INTO work.
- **Custom logger everywhere.** `src/common/logger.py` gives every module the
  same `get_logger(__name__, catalog=...)` — logs to stdout (visible in the
  Databricks job run UI) and best-effort appends to
  `<catalog>.audit.pipeline_logs`, a real Delta table backing the "Audit &
  Observability Layer" in the architecture diagram.
- **DABs from Day 1, not bolted on at the end.** Every job gets deployed and
  run the same day it's written — see below. By Day 10 there's no first-time
  deploy risk, just a target switch to `prod`.

## One-time setup

```bash
git clone <your-repo-url>
cd vstone-databricks-pipeline

pip install -r tests/requirements.txt        # local lint/test only, no cluster needed

databricks auth login --host https://<your-workspace-url>.cloud.databricks.com
```

Then in `databricks.yml`, replace the three `host:` placeholders with your
actual Free Edition workspace URL (same URL in all three targets — Free
Edition is one workspace; only the `catalog` variable differs per target).

Before the first deploy, create the three Unity Catalog catalogs and the raw
Volume (one-time, via SQL editor or `databricks` CLI):

```sql
CREATE CATALOG IF NOT EXISTS vstone_traffic_dev;
CREATE SCHEMA  IF NOT EXISTS vstone_traffic_dev.raw;
CREATE SCHEMA  IF NOT EXISTS vstone_traffic_dev.bronze;
CREATE SCHEMA  IF NOT EXISTS vstone_traffic_dev.audit;
CREATE VOLUME  IF NOT EXISTS vstone_traffic_dev.raw.raw_volume;
-- repeat for vstone_traffic_test / vstone_traffic_prod when you get there
```

**Uploading the raw data — read this before you try the UI.** Catalog
Explorer's drag-and-drop upload is capped at **5 GB per file**. That's a UI
limit, not a Volume storage cap or a Free Edition data cap — Volumes
themselves hold files up to your cloud storage provider's max size. It still
means `streets.csv` (7.8 GB) can't go through the UI.

Use `scripts/upload_raw_data.py` for **all 5 files, in one run** — it streams
straight from disk (never loads a whole file into memory) and works the same
way regardless of file size:

```bash
pip install databricks-sdk
python scripts/upload_raw_data.py --catalog vstone_traffic_dev --data-dir "D:\v4c\Databricks\vstone\traffic_simulator\src\data"
```

This uploads `cars.csv`, `streets.csv`, `node_locations.csv`,
`streets_list.csv`, and `telegram.csv` to
`/Volumes/vstone_traffic_dev/raw/raw_volume/incoming/`.

**Checkpointing — safe to just re-run the exact same command if something
fails.** The script writes a local `scripts/.upload_state.<catalog>.json`
(gitignored) recording which files already succeeded. Re-running skips
anything already uploaded and unchanged, and only retries whatever failed —
so if `streets.csv` times out partway through, you don't sit through
re-uploading the 4 small files again. Each file also gets 3 attempts with
backoff (2s/4s/8s) within a single run before being marked failed. One real
limit worth knowing: this does **not** resume a single file from the byte it
died at — the Databricks SDK doesn't expose an offset/resume parameter, so a
failed `streets.csv` restarts from 0 on retry, it just doesn't also drag the
other 4 files down with it. Use `--force` to ignore the checkpoint and
re-upload everything regardless of prior success; `--files streets.csv` to
target just one file.

(The `databricks fs cp` CLI command works too, and doesn't require installing
the SDK — but it's only reliable for the 4 smaller files; Databricks' own
docs note it can hit transient I/O errors on very large files over the
FUSE-based copy path. Stick with the script unless you have a reason not to.)

## Every-day workflow (not just Day 1)

```bash
databricks bundle validate -t dev      # catches yaml/schema mistakes immediately
databricks bundle deploy   -t dev      # pushes notebooks + job definitions to the workspace
databricks bundle run data_chunking_job -t dev   # runs it, right now, in dev
```

Do this after every new job you add. Never let deploy-testing pile up until
the end.

## Local test loop (no Databricks needed)

```bash
pip install -r tests/requirements.txt
flake8 src tests --max-line-length=120
pytest tests/unit -v
```

CI runs the same two commands automatically on every PR into `dev` or `main`
(`.github/workflows/databricks-ci-cd.yml`).

## Git workflow

`feature/data-profiling` → `feature/bronze-layer` → `feature/silver-layer` →
`feature/gold-layer`, each merged into `dev` only after its unit tests pass
and the corresponding DABs job has been deployed + run successfully in `dev`.
`dev` → `main` once, at the very end (Day 10), for the production release.
No direct commits to `main`.
