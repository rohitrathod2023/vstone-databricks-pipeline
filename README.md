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
  utils/                     Shared, reusable code: logger.py, config_loader.py, audit.py, io_readers.py
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
- **One reusable ingestion path across formats.** `src/utils/io_readers.py` is a
  strategy-pattern dispatch (`READERS = {"csv": ..., "json": ..., "xml": ...}`) —
  the same `read_source()`/`write_source()` calls work for every format Day 1-3
  touches, and adding Parquet or another format later is one function + one
  registry entry.
- **Every table gets the same audit columns.** `src/utils/audit.py` adds
  `load_dt`, `source_format`, `source_file`, `run_id` identically in Bronze,
  Silver, and Gold — required by the brief, and it's what makes `run_id`
  traceable end-to-end for the Day 7 late-arriving-data / MERGE INTO work.
- **Custom logger everywhere.** `src/utils/logger.py` gives every module the
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

**Getting the raw data into the Volume.** Don't download it to your laptop at
all — `src/notebooks/00_download_raw_data.py` downloads all 5 files from
Kaggle ([xxjcaxx/trafficsimulator](https://www.kaggle.com/datasets/xxjcaxx/trafficsimulator))
**directly into the Volume**, running entirely on Databricks compute. No local
disk involved, no 5GB Catalog Explorer UI upload limit (that cap is a
drag-and-drop UI restriction, not a Volume storage limit — Volumes support
local file API access from cluster/serverless compute, so a plain file write
has no such cap).

One-time setup — store your Kaggle token as a Databricks secret, never in a
notebook or committed file:
```bash
databricks secrets create-scope kaggle
databricks secrets put-secret kaggle api_token   # paste your token from kaggle.com/settings/api when prompted
```

Then run the notebook (via your Git folder, or upload it manually) with the
`files` widget set to just the small files first
(`node_locations.csv,streets_list.csv`) to confirm serverless compute can
reach Kaggle's API before trusting it with `streets.csv` (7.8GB). Once that
works, widen the widget to the full file list and re-run — already-downloaded
files are skipped unless `force` is set to `true`.

Kaggle serves larger files as `.zip` archives; the notebook detects and
extracts these automatically so the Volume ends up with plain CSVs either
way. Its underlying Kaggle client already streams in 1MB chunks with 5
automatic retries and resume support — no extra code needed for large files.

## Every-day workflow (not just Day 1)

```bash
databricks bundle validate -t dev      # catches yaml/schema mistakes immediately
databricks bundle deploy   -t dev      # pushes notebooks + job definitions to the workspace
databricks bundle run data_chunking_job -t dev   # runs it, right now, in dev
```

Do this after every new job you add. Never let deploy-testing pile up until
the end.

## Local test loop (no Databricks needed)

These run the exact same two commands CI runs
(`.github/workflows/databricks-ci-cd.yml`), so a clean pass locally means CI
will pass too.

**Requirements before you start:**
- **Python 3.10+** (3.10 through 3.13 all verified working). `tests/requirements.txt`
  pins `pyspark>=4.0,<5.0` and `pandas>=2.0,<3.0` — matching Databricks'
  current runtime (DBR 18 ships Spark 4.1.0) and PySpark's own pandas-on-Spark
  module, which still depends on a pandas internal (`pandas.core.common._builtin_table`)
  that pandas 3.0 removed. That's the one real constraint: pandas must stay
  below 3.0 regardless of Python version — everything else is flexible.
- **A JDK (Java 17 recommended)** — PySpark needs one to launch its JVM.
  If `java -version` in your terminal already prints something 11+, you're
  set. If not, see the install commands below.

**Setup:**

```bash
py -3.12 -m venv .venv           # any Python 3.10+ works
.venv\Scripts\activate           # Windows; use `source .venv/bin/activate` on macOS/Linux
pip install -r tests/requirements.txt
```

That's it — no env vars to set by hand. `tests/conftest.py` runs automatically
before any test and handles the two things that used to require manual
per-shell setup:

- **`PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON`**: pinned to whichever Python is
  running pytest. Needed because Spark's worker subprocess otherwise calls a
  bare `python` command, which on Windows hits the fake Microsoft Store alias
  instead of your venv's real interpreter — causing `JAVA_GATEWAY_EXITED` /
  socket-timeout failures that look unrelated to your code.
- **`JAVA_HOME`**: left alone if you've already set it; otherwise auto-detected
  from common per-OS JDK install locations (`C:\Program Files\Microsoft`,
  `/usr/libexec/java_home` on macOS, `/usr/lib/jvm` on Linux). If no JDK is
  found anywhere, tests fail immediately with one clear line telling you what
  to install — not a multi-page traceback.

If you don't have a JDK yet, install one (Java 17 recommended) before running
tests:
- Windows: `winget install Microsoft.OpenJDK.17`
- macOS: `brew install openjdk@17`
- Linux: `sudo apt install openjdk-17-jdk` (Debian/Ubuntu) or your distro's
  equivalent

If your JDK lives somewhere `conftest.py` doesn't know to look, just set
`JAVA_HOME` yourself once (`setx JAVA_HOME "..."` on Windows, or add
`export JAVA_HOME=...` to your shell profile) — the auto-detection only
kicks in when it's unset.

Retyping `$env:JAVA_HOME` every session gets old fast — to set it once,
permanently, for your Windows user account instead:

```powershell
setx JAVA_HOME "C:\Program Files\Microsoft\jdk-17.<your-version>-hotspot"
```

(then open a **new** terminal for it to take effect). macOS/Linux: add the
`export JAVA_HOME=...` line to `~/.zshrc` / `~/.bashrc`.

**Run:**

```bash
flake8 src tests --max-line-length=120
pytest tests/unit -v
```

## Git workflow

`feature/data-profiling` → `feature/bronze-layer` → `feature/silver-layer` →
`feature/gold-layer`, each merged into `dev` only after its unit tests pass
and the corresponding DABs job has been deployed + run successfully in `dev`.
`dev` → `main` once, at the very end (Day 10), for the production release.
No direct commits to `main`.
