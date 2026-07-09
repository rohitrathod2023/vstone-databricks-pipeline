# New Laptop Setup Guide — DABs, GitHub, GitHub Actions, Branch Protection

Follow this top to bottom on the new laptop. Steps 1–3 are one-time machine
setup, 4–6 get the repo live and CI running, 7 covers branch protection —
including the honest answer on the "PR approver = myself" question.

## 0. First, the one fact that shapes step 7

GitHub does not let a pull request author approve their own pull request —
this is a hard platform rule, not a setting, and it applies the same way on
personal repos as on organization repos ([GitHub Docs: Approving a pull
request with required reviews](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/reviewing-changes-in-pull-requests/approving-a-pull-request-with-required-reviews)).
So there's no configuration that gives you a real "Require 1 approval" rule
where you are the one clicking Approve on your own PR.

The workaround you picked: **branch protection with an admin bypass.** You
turn on "require a pull request" and "require status checks to pass" (CI),
but you leave "Do not allow bypassing the above settings" unchecked. As the
repo owner/admin, GitHub then lets you merge your own PR once CI is green —
no approval step exists to block on, but nothing merges without going
through a PR and passing tests/lint first. That's what step 7 configures.

## 1. Install prerequisites

- **Git** — https://git-scm.com/downloads. Verify: `git --version`
- **Python 3.10+** — https://www.python.org/downloads/. Verify: `python --version`
  (Windows: check "Add python.exe to PATH" during install.)
- **Databricks CLI (v2, includes `databricks bundle`)**
  - macOS/Linux: `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh`
  - Windows (PowerShell): `winget install Databricks.DatabricksCLI`
  - Verify: `databricks -v` (needs to be 0.205+ for DABs)
- **GitHub CLI** (optional but makes step 4 much faster) —
  https://cli.github.com/. Verify: `gh --version`
- A code editor (VS Code recommended, with the Databricks and Python
  extensions).

## 2. Get the project files onto the new laptop

Copy (USB/cloud drive/AirDrop/whatever) the `vstone-databricks-pipeline`
folder you already have onto the new laptop. Don't clone from GitHub yet —
this local folder is what you're about to push up as the initial commit.

## 3. Authenticate the Databricks CLI

```bash
cd vstone-databricks-pipeline
databricks auth login --host https://<your-workspace-url>.cloud.databricks.com
```

This opens a browser, you log into your Free Edition workspace, and the CLI
stores a token profile locally. Confirm it worked:

```bash
databricks current-user me
```

Then open `databricks.yml` and replace the three `host:` placeholders (one
per target: dev/test/prod) with that same workspace URL — Free Edition is a
single workspace, so all three targets point at the same host and only the
`catalog` variable differs.

Sanity-check the bundle parses before touching GitHub at all:

```bash
databricks bundle validate -t dev
```

Fix anything it complains about now, while it's just you and the CLI —
easier than debugging it inside a failing GitHub Actions run later.

## 4. Create the GitHub repo and push

**Using GitHub CLI (fastest):**

```bash
gh auth login                      # one-time, follow the browser prompt
cd vstone-databricks-pipeline
git init
git add .
git commit -m "Initial commit: Day 1 scaffold"
gh repo create vstone-databricks-pipeline --private --source=. --remote=origin
git branch -M main
git push -u origin main
git checkout -b dev
git push -u origin dev
```

**Without GitHub CLI (web UI):**

1. github.com → **New repository** → name `vstone-databricks-pipeline` →
   **Private** → don't initialize with README/gitignore (you already have
   both) → **Create repository**.
2. Copy the URL it gives you, then:

```bash
cd vstone-databricks-pipeline
git init
git add .
git commit -m "Initial commit: Day 1 scaffold"
git branch -M main
git remote add origin https://github.com/<your-username>/vstone-databricks-pipeline.git
git push -u origin main
git checkout -b dev
git push -u origin dev
```

Either way, you now have `main` and `dev` on GitHub. Per the repo's Git
workflow, all real work happens on `feature/*` branches merged into `dev`;
`main` only gets `dev` merged into it once, at the very end.

## 5. Wire up GitHub Actions secrets

The CI workflow (`.github/workflows/databricks-ci-cd.yml`) runs `flake8` +
`pytest` on every PR with no Databricks credentials needed. Its deploy step
is currently commented out — uncomment it only when you're ready for CI to
also run `databricks bundle deploy`. Either way, add the secrets now so
they're ready:

1. On GitHub: repo → **Settings** → **Secrets and variables** → **Actions**
   → **New repository secret**.
2. Add:
   - `DATABRICKS_HOST` → `https://<your-workspace-url>.cloud.databricks.com`
   - `DATABRICKS_TOKEN` → a personal access token. Generate one in the
     Databricks workspace: **Settings** (user icon, top right) →
     **Developer** → **Access tokens** → **Generate new token**. Copy it
     immediately — Databricks only shows it once.

If/when you uncomment the deploy step, it should authenticate using these
two secrets as environment variables (`DATABRICKS_HOST` /
`DATABRICKS_TOKEN`), the same way the CLI does locally.

## 6. Open a real PR and watch CI run

```bash
git checkout dev
git checkout -b feature/data-profiling
# (this branch should already have your profiling commits from before —
#  if not, make a small change here just to test the pipeline)
git push -u origin feature/data-profiling
gh pr create --base dev --title "Data profiling" --body "Day 1 profiling docs + chunking pipeline"
```

Or via the web UI: push the branch, GitHub will show a **Compare & pull
request** banner — click it, set base branch to `dev`, create the PR. Either
way, check the **Checks** tab on the PR and confirm `flake8`/`pytest` run
and pass. This confirms Actions is wired up correctly before you add the
protection rule that will start requiring it.

## 7. Branch protection — CI-gated, admin-bypass merge

This is the "admin bypass" setup from step 0: protects `dev` and `main` so
nothing merges without going through a PR and passing CI, without a
same-person-can't-approve deadlock.

Repo → **Settings** → **Branches** → **Add branch protection rule**.

**Rule for `dev`:**
- Branch name pattern: `dev`
- ☑ **Require a pull request before merging**
  - Required approvals: **0** (leave unchecked / set to 0 — this is the
    piece that would otherwise deadlock a solo repo, since you can never
    approve your own PR)
- ☑ **Require status checks to pass before merging**
  - Search and select the CI job(s) from `databricks-ci-cd.yml` (e.g.
    `test` / `lint` — the exact names are the `jobs:` keys in that
    workflow file) — this list only populates with checks that have run at
    least once, which is why step 6 ran a real PR first
  - ☑ Require branches to be up to date before merging
- Leave **"Do not allow bypassing the above settings"** unchecked. This is
  the actual mechanism: it keeps you, the admin, able to merge once CI is
  green, while still blocking anyone/anything without admin rights and
  blocking any merge where CI hasn't passed.
- **Save changes.**

**Rule for `main`:** repeat the same rule with branch name pattern `main`.
Since your workflow only merges `dev` → `main` once at the very end, you
could also skip protecting `main` until you're closer to Day 10 — either is
fine.

**What this gets you, concretely:** every change has to go through a PR;
every PR has to pass `flake8` + `pytest` in Actions before the merge button
even unlocks; direct pushes to `dev`/`main` are blocked for anyone without
admin rights (which on a solo repo is just you, so this mainly future-proofs
it if you ever add a collaborator). What it does *not* give you is a second
set of eyes clicking "Approve" — that's the tradeoff of working solo, and
GitHub's own author-can't-approve-own-PR rule makes a same-account version
of that impossible regardless of settings.

If you later want an actual approval gate (not just CI), the only way to
get one on GitHub is a second account (a free personal account you also
own) added as a collaborator, used solely to click Approve on your PRs.
That's a heavier setup for not much real benefit on a solo project — the
CI gate above already blocks broken code from merging, which is the part
that matters.

## Quick reference — full loop on this new laptop from here on

```bash
git checkout dev && git pull
git checkout -b feature/<name>
# ... make changes ...
pytest tests/unit -v
flake8 src tests --max-line-length=120
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run <job_name> -t dev
git add . && git commit -m "..."
git push -u origin feature/<name>
gh pr create --base dev
# wait for CI to go green, then merge (admin bypass lets you do this yourself)
```

## Sources

- [Approving a pull request with required reviews — GitHub Docs](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/reviewing-changes-in-pull-requests/approving-a-pull-request-with-required-reviews)
- [Managing a branch protection rule — GitHub Docs](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/managing-a-branch-protection-rule)
- [About protected branches — GitHub Docs](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches)
- [Pull request approval permissions and rules in GitHub — Graphite](https://graphite.com/guides/pull-request-approval-permissions-rules-github)
