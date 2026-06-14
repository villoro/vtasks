# AGENTS.md

Guidance for AI agents (and humans) working in this repository.

## What this is

`vtasks` is Arnau Villoro's **personal ELT pipeline**. It extracts data from various
personal sources (Dropbox, Google Sheets, Google Calendar, CoinGecko, Indexa Capital),
loads it into DuckDB / MotherDuck, transforms it with **dbt**, and exposes the result to
Metabase for analysis. Everything is orchestrated with **Prefect 3** and runs hourly on
the author's NAS.

The acronym order is intentional: this is **E-L-T** (extract → load raw → transform with
dbt), not classic ETL.

## High-level architecture

```
sources (APIs, Dropbox, GSheets, GCal)
        │  extract + load raw          (vtasks/jobs/*)
        ▼
   DuckDB / MotherDuck  (raw__* schemas)
        │  transform                   (vtasks/vdbt — dbt project)
        ▼
   staging → core → marts schemas
        │  sync / upload               (vtasks/jobs/local/maintain.py)
        ▼
   MotherDuck  ──►  Metabase (visualization)
```

Orchestration entrypoint: [`vtasks/schedules/hourly.py`](vtasks/schedules/hourly.py).
The `hourly` flow runs phases **in order**: `maintain` → `updates` → `export` →
`run_dbt` → `post_dbt`. Each phase runs a list of subflows and fails the parent if any
child fails (see `run_flows`).

## Repository layout

| Path | Purpose |
|------|---------|
| `vtasks/common/` | Shared helpers: DuckDB IO (`duck.py`), Google Sheets (`gsheets.py`), Dropbox (`dropbox.py`), secrets (`secrets.py`), paths/env detection (`paths.py`), logging (`logs.py`), text utils (`texts.py`). |
| `vtasks/jobs/` | One folder per data source. Each is a set of Prefect `@flow`/`@task` functions. `main.py` usually exposes the top-level flow. |
| `vtasks/jobs/local/maintain.py` | DuckDB↔MotherDuck sync / upload flows. |
| `vtasks/schedules/hourly.py` | The hourly orchestration flow (main entrypoint). |
| `vtasks/vdbt/` | The dbt project (`dbt_project.yml`, `models/`, `macros/`, `profiles.yml`). |
| `vtasks/vdbt/python/` | Python wrapper that invokes dbt programmatically via `dbtRunner` and surfaces logs through Prefect (`run.py`, `dbt_utils.py`, `log_utils.py`, `paths.py`). |
| `scripts/` | Standalone, non-pipeline utilities (Calibre book sync, log parsing, subscriptions, hardware monitor). Not part of the Prefect flows. |
| `.github/` | CI workflow, Docker image for the Prefect server, version-bump scripts. |
| `prefect.yaml` | Prefect deployment definitions (one per flow). |
| `secrets.yaml` | Secrets, **encrypted at rest** with `vcrypto` (Fernet). Safe to commit. |

## dbt project (`vtasks/vdbt`)

Layered following dbt conventions, with schema-per-layer:

- **staging** (`stg__*`): `base_*` (1:1 with source, ephemeral) → `stg_*` (cleaned, materialized as table). Sources declared in `_sources.yml` files reference `raw__*` schemas.
- **core** (`core__*`): business entities, e.g. `core_expensor__investments`.
- **marts** (`marts__*`): final consumption tables for Metabase.

Naming convention: `<layer>_<domain>__<entity>.sql` (e.g. `marts_expensor__totals.sql`).
Account lists for the "expensor" domain are configured as dbt `vars` in `dbt_project.yml`.

dbt is **not** run via the `dbt` CLI in production — it is invoked from Python
(`vtasks/vdbt/python/run.py:run_dbt`), which runs `clean → deps → [debug] → build →
source freshness` and selects the target automatically (`md` on NAS, `local` elsewhere).

## Environment detection (important)

`vtasks/common/paths.py` infers the environment **once at import** into `ENV`:

- `GITHUB_ACTIONS` env var present → `"github"`
- Windows → `"local"`
- `/mnt/duckdb` exists → `"nas"` (this is "production", `is_pro() == True`)
- otherwise → raises `RuntimeError`

`is_pro()` gates production behavior: connecting to MotherDuck vs. a local
`.duckdb/villoro.duckdb` file, choosing the dbt target, and whether marts get uploaded to
MotherDuck. When testing locally you are **not** `pro`, so MotherDuck is skipped unless
`use_md=True` is passed explicitly.

## Secrets

- Stored encrypted in `secrets.yaml`, decrypted at runtime by `vcrypto`.
- The master key comes from the `VTASKS_TOKEN` environment variable.
- Access via `from vtasks.common.secrets import read_secret` / `export_secret`.
- Google service-account JSON and similar credentials are materialized on demand into
  `.auth/` (gitignored).

## Conventions

- **Logging:** always `from vtasks.common.logs import get_logger; logger = get_logger()`.
  `get_logger` returns the Prefect run logger when inside a flow/task, otherwise loguru.
  Prefer f-string `{var=}` debug style as used throughout.
- **Prefect:** name flows/tasks explicitly with dotted names
  (`@flow(name="crypto")`, `@task(name=f"{FLOW_NAME}.update_expensor")`).
- **DuckDB writes:** go through `vtasks/common/duck.py:write_df` (modes: `overwrite`,
  `append`, `merge`). Every write adds `_exported_at` and `_n_updates` columns.
- **External API calls:** wrap in retries/backoff (see `gsheets.retry_on_exception`,
  `backoff` dependency).
- **Imports:** sorted by `reorder-python-imports` (one import per line). Formatting/linting
  by `ruff`. Don't hand-format imports — let the hooks do it.
- **dbt model files** always come paired with a `.yml` describing columns/tests.

## Tooling & commands

Dependency management is **uv**; git hooks via **prek** (a pre-commit runner).

```bash
# setup
uv venv .venv
uv pip install --editable .
prek install

# update deps (regenerates requirements.txt via the uv-export hook)
uv lock --upgrade && uv sync

# run the full hourly pipeline locally
uv run python -m vtasks.schedules.hourly

# run a single job (module execution keeps relative imports working)
uv run python -m vtasks.jobs.crypto.main

# run dbt through the python wrapper
uv run python -m vtasks.vdbt.python.run

# version bump (updates pyproject.toml, prefect.yaml, dbt_project.yml together)
uvx bump-my-version bump [major|minor|patch]
```

`requirements.txt` is **generated** from `uv.lock` by a prek hook — never edit it by hand.

## CI

`.github/workflows/CI.yaml` runs on pull requests:

- `pre_commit` — runs all prek/pre-commit hooks (ruff, reorder-imports, yamlfmt, etc.).
- `labeler` — auto-labels the PR.
- `check_version` — enforces a version bump (via `villoro/vhooks`).
- `integration_tests` — installs deps and **runs the actual hourly flow end-to-end**
  (gated to changes under `vtasks/**`). There are **no unit tests** — this run is the test.

## Deployment

Manual, by design (the author does not give GitHub Actions access to the NAS):

```bash
set PREFECT_API_URL=http://tnas:6006/api   # NAS, reachable over Tailscale
uv run prefect --no-prompt deploy --all
```

Deployments and their schedules are defined in `prefect.yaml`.

## Gotchas for agents

- **No real test suite.** Verifying behavior usually means running a flow/module, which
  hits live external services and needs decryptable secrets (`VTASKS_TOKEN`). Be careful
  about side effects (it writes to real Google Sheets and MotherDuck).
- `paths.ENV` is computed at import time; changing env vars after import has no effect.
- Running modules with `python path/to/file.py` will break relative imports — always use
  `python -m vtasks.<...>`.
- `requests` and `loguru` are imported directly but are only transitive dependencies (not
  declared in `pyproject.toml`).
- Windows is a first-class local environment (the repo lives on `C:\`); `paths.py`
  normalizes `\` → `/` for DuckDB paths. Keep cross-platform behavior in mind.
