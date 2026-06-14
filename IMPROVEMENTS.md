# IMPROVEMENTS.md

Suggested improvements for `vtasks`, ordered roughly by impact. Each item notes the
affected file(s) and why it matters. Bugs come first because they are likely already
breaking flows in production.

## 🐞 Correctness bugs (fix first)

### 1. `maintain.sync_duckdb` passes the wrong keyword argument
[`vtasks/jobs/local/maintain.py:27`](vtasks/jobs/local/maintain.py:27)

```python
duck.sync_duckdb(src=src, dest=dest, schema_prefix=schema_prefixes, mode=mode)
```

`duck.sync_duckdb` expects `schema_prefixes`, not `schema_prefix`
([`duck.py:194`](vtasks/common/duck.py:194)). This call raises `TypeError` every time the
`maintain.sync_duckdb` deployment runs. Fix the keyword name.

### 2. `upload_marts_to_md` references a non-existent constant
[`vtasks/jobs/local/maintain.py:39`](vtasks/jobs/local/maintain.py:39)

```python
src=paths.FILE_DUCKDB_DBT,
```

`paths.py` only defines `FILE_DUCKDB` ([`paths.py:17`](vtasks/common/paths.py:17)); there
is no `FILE_DUCKDB_DBT`. This raises `AttributeError`. Either add the constant or use the
correct path. This flow is in `prefect.yaml`, so it is a live deployment.

### 3. `write_df` `mode` type hint omits `"merge"`
[`vtasks/common/duck.py:137`](vtasks/common/duck.py:137)

The signature is typed `Literal["append", "overwrite"]` but the body handles a third
`"merge"` mode ([`duck.py:181`](vtasks/common/duck.py:181)) and the merge path is the only
one that uses the `pk` argument. Add `"merge"` to the `Literal` so the type matches reality
and callers/linters know it's valid.

### 4. `run_query` ignores `merge` and `sync_duckdb` type vs. default mismatch
- `duck.sync_duckdb`'s `schema_prefixes` is annotated `str` but defaults to a `list`
  ([`duck.py:190-195`](vtasks/common/duck.py:190)). Annotate as `list[str]`.
- `run_flows` in `hourly.py` returns the `states` *module* on success
  ([`hourly.py:51`](vtasks/schedules/hourly.py:51)) instead of a state/`None`. Harmless
  today but misleading; return `states.Completed(...)` or nothing.

## 🧪 Testing

There is **no unit test suite** — the "integration test" in CI just runs the entire hourly
flow against live services ([`CI.yaml:35-64`](.github/workflows/CI.yaml)). This means:

- A PR can't be validated without working production secrets.
- Pure logic is untested and regressions (like bugs #1–#2) ship silently.

Recommendations:
- Add `pytest` and unit-test the pure helpers that have no I/O:
  `gsheets._get_coordinates` / `_get_range_to_update` / `_get_values_to_update`,
  `crypto.to_number`, `texts.remove_extra_spacing`, `paths.get_duckdb_path`.
- Test `duck.write_df` / `_merge_table` against an **in-memory or temp-file DuckDB** (no
  MotherDuck needed) — this is fast and covers the merge/overwrite/append logic.
- Keep the live end-to-end run as a separate, optional/manual CI job rather than the only
  signal on every PR.
- Add dbt tests beyond the schema scaffolding (the `tests/` dir is empty) and run
  `dbt build` against a local DuckDB in CI.

## 📦 Dependencies

- **Undeclared direct dependencies.** `requests` ([`indexa/indexa.py:1`](vtasks/jobs/indexa/indexa.py:1))
  and `loguru` ([`common/logs.py:11`](vtasks/common/logs.py:11)) are imported directly but
  not listed in `pyproject.toml` — they only resolve transitively. A dependency tree change
  could break imports. Add them explicitly.
- **Pinned-to-the-patch `prefect==3.2.7`** ([`pyproject.toml:31`](pyproject.toml)). An exact
  pin on the orchestrator blocks security/patch updates. Consider `>=3.2.7,<3.3` unless the
  exact pin is deliberate.
- The `loguru` import is wrapped in a `try/except ImportError` fallback. If it's a real
  dependency, declare it and drop the fallback; the dual-path logger adds complexity for no
  benefit.

## 🔍 Observability & error handling

- **Failure visibility.** `run_flows` logs failures and returns a `Failed` state, but there
  is no alerting (e.g. Slack/email) when an hourly run fails. A `SLACK_*` token exists in
  `secrets.yaml` — wire up a Prefect failure hook/notification so silent failures on the NAS
  surface somewhere.
- **Bare module-level side effects at import.** `paths.py` creates `.auth/` and computes
  `ENV` at import time ([`paths.py:36-41`](vtasks/common/paths.py:36)), and `secrets.py`
  calls `init_vcrypto` at import ([`secrets.py:7`](vtasks/common/secrets.py:7)). This makes
  importing the package do real work and raise `RuntimeError` in unrecognized environments,
  which hurts testability. Prefer lazy initialization behind functions.
- **Global connection cache.** `duck.py` caches `DB_PATH` in a module global
  ([`duck.py:14-33`](vtasks/common/duck.py)). It caches the *path string*, not a connection,
  yet the name and `Reusing` log imply connection reuse. Rename for clarity, and note it
  prevents switching between local/MD within one process.

## 🔐 Security

- Secrets are encrypted with `vcrypto` (good). Make sure `VTASKS_TOKEN` is never logged —
  audit the `{var=}` debug logs (e.g. `duck.py` logs the full MotherDuck connection string
  including the token at [`duck.py:22`](vtasks/common/duck.py:22) via `DB_PATH`). **The
  token-bearing `DB_PATH` is logged at debug level** — scrub it.
- Account numbers and API endpoints are hard-coded in source
  ([`indexa/indexa.py:9`](vtasks/jobs/indexa/indexa.py:9),
  [`crypto/main.py`](vtasks/jobs/crypto/main.py)). Low risk for a private repo, but consider
  moving identifiers to config/secrets if the repo ever goes public.

## 🧱 Code structure & maintainability

- **Duplicated `sync_duckdb` docstring/signature** between
  [`maintain.py`](vtasks/jobs/local/maintain.py) and [`duck.py`](vtasks/common/duck.py),
  already drifted out of sync (the docstring says default `"raw__"` but the code uses
  `["_marts__", "_core__"]`). Keep one source of truth.
- **Repetitive phase flows in `hourly.py`.** `maintain`/`updates`/`export`/`post_dbt` are
  four near-identical wrappers ([`hourly.py:54-75`](vtasks/schedules/hourly.py)). A small
  factory or a loop over `JOBS` would remove the duplication.
- **`paths.get_path` reimplements `os.path.join`/`Path`** with a manual split loop
  ([`paths.py:49-57`](vtasks/common/paths.py)). Use `PATH_ROOT.joinpath(*parts)`.
- Consider a top-level `CONTRIBUTING`/dev section or `Makefile`/`taskipy` targets so the
  many `uv run python -m ...` invocations are discoverable.

## 🏗️ dbt

- `tests/` and several `macros/`, `analyses/`, `seeds/`, `snapshots/` dirs are empty
  (`.gitkeep` only). Add `not_null`/`unique`/`relationships` tests on key columns and on
  the `pk` used by `_merge_table`, so data-quality regressions are caught by
  `dbt build --store-failures` (already enabled in `run.py`).
- `source freshness` is run but no `freshness` thresholds appear in the `_sources.yml`
  files — add `loaded_at_field` + `warn_after`/`error_after` so freshness checks are
  meaningful.

## ⚙️ CI/CD

- The `pre_commit` job and the local hooks can drift (hook versions are pinned in
  `.pre-commit-config.yaml`). Add a scheduled `prek autoupdate` / Dependabot for actions
  and hooks.
- CI uses mixed `actions/checkout` versions (`@v4` in one job,
  [`@v3`](.github/workflows/CI.yaml:38) in another) — standardize on `@v4`.
- Consider caching `uv` downloads in CI to speed up the integration job.

## 📚 Documentation

- `README.md` example `uv run python -m vtasks.jobs.dropbox.export_tables` points at a
  module that doesn't exist (the dropbox job is `money_lover.py`). Update the example.
- Document the environment-detection rules and the `is_pro()` production gate prominently —
  it's the single most surprising behavior for a new contributor (now captured in
  `AGENTS.md`).
