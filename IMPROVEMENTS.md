# IMPROVEMENTS.md

Suggested improvements for `vtasks`, ordered roughly by impact. Each item notes the
affected file(s) and why it matters.

> **How to maintain this file.** This is a living checklist of **open** items only. When
> something here is fixed, **delete its entry in the same change that resolves it** — the
> rationale and the diff live in git history and the PR that closed it. The list should
> always reflect what is still pending, never what has already been done. (For example, the
> correctness/type/security bugs originally listed here were removed when they were fixed in
> `7.6.2`.)

## 🧪 Testing

There is **no unit test suite** — the "integration test" in CI just runs the entire hourly
flow against live services ([`CI.yaml:35-64`](.github/workflows/CI.yaml)). This means:

- A PR can't be validated without working production secrets.
- Pure logic is untested, so regressions ship silently (several such bugs were only caught
  by manual review, not CI).

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
  could break imports. Add them explicitly (and re-run `uv lock`).
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
  ([`duck.py`](vtasks/common/duck.py)). It caches the *path string*, not a connection, yet
  the name and `Reusing` log imply connection reuse. Rename for clarity, and note it prevents
  switching between local/MD within one process.

## 🔐 Security

- Secrets are encrypted with `vcrypto` (good). Keep auditing `{var=}`-style debug logs across
  the jobs for any secret-bearing values before they reach a sink (the MotherDuck connection
  string in `duck.py` is already redacted; other call sites should get the same treatment).
- Account numbers and API endpoints are hard-coded in source
  ([`indexa/indexa.py:9`](vtasks/jobs/indexa/indexa.py:9),
  [`crypto/main.py`](vtasks/jobs/crypto/main.py)). Low risk for a private repo, but consider
  moving identifiers to config/secrets if the repo ever goes public.

## 🧱 Code structure & maintainability

- **`maintain.sync_duckdb` is a thin wrapper** around `duck.sync_duckdb`
  ([`maintain.py`](vtasks/jobs/local/maintain.py), [`duck.py`](vtasks/common/duck.py)) with a
  duplicated signature/docstring that has to be kept in sync by hand. Consider collapsing it
  so there is one source of truth.
- **Repetitive phase flows in `hourly.py`.** `maintain`/`updates`/`export`/`post_dbt` are
  four near-identical wrappers ([`hourly.py`](vtasks/schedules/hourly.py)). A small factory or
  a loop over `JOBS` would remove the duplication.
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
- Consider caching `uv` downloads in CI to speed up the integration job.
