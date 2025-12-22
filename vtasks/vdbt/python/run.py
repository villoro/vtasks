import os

from prefect import flow
from prefect import task

from vtasks.common import paths
from vtasks.common.logs import get_logger
from vtasks.vdbt.python import dbt_utils


def set_dbt_env():
    """
    Set environment variables for dbt depending on target.
    This allows to query the appropiate duckdb local files
    """

    logger = get_logger()
    logger.debug("Checking duckdb_paths")

    for name in [paths.FILE_DUCKDB_RAW, paths.FILE_DUCKDB_DBT]:
        env_var_name = f"PATH_{name.upper()}_DUCKDB"
        path = paths.get_duckdb_path(name)
        logger.info(f"Setting {env_var_name}={path}")
        os.environ[env_var_name] = path


@task(name="dbt.clean")
def clean():
    """Clean temporary paths"""
    dbt_utils.run_dbt_command(["clean"])


@task(name="dbt.deps")
def deps():
    """Install DBT dependencies"""
    dbt_utils.run_dbt_command(["deps"])


@task(name="dbt.debug")
def run_debug():
    """Check for DBT problems"""
    dbt_utils.run_dbt_command(["debug"])


@task(name="dbt.build")
def build(select, exclude, store_failures):
    """Perform DBT build (seed + run + test)"""
    command = ["build"]

    if select:
        command += ["--select", select]
    if exclude:
        command += ["--exclude", exclude]
    if store_failures:
        command += ["--store-failures"]

    dbt_utils.run_dbt_command(command)


@task(name="dbt.freshness")
def freshness():
    """Check for DBT problems"""
    dbt_utils.run_dbt_command(["source", "freshness"])


@flow(name="dbt")
def run_dbt(select=None, exclude=None, debug=False, store_failures=True):
    """Run all DBT commands"""
    set_dbt_env()

    # Clean commands (in some cases it includes unwanted quotation marks)
    select = select.strip('"') if select is not None else None
    exclude = exclude.strip('"') if exclude is not None else None

    clean()
    deps()

    if debug:
        run_debug()

    build(select, exclude, store_failures)
    freshness()


if __name__ == "__main__":
    run_dbt()
