import os
import shutil

from prefect import flow
from prefect import task

from vtasks.common.logs import get_logger
from vtasks.common.paths import get_duckdb_path
from vtasks.vdbt.python import dbt_utils
from vtasks.vdbt.python import export

FILE_DUCKDB_RAW = "raw"
FILE_DUCKDB_DBT = "dbt"
FILE_DUCKDB_METABASE = "metabase"


def set_dbt_env():
    """Set environment variables for dbt depending on target."""
    logger = get_logger()
    logger.debug("Checking duckdb_paths")

    for name in [FILE_DUCKDB_RAW, FILE_DUCKDB_DBT]:
        env_var_name = f"PATH_{name.upper()}_DUCKDB"
        path = get_duckdb_path(name)
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


@task(name="dbt.export_models")
def export_models():
    """Export information about the present models"""
    dbt_utils.run_dbt_command(["compile"])
    export.export_models()


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


@task(name="dbt.copy_duckdb")
def copy_duckdb():
    """Copy the output duckdb file to prevent locks with metabase"""

    logger = get_logger()

    src = get_duckdb_path(FILE_DUCKDB_DBT)
    dest = get_duckdb_path(FILE_DUCKDB_METABASE)

    logger.info(f"Copying {src=} to {dest=}")
    shutil.copy2(src, dest)
    logger.info(f"{dest=} successfully exported")


@flow(name="dbt")
def run_dbt(select=None, exclude=None, debug=False, store_failures=True):
    """Run all DBT commands"""
    set_dbt_env()

    # Clean commands (in some cases it includes unwanted quotation marks)
    select = select.strip('"') if select is not None else None
    exclude = exclude.strip('"') if exclude is not None else None

    is_complete_run = select is None

    clean()
    deps()

    if debug:
        run_debug()

    if is_complete_run:
        export_models()

    build(select, exclude, store_failures)
    freshness()
    copy_duckdb()


if __name__ == "__main__":
    run_dbt()
