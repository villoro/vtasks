import os

from prefect import flow
from prefect import task

from src.common.secrets import read_secret
from src.vdbt.python import dbt_utils
from src.vdbt.python import export

MD_TOKEN = "MOTHERDUCK_TOKEN"


@task(name="dbt.export_token")
def export_motherduck_token():
    os.environ[MD_TOKEN] = read_secret(MD_TOKEN)


@task(name="dbt.clean")
def clean():
    """Clean temporal paths"""
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


@task(name="dbt.export_models")
def export_models():
    """Export information about the present models"""
    dbt_utils.run_dbt_command(["compile"])
    export.export_models()


@flow(name="dbt")
def run_dbt(select=None, exclude=None, debug=False, store_failures=True):
    """Run all DBT commands"""
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


if __name__ == "__main__":
    run_dbt()
