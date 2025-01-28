from prefect import flow
from prefect import tags
from prefect import task

from src.vdbt.python import click
from src.vdbt.python import dbt_utils
from src.vdbt.python import export


@task(name="dbt.clean")
def clean(flow_run_id):
    """Clean temporal paths"""
    dbt_utils.run_dbt_command(["clean"], flow_run_id)


@task(name="dbt.deps")
def deps(flow_run_id):
    """Install DBT dependencies"""
    dbt_utils.run_dbt_command(["deps"], flow_run_id)


@task(name="dbt.debug")
def run_debug(flow_run_id):
    """Check for DBT problems"""
    dbt_utils.run_dbt_command(["debug"], flow_run_id)


@task(name="dbt.build")
def build(target, select, exclude, full_refresh, store_failures, flow_run_id):
    """Perform DBT build (seed + run + test)"""
    command = ["build"]

    if target:
        command += ["--target", target]
    if select:
        command += ["--select", select]
    if exclude:
        command += ["--exclude", exclude]
    if full_refresh:
        command += ["--full-refresh"]
    if store_failures:
        command += ["--store-failures"]

    dbt_utils.run_dbt_command(command, flow_run_id)


@task(name="dbt.export_models")
def export_models(flow_run_id):
    """Export information about the present models"""
    dbt_utils.run_dbt_command(["compile"], flow_run_id)
    export.export_models()


@flow(name="dbt")
def run_all(target, select, exclude, full_refresh, debug, store_failures, flow_run_id):
    """Run all DBT commands"""
    is_complete_run = select is None

    clean(flow_run_id)
    deps(flow_run_id)

    if debug:
        run_debug(flow_run_id)

    if is_complete_run:
        export_models(flow_run_id=flow_run_id)

    build(target, select, exclude, full_refresh, store_failures, flow_run_id)


@click.command()
@click.option("--target", "-t", default=None, help="DBT target")
@click.option("--select", "-s", default=None, help="Selected DBT models")
@click.option("--exclude", "-e", default=None, help="Excluded DBT models")
@click.option("--full_refresh", "--full-refresh", "-f", is_flag=True, default=False)
@click.option("--debug", "-d", is_flag=True, default=False, help="If true do debug")
@click.option("--store_failures", "--store-failures", is_flag=True, default=False)
@click.option(
    "--flow_run_id", "--flow-run-id", default=None, help="Prefect flow run ID"
)
def main(target, select, exclude, full_refresh, debug, store_failures, flow_run_id):
    from loguru import logger

    logger.info("Starting DBT project")

    target_default, target_type = dbt_utils.get_target_info_from_profiles()

    # Clean commands (in some cases it includes unwanted quotation marks)
    select = select.strip('"') if select is not None else None
    exclude = exclude.strip('"') if exclude is not None else None

    if target_type != "duckdb":
        dbt_utils.ENV = dbt_utils.get_env()
    else:
        dbt_utils.ENV = "CI"

    export.TAGS = {
        "type": "dbt",
        "env": dbt_utils.ENV,
        "version": dbt_utils.get_project_version(),
        "dbt_version": dbt_utils.get_dbt_version(),
        "target": target or target_default,
        "select": select,
        "exclude": exclude,
        "full_refresh": full_refresh,
        "target_type": target_type,
    }

    run_tags = [f"{k}:{v}" for k, v in export.TAGS.items()]
    logger.info(f"Using {run_tags=}")

    # This try catch is to let `fargate` know the process failed
    with tags(*run_tags):
        run_all(
            target,
            select,
            exclude,
            full_refresh,
            debug,
            store_failures,
            flow_run_id,
        )


if __name__ == "__main__":
    main()
