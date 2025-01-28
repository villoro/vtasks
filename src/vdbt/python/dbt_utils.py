import yaml
from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest
from prefect import get_run_logger

from src.vdbt.python import log_utils
from src.vdbt.python import paths
from src.vdbt.python.export import export_execution_and_run_results


ENV = None


def get_dbt_version():
    # This has less info than the 'manifest.json'
    return Manifest().to_dict()["metadata"]["dbt_version"]


def get_project_version():
    with open(paths.FILE_DBT_PROJECT, "r") as stream:
        return yaml.safe_load(stream)["version"]


def run_dbt_command(args, flow_run_id, log_level="error"):
    """Run dbt command and raise exception if a problem is encountered"""

    # In order to use 'prefect.logger' inside the 'dbt.callbacks' we need to export it
    logger = get_run_logger()
    log_utils.LOGGER = logger

    export_results = args[0] in ["seed", "docs", "test", "run", "build"]

    # We change the 'log_level' to avoid log duplicates with the `prefect logger`
    assert log_level in ["debug", "info", "warn", "error", "none"]
    args = ["--log-level", log_level] + args

    command = f"dbt {' '.join(args)}"
    logger.info(f"Running {command=}")

    res = dbtRunner(callbacks=[log_utils.log_callback]).invoke(args)

    if export_results:
        export_execution_and_run_results(flow_run_id)

    if not res.success:
        if res.exception:
            logger.critical(f"There was an exception in DBT: {res.exception=}")
            raise res.exception

    return res.result
