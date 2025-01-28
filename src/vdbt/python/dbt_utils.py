from dbt.cli.main import dbtRunner
from prefect import get_run_logger

from src.vdbt.python import log_utils
from src.vdbt.python.export import export_execution_and_run_results
from src.vdbt.python.paths import PATH_DBT

COMMANDS_EXPORT = ["seed", "docs", "test", "run", "build"]
VALID_LOG_LEVES = ["debug", "info", "warn", "error", "none"]


def run_dbt_command(args, log_level="error"):
    """Run dbt command and raise exception if a problem is encountered"""

    # In order to use 'prefect.logger' inside the 'dbt.callbacks' we need to export it
    logger = get_run_logger()
    log_utils.LOGGER = logger

    assert args, f"{args=} must have at least one element"
    export_results = args[0] in COMMANDS_EXPORT

    # We change the 'log_level' to avoid log duplicates with the `prefect logger`
    assert log_level in VALID_LOG_LEVES, f"Invalid {log_level=} ({VALID_LOG_LEVES=})"
    args += ["--log-level", log_level]

    # Set up DBT paths
    args += ["--project-dir", PATH_DBT, "--profiles-dir", PATH_DBT]

    command = f"dbt {' '.join(args)}"
    logger.info(f"Running {command=}")

    res = dbtRunner(callbacks=[log_utils.log_callback]).invoke(args)

    if export_results:
        export_execution_and_run_results()

    return res.result
