from dbt.cli.main import dbtRunner
from prefect import get_run_logger

from vtasks.vdbt.python import log_utils
from vtasks.vdbt.python.paths import PATH_DBT

VALID_LOG_LEVES = {"debug", "info", "warn", "error", "none"}
COMMANDS_NO_RESULT = {"clean", "deps"}


def check_dbt_result(res, command):
    """Check dbt result and raise an exception if an error occurred"""

    logger = get_run_logger()

    command_name = command.split()[1]
    missing_result = res.result is None and command_name not in COMMANDS_NO_RESULT
    logger.debug(f"{command_name=}, {res.result}, {res.success}, {missing_result=}")

    if missing_result or not res.success:
        message = f"DBT failed: {command=}"
        logger.error(message)
        raise RuntimeError(message)

    return res.result


def run_dbt_command(args, log_level="error"):
    """Run dbt command and raise exception if a problem is encountered"""

    # In order to use 'prefect.logger' inside the 'dbt.callbacks' we need to export it
    logger = get_run_logger()
    log_utils.LOGGER = logger

    original_command = f"dbt {' '.join(args)}"

    assert args, f"{args=} must have at least one element"

    # We change the 'log_level' to avoid log duplicates with the `prefect logger`
    assert log_level in VALID_LOG_LEVES, f"Invalid {log_level=} ({VALID_LOG_LEVES=})"
    args += ["--log-level", log_level]

    # Set up DBT paths
    args += ["--project-dir", PATH_DBT, "--profiles-dir", PATH_DBT]

    command = f"dbt {' '.join(args)}"
    logger.info(f"Running {command=}")

    res = dbtRunner(callbacks=[log_utils.log_callback]).invoke(args)
    return check_dbt_result(res, original_command)
