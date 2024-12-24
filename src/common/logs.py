import sys
from loguru import logger as loguru_logger

from prefect import get_run_logger
from prefect.exceptions import MissingContextError

LOGURU_LEVEL = "DEBUG"

loguru_logger.configure(handlers=[{"sink": sys.stdout, "level": LOGURU_LEVEL}])
loguru_logger.enable("vtasks")


def get_logger(force_loguru=False):
    if force_loguru:
        return loguru_logger

    try:
        return get_run_logger()
    except MissingContextError:
        return loguru_logger
