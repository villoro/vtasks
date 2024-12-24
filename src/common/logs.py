import sys
from loguru import logger

from prefect import get_run_logger
from prefect.exceptions import MissingContextError


def get_logger():
    try:
        return get_run_logger()
    except MissingContextError:
        logger.configure(handlers=[{"sink": sys.stdout, "level": "DEBUG"}])
        logger.enable("vtasks")
        return logger
