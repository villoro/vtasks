import logging
import sys

from prefect import get_run_logger
from prefect.exceptions import MissingContextError

LOG_LEVEL = "DEBUG"

# Try importing loguru, fallback to standard logging if not available
try:
    from loguru import logger

    logger.configure(handlers=[{"sink": sys.stdout, "level": LOG_LEVEL}])
    logger.enable("vtasks")

except ImportError:
    # Set up basic logging as a fallback
    logging.basicConfig(level=LOG_LEVEL)
    logger = logging.getLogger("fallback_logger")


def get_logger(force_loguru=False):
    if force_loguru:
        return logger

    try:
        return get_run_logger()
    except MissingContextError:
        return logger
