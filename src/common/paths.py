from pathlib import Path

import yaml
from common.logs import get_logger

# Base path of the repo.
# It needs to go 3 times up since this file has the following relative path:
#   /src/common/paths.py
PATH_ROOT = Path(__file__).parent.parent.parent


def get_path(path_relative):
    """Returns absolute path using PATH_ROOT"""

    path_out = PATH_ROOT

    for x in path_relative.split("/"):
        path_out /= x

    return str(path_out)


def read_yaml(filename, encoding="utf8", silent=False):
    """Read a yaml file"""

    logger = get_logger()
    if not silent:
        logger.info(f"Reading {filename=} with {encoding=}")

    with open(filename, "r", encoding=encoding) as stream:
        return yaml.safe_load(stream)
