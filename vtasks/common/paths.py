import os
import platform
from pathlib import Path

import yaml

from vtasks.common.logs import get_logger

# Base path of the repo.
# It needs to go 3 times up since this file has the following relative path:
#   /vtasks/common/paths.py
PATH_ROOT = Path(__file__).parent.parent.parent

PATH_DATA_NAS = Path("/mnt/duckdb")
FOLDER_DUCKDB_LOCAL = ".duckdb"

FILE_DUCKDB_RAW = "raw"
FILE_DUCKDB_DBT = "dbt"
FILE_DUCKDB_METABASE = "metabase"


def infer_environment():
    """Detects whether the script is running on GitHub Actions, locally, or on the NAS."""
    if "GITHUB_ACTIONS" in os.environ:
        return "github"
    elif platform.system() == "Windows":
        return "local"
    elif PATH_DATA_NAS.exists():
        return "nas"
    else:
        return "unknown"


# Needed for storing some tokens
PATH_AUTH = PATH_ROOT / ".auth"
PATH_AUTH.mkdir(parents=True, exist_ok=True)


def get_path(path_relative):
    """Returns absolute path using PATH_ROOT"""

    path_out = PATH_ROOT

    for x in path_relative.split("/"):
        path_out /= x

    return str(path_out)


def get_duckdb_path(db_name, as_str=True):
    """Returns the correct DuckDB file path based on the environment."""
    env = infer_environment()

    if env == "github" or env == "local":
        duckdb_dir = PATH_ROOT / FOLDER_DUCKDB_LOCAL
    elif env == "nas":
        duckdb_dir = PATH_DATA_NAS
    else:
        raise RuntimeError(
            "Environment not recognized, unable to determine DuckDB path."
        )

    # Ensure directory exists for local testing
    if env in ["github", "local"]:
        duckdb_dir.mkdir(parents=True, exist_ok=True)

    if "." in db_name:
        db_name = db_name.split(".")[0]

    out = duckdb_dir / f"{db_name}.duckdb"

    if not as_str:
        return out

    return str(out).replace("\\", "/")


def read_yaml(filename, encoding="utf8", silent=False):
    """Read a yaml file"""

    logger = get_logger()
    if not silent:
        logger.info(f"Reading {filename=} with {encoding=}")

    with open(filename, "r", encoding=encoding) as stream:
        return yaml.safe_load(stream)
