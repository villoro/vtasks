from pathlib import Path

# The number of '.parent' must match the level where this file is placed in Docker
PATH_DBT = str(Path(__file__).parent.parent)

PATH_TARGET = f"{PATH_DBT}/target"

FILE_DBT_PROJECT = f"{PATH_DBT}/dbt_project.yml"
FILE_PROFILES = f"{PATH_DBT}/profiles.yml"

FILE_RUN_RESULTS = f"{PATH_TARGET}/run_results.json"
FILE_MANIFEST = f"{PATH_TARGET}/manifest.json"
