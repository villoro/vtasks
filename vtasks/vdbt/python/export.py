import json

import pandas as pd
from prefect import get_run_logger

from vtasks.common.duck import write_df
from vtasks.common.paths import FILE_DUCKDB_DBT_METADATA
from vtasks.vdbt.python import paths

DATABASE = "raw__dbt"

TABLE_EXECUTION = "dbt_executions"
TABLE_RUN_RESULTS = "dbt_run_results"
TABLE_MODELS = "dbt_models"


COLS_MANIFEST = [
    "database",
    "schema",
    "name",
    "resource_type",
    "path",
    "unique_id",
    "fqn",
    "alias",
    "tags",
    "raw_code",
    "description",
    "sources",
    "depends_on",
    # "refs", # It's giving problems and I don't know why
    "config",
    "unrendered_config",
    "compiled_path",
    "compiled",
    "compiled_code",
    "meta",
]

KWARGS_WRITE = {
    "mode": "append",
    "as_str": True,
    "filename": FILE_DUCKDB_DBT_METADATA,
}


def read_manifest():
    with open(paths.FILE_MANIFEST) as stream:
        return json.load(stream)


def export_models():
    logger = get_run_logger()
    logger.info(f"Exporting '{TABLE_MODELS}' from {paths.FILE_MANIFEST=}")

    manifest = read_manifest()

    df = pd.DataFrame(manifest.get("nodes", {})).T
    df = df.loc[df["resource_type"] == "model", COLS_MANIFEST]

    # Cast to string problematic columns
    for x in ["config", "unrendered_config"]:
        df[x] = df[x].apply(str)

    write_df(df, DATABASE, TABLE_MODELS, **KWARGS_WRITE)


def export_execution(data):
    logger = get_run_logger()

    logger.info(f"Exporting '{TABLE_EXECUTION}'")
    df = pd.DataFrame([{**data["metadata"], **data["args"]}])
    df["elapsed_time"] = data["elapsed_time"]

    # Drop some columns and force 'string' in some others
    df = df.drop(columns=["env", "warn_error_options"])
    write_df(df, DATABASE, TABLE_EXECUTION, **KWARGS_WRITE)


def export_run_results(data):
    logger = get_run_logger()

    logger.info(f"Exporting '{TABLE_RUN_RESULTS}'")
    df = pd.DataFrame(data["results"])
    df["invocation_id"] = data["metadata"]["invocation_id"]

    if "compiled_code" in df.columns:
        df["compiled_code"] = df["compiled_code"].str.strip()

    write_df(df, DATABASE, TABLE_RUN_RESULTS, **KWARGS_WRITE)


def export_execution_and_run_results():
    logger = get_run_logger()

    logger.info("Running 'run_results' export")

    with open(paths.FILE_RUN_RESULTS) as stream:
        data = json.load(stream)

    export_execution(data)
    export_run_results(data)
