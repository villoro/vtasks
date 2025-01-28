import json
from datetime import datetime

import pandas as pd
import paths
from prefect import get_run_logger

from src.common.duck import write_df

TAGS = {}

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
    "deferred",
    "meta",
]


def export_table(df, table, dtypes):
    logger = get_run_logger()

    n_records = df.shape[0]
    logger.info(f"Writing table to '{DATABASE}.{table}' ({n_records=})")

    df["p_extracted_at"] = datetime.now()

    # Mixed types will likely fail, logging them for easier fixing
    for column in df.columns:
        types = df[column].apply(type).value_counts().to_dict()
        if len(types) > 1:
            logger.debug(f"{column=} has mixed {types=}")

    write_df(df, DATABASE, table, mode="append")


def read_manifest():
    with open(paths.FILE_MANIFEST) as stream:
        return json.load(stream)


def export_models():
    logger = get_run_logger()
    logger.info(f"Exporting '{TABLE_MODELS}' from {paths.FILE_MANIFEST=}")

    manifest = read_manifest()

    df = pd.DataFrame(manifest.get("nodes", {})).T
    df = df.loc[df["resource_type"] == "model", COLS_MANIFEST]

    # Extract owner
    df["owner"] = df["meta"].apply(lambda x: x.get("owner", None))

    # Cast to string problematic columns
    for x in ["config", "unrendered_config"]:
        df[x] = df[x].apply(str)

    export_table(df, TABLE_MODELS, {})


def export_execution(data, flow_run_id):
    logger = get_run_logger()

    logger.info(f"Exporting '{TABLE_EXECUTION}'")
    df = pd.DataFrame([{**TAGS, **data["metadata"], **data["args"]}])
    df["elapsed_time"] = data["elapsed_time"]
    df["flow_run_id"] = flow_run_id

    # Drop some columns and force 'string' in some others
    df = df.drop(columns=["env", "warn_error_options"])
    dtypes = {
        "select": "string",
        "exclude": "string",
        "vars": "string",
        "resource_types": "string",
        "flow_run_id": "string",
    }
    export_table(df, TABLE_EXECUTION, dtypes)


def export_run_results(data):
    logger = get_run_logger()

    logger.info(f"Exporting '{TABLE_RUN_RESULTS}'")
    df = pd.DataFrame(data["results"])
    df["invocation_id"] = data["metadata"]["invocation_id"]

    if "compiled_code" in df.columns:
        df["compiled_code"] = df["compiled_code"].str.strip()

    # Force 'string' type in some columns
    dtypes = {"message": "string", "failures": "int", "adapter_response": "string"}
    export_table(df, TABLE_RUN_RESULTS, dtypes)


def export_execution_and_run_results(flow_run_id):
    logger = get_run_logger()

    logger.info("Running 'run_results' export")

    with open(paths.FILE_RUN_RESULTS) as stream:
        data = json.load(stream)

    export_execution(data, flow_run_id)
    export_run_results(data)
