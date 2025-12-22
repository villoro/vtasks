from typing import Literal

from prefect import flow

from vtasks.common import duck
from vtasks.common import paths
from vtasks.common.logs import get_logger


@flow(name="maintain.sync_duckdb")
def sync_duckdb(
    src: str = "dbt",
    dest: str = "motherduck",
    schema_prefixes: str = ["_marts__", "_core__"],
    mode: Literal["append", "overwrite"] = "overwrite",
):
    """
    Sync tables between two DuckDB sources (MotherDuck or a local file).

    Args:
        src (str): Source database. Either "motherduck" or a DuckDB file path.
        dest (str): Destination database. Either "motherduck" or a DuckDB file path.
        schema_prefix (str): Only copy schemas that start with this prefix (default: "raw__").
        mode (str): Sync mode, either "append" or "overwrite" (default: "overwrite").
    """

    duck.sync_duckdb(src=src, dest=dest, schema_prefix=schema_prefixes, mode=mode)


@flow(name="maintain.upload_marts_to_md")
def upload_marts_to_md():
    logger = get_logger()

    if not paths.is_pro():
        logger.warning(f"Skipping upload to motherduck since we are in {paths.ENV=}")
        return False

    duck.sync_duckdb(
        src=paths.FILE_DUCKDB_DBT,
        dest="motherduck",
        schema_prefixes=["_marts__", "_core__"],
        mode="overwrite",
    )
