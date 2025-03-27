from typing import Literal

from prefect import flow

from vtasks.common import duck
from vtasks.common.logs import get_logger
from vtasks.common.paths import infer_environment
from vtasks.vdbt.python import export


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

    env = infer_environment()
    if env != "nas":
        logger.warning(f"Skipping upload to motherduck since we are in {env=}")
        return False

    duck.sync_duckdb(
        src=duck.FILE_DUCKDB_DBT,
        dest="motherduck",
        schema_prefixes=["_marts__", "_core__"],
        mode="overwrite",
    )


@flow(name="maintain.sync_dbt_metadata")
def sync_dbt_metadata():
    logger = get_logger()

    src = export.FILE_DUCKDB_RAW
    dest = duck.DEFAULT_FILE

    path_src = duck.get_duckdb_path(src, as_str=False)

    if path_src.exists():
        duck.sync_duckdb(src=src, dest=dest, schema_prefixes=["raw__"], mode="append")

        logger.info(f"Removing temporal duckdb {src=}")
        path_src.unlink()
        logger.info(f"Removed {src=}")

    else:
        logger.warning(f"{src=} doesn't exist, nothing to sync")


if __name__ == "__main__":
    sync_dbt_metadata()
