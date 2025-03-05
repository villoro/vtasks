from typing import Literal

from prefect import flow

from vtasks.common import duck
from vtasks.common.logs import get_logger
from vtasks.vdbt.python import export


@flow(name="maintain.sync_duckdb")
def sync_duckdb(
    src: str = "motherduck",
    dest: str = "local",
    schema_prefix: str = "raw__",
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

    duck.sync_duckdb(src=src, dest=dest, schema_prefix=schema_prefix, mode=mode)


@flow(name="maintain.sync_dbt_metadata")
def sync_dbt_metadata():
    logger = get_logger()

    src = export.DUCKDB_FILE
    dest = duck.DEFAULT_FILE

    path_src = duck.get_duckdb_path(export.DUCKDB_FILE, as_str=False)

    if path_src.exists():
        duck.sync_duckdb(src=src, dest=dest, mode="append")

        logger.info(f"Removing temporal duckdb {src=}")
        path_src.unlink()
        logger.info(f"Removed {src=}")

    else:
        logger.warning(f"{src=} doesn't exist, nothing to sync")


if __name__ == "__main__":
    sync_dbt_metadata()
