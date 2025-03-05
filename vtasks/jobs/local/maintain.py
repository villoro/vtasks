from typing import Literal

from prefect import flow

from vtasks.common import duck
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
    duck.sync_duckdb(src=export.DUCKDB_FILE, dest=duck.DEFAULT_FILE, mode="append")
