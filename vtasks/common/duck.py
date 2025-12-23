from datetime import datetime
from typing import Literal

import duckdb

from vtasks.common import paths
from vtasks.common.logs import get_logger
from vtasks.common.secrets import read_secret
from vtasks.common.texts import remove_extra_spacing

DB_DUCKDB_MD = "md:villoro?motherduck_token={token}"
SECRET_MD = "MOTHERDUCK_TOKEN"

DB_PATH = None


def get_duckdb(use_md=False, filename=None):
    logger = get_logger()

    global DB_PATH
    if DB_PATH is not None:
        logger.debug(f"Reusing {DB_PATH=}")
        return duckdb.connect(DB_PATH)

    if (is_pro := paths.is_pro()) or use_md:
        logger.info(f"Connecting to MotherDuck since {is_pro=} or {use_md=}")
        token = read_secret(SECRET_MD)
        DB_PATH = DB_DUCKDB_MD.format(token=token)
    else:
        DB_PATH = paths.get_duckdb_path(filename or paths.FILE_DUCKDB)
        logger.info(f"Connecting to local DuckDB at {DB_PATH=}")

    return duckdb.connect(DB_PATH)


def run_query(query, df_duck=None, silent=False, use_md=False, con=None, filename=None):
    logger = get_logger()
    log_func = logger.debug if silent else logger.info

    if df_duck is not None:
        logger.debug(f"Using the variable `df_duck` for the query {df_duck.shape=}")

    log_func(f"Querying duckdb query='{remove_extra_spacing(query)} ({use_md=})'")

    if con is not None:
        return con.execute(query)

    with get_duckdb(use_md, filename) as con:
        return con.execute(query)


def read_query(query, silent=False, use_md=False, con=None, filename=None):
    logger = get_logger()
    log_func = logger.debug if silent else logger.info

    log_func(f"Reading from duckdb query='{remove_extra_spacing(query)}' ({use_md=})")

    if con is not None:
        df = con.sql(query).df()

    else:
        with get_duckdb(use_md, filename) as con:
            df = con.sql(query).df()

    log_func(f"{len(df)} rows read from query='{remove_extra_spacing(query)}'")
    return df


def table_exists(schema, table, silent=False, use_md=False, con=None):
    """
    Check if a table exists in a DuckDB/MotherDuck database.

    Args:
        schema: Schema name.
        table: Table name.

    Returns:
        bool: True if the table exists, False otherwise.
    """

    logger = get_logger()
    log_func = logger.debug if silent else logger.info

    log_func(f"Checking if '{schema}.{table}' exists")
    df_tables = read_query("SHOW ALL TABLES", silent=True, use_md=use_md, con=con)
    table_names = (df_tables["schema"] + "." + df_tables["name"]).values
    out = f"{schema}.{table}" in table_names

    log_func(f"'{schema}.{table}' exists={out}")
    return out


def _merge_table(df_input, schema, table, pk, silent=True, use_md=False, con=None):
    logger = get_logger()

    if not pk:
        raise ValueError("Primary key (pk) must be provided for merge mode")

    kwargs = {"silent": silent, "use_md": use_md, "con": con}

    table_name = f"{schema}.{table}"
    query = (
        f"CREATE UNIQUE INDEX IF NOT EXISTS idx__{table}__{pk} ON {table_name} ({pk})"
    )
    run_query(query, **kwargs)

    logger.info(f"Merging data into {table_name=} using {pk=}")

    temp_table_name = f"_temp_{table}"
    logger.info(f"Creating temporal table '{temp_table_name}'")
    query = (
        f"CREATE OR REPLACE TEMPORARY TABLE {temp_table_name} AS SELECT * FROM df_duck"
    )
    run_query(query, df_input, **kwargs)

    cols = [
        f"{x}=EXCLUDED.{x}" for x in df_input.columns if x not in [pk, "_n_updates"]
    ]
    merge_query = f"""
    INSERT INTO {table_name}
    SELECT * FROM {temp_table_name}
    ON CONFLICT ({pk}) DO UPDATE SET
      _n_updates = {table_name}._n_updates + 1,
      {", ".join(cols)}
    """
    logger.info(f"Merging '{temp_table_name}' into '{table_name}'")
    run_query(merge_query, **kwargs)

    logger.info(f"Droping '{temp_table_name}'")
    run_query(f"DROP TABLE IF EXISTS {temp_table_name}", **kwargs)


def write_df(
    df_input,
    schema,
    table,
    mode: Literal["append", "overwrite"] = "overwrite",
    pk=None,
    as_str=False,
    use_md=False,
    filename=None,
):
    """Write a DataFrame to a DuckDB table with flexible modes"""

    logger = get_logger()

    df_duck = df_input.copy()

    if as_str:
        logger.debug("Casting all columns to string")
        df_duck = df_duck.astype(str)

    df_duck["_exported_at"] = datetime.now()
    df_duck["_n_updates"] = 0

    table_name = f"{schema}.{table}"
    logger.info(
        f"Writting {len(df_input)} rows to {table_name=} ({mode=}, {use_md=}, {filename=})"
    )

    with get_duckdb(use_md, filename) as con:
        kwargs = {"silent": True, "use_md": use_md, "con": con}
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        if not table_exists(schema, table, **kwargs):
            logger.info(f"Creating {table_name=} since it doesn't exist")
            query = f"CREATE TABLE {table_name} AS SELECT * FROM df_duck"
            run_query(query, df_duck, **kwargs)
            return True

        if mode == "overwrite":
            logger.info(f"Overwriting {table_name=}")
            query = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_duck"
            run_query(query, df_duck, **kwargs)

        elif mode == "append":
            logger.info(f"Appending data to {table_name=}")
            query = f"INSERT INTO {table_name} SELECT * FROM df_duck"
            run_query(query, df_duck, **kwargs)

        elif mode == "merge":
            _merge_table(df_duck, schema, table, pk, **kwargs)

        else:
            raise ValueError(f"Unsupported {mode=}")

    return True


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
        schema_prefixes (List[str]): Only copy schemas that start with any of those prefixes.
        mode (str): Sync mode, either "append" or "overwrite".
    """

    logger = get_logger()
    logger.info(f"Starting sync from {src} → {dest} ({mode=}, {schema_prefixes=})")

    use_md_src = src == "motherduck"
    use_md_dest = dest == "motherduck"
    filename_src = None if use_md_src else src
    filename_dest = None if use_md_dest else dest

    logger.info(
        f"Config({use_md_src=}, {use_md_dest=}, {filename_src=}, {filename_dest=})"
    )

    # Source and destination connections
    with get_duckdb(use_md=use_md_src, filename=filename_src) as con_src:
        df_tables = read_query("SHOW ALL TABLES", con=con_src)

        for _, row in df_tables.iterrows():
            schema, table = row["schema"], row["name"]
            if not any(schema.startswith(x) for x in schema_prefixes):
                logger.debug(
                    f"Skipping {schema=} since it doesn't match any of {schema_prefixes=}"
                )
                continue  # Skip schemas that don't match any of the prefixes

            # Copy tables
            df = read_query(f"SELECT * FROM {schema}.{table}", con=con_src)
            write_df(
                df, schema, table, mode, use_md=use_md_dest, filename=filename_dest
            )

    logger.info("DuckDB sync completed successfully")
