from datetime import datetime

import duckdb

from vtasks.common.logs import get_logger
from vtasks.common.paths import get_duckdb_path
from vtasks.common.secrets import read_secret
from vtasks.common.texts import remove_extra_spacing

CON = None
DB_DUCKDB_MD = "md:villoro?motherduck_token={token}"
SECRET_MD = "MOTHERDUCK_TOKEN"


def init_duckdb(use_motherduck=False):
    """
    Initialize a DuckDB connection, choosing between MotherDuck or a local file.

    :param use_motherduck: If True, always connect to MotherDuck.
                           If False, use local DuckDB based on environment.
    :return: A DuckDB connection.
    """
    logger = get_logger()
    global CON

    if CON is None:
        if use_motherduck:
            # Use MotherDuck (GitHub Actions default)
            token = read_secret(SECRET_MD)
            db_path = DB_DUCKDB_MD.format(token=token)
            logger.info("Connecting to MotherDuck")
        else:
            # Use local DuckDB file
            db_path = get_duckdb_path("raw")
            logger.info(f"Connecting to local DuckDB at {db_path}")

        CON = duckdb.connect(db_path)

    return CON


def query_md(query, df_md=None, silent=False):
    con = init_duckdb()
    logger = get_logger()
    log_func = logger.debug if silent else logger.info

    log_func(f"Querying motherduck query='{remove_extra_spacing(query)}'")
    return con.execute(query)


def table_exists(schema, table, silent=False):
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
    df_tables = query_md("SHOW ALL TABLES", silent=True).df()
    table_names = (df_tables["schema"] + "." + df_tables["name"]).values
    out = f"{schema}.{table}" in table_names

    log_func(f"'{schema}.{table}' exists={out}")
    return out


def _merge_table(df_input, schema, table, pk):
    logger = get_logger()

    if not pk:
        raise ValueError("Primary key (pk) must be provided for merge mode")

    table_name = f"{schema}.{table}"
    query = (
        f"CREATE UNIQUE INDEX IF NOT EXISTS idx__{table}__{pk} ON {table_name} ({pk})"
    )
    query_md(query, silent=True)

    logger.info(f"Merging data into {table_name=} using {pk=}")

    temp_table_name = f"_temp_{table}"
    logger.info(f"Creating temporal table '{temp_table_name}'")
    query = (
        f"CREATE OR REPLACE TEMPORARY TABLE {temp_table_name} AS SELECT * FROM df_md"
    )
    query_md(query, df_input, silent=True)

    cols = [
        f"{x}=EXCLUDED.{x}" for x in df_input.columns if x not in [pk, "_n_updates"]
    ]
    merge_query = f"""
    INSERT INTO {table_name}
    SELECT * FROM {temp_table_name}
    ON CONFLICT ({pk}) DO UPDATE SET
      _n_updates = {table_name}._n_updates + 1,
      {', '.join(cols)}
    """
    logger.info(f"Merging '{temp_table_name}' into '{table_name}'")
    query_md(merge_query, silent=True)

    logger.info(f"Droping '{temp_table_name}'")
    query_md(f"DROP TABLE IF EXISTS {temp_table_name}", silent=True)


def write_df(df_input, schema, table, mode="overwrite", pk=None, as_str=False):
    """
    Write a DataFrame to a DuckDB table with flexible modes.

    Args:
        df_input: DataFrame to upload
        schema: Schema name in DuckDB
        table: Table name in DuckDB
        mode: "overwrite", "append", or "merge"
        pk: For "merge", the column used as the primary key
    """
    con = init_duckdb()
    logger = get_logger()

    df_md = df_input.copy()

    if as_str:
        logger.debug("Casting all columns to string")
        df_md = df_md.astype(str)

    df_md["_exported_at"] = datetime.now()
    df_md["_n_updates"] = 0

    table_name = f"{schema}.{table}"
    logger.info(f"Writting {len(df_input)} rows to {table_name=} ({mode=})")

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    if not table_exists(schema, table, silent=True):
        logger.info(f"Creating {table_name=} since it doesn't exist")
        query = f"CREATE TABLE {table_name} AS SELECT * FROM df_md"
        query_md(query, df_md, silent=True)
        return True

    if mode == "overwrite":
        logger.info(f"Overwriting {table_name=}")
        query = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_md"
        query_md(query, df_md, silent=True)

    elif mode == "append":
        logger.info(f"Appending data to {table_name=}")
        query = f"INSERT INTO {table_name} SELECT * FROM df_md"
        query_md(query, df_md, silent=True)

    elif mode == "merge":
        _merge_table(df_md, schema, table, pk)

    else:
        raise ValueError(f"Unsupported {mode=}")

    return True
