from datetime import datetime
from typing import Literal

import duckdb

from vtasks.common.logs import get_logger
from vtasks.common.paths import get_duckdb_path
from vtasks.common.secrets import read_secret
from vtasks.common.texts import remove_extra_spacing

DB_DUCKDB_MD = "md:villoro?motherduck_token={token}"
SECRET_MD = "MOTHERDUCK_TOKEN"
DEFAULT_FILE = "raw"


def get_duckdb(use_md=False, filename=None):
    logger = get_logger()
    if use_md:
        logger.debug("Connecting to MotherDuck")
        token = read_secret(SECRET_MD)
        db_path = DB_DUCKDB_MD.format(token=token)
    else:
        db_path = get_duckdb_path(filename or DEFAULT_FILE)
        logger.debug(f"Connecting to local DuckDB at {db_path=}")

    return duckdb.connect(db_path)


def query_ddb(query, df_duck=None, silent=False, use_md=False, con=None, filename=None):
    logger = get_logger()
    log_func = logger.debug if silent else logger.info

    if df_duck is not None:
        logger.debug(f"Using the variable `df_duck` for the query {df_duck.shape=}")

    log_func(f"Querying duckdb ({use_md=}) query='{remove_extra_spacing(query)}'")

    if con is not None:
        return con.execute(query)

    with get_duckdb(use_md, filename) as con:
        return con.execute(query)


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
    df_tables = query_ddb("SHOW ALL TABLES", silent=True, use_md=use_md, con=con).df()
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
    query_ddb(query, **kwargs)

    logger.info(f"Merging data into {table_name=} using {pk=}")

    temp_table_name = f"_temp_{table}"
    logger.info(f"Creating temporal table '{temp_table_name}'")
    query = (
        f"CREATE OR REPLACE TEMPORARY TABLE {temp_table_name} AS SELECT * FROM df_duck"
    )
    query_ddb(query, df_input, **kwargs)

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
    query_ddb(merge_query, **kwargs)

    logger.info(f"Droping '{temp_table_name}'")
    query_ddb(f"DROP TABLE IF EXISTS {temp_table_name}", **kwargs)


def write_df(
    df_input,
    schema,
    table,
    mode="overwrite",
    pk=None,
    as_str=False,
    use_md=False,
    filename=None,
):
    """
    Write a DataFrame to a DuckDB table with flexible modes.
    """

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
            query_ddb(query, df_duck, **kwargs)
            return True

        if mode == "overwrite":
            logger.info(f"Overwriting {table_name=}")
            query = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_duck"
            query_ddb(query, df_duck, **kwargs)

        elif mode == "append":
            logger.info(f"Appending data to {table_name=}")
            query = f"INSERT INTO {table_name} SELECT * FROM df_duck"
            query_ddb(query, df_duck, **kwargs)

        elif mode == "merge":
            _merge_table(df_duck, schema, table, pk, **kwargs)

        else:
            raise ValueError(f"Unsupported {mode=}")

    return True


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

    logger = get_logger()
    logger.info(f"Starting sync from {src} → {dest} ({mode=}, {schema_prefix=})")

    src_use_md = src == "motherduck"
    dest_use_md = dest == "motherduck"
    src_filename = None if src_use_md else src
    dest_filename = None if dest_use_md else dest

    # Source and destination connections
    with get_duckdb(use_md=src_use_md, filename=src_filename) as src_con:
        df_tables = src_con.execute("SHOW ALL TABLES").df()

        for _, row in df_tables.iterrows():
            schema, table = row["schema"], row["name"]
            if not schema.startswith(schema_prefix):
                continue  # Skip schemas that don't match the prefix

            full_table_name = f"{schema}.{table}"
            logger.info(f"Syncing {full_table_name=}")

            # Read column names from the source
            query = f"DESCRIBE {full_table_name}"
            col_names = [f'"{x[0]}"' for x in src_con.execute(query).fetchall()]
            col_list = ", ".join(col_names)

            # Read data from source DuckDB
            query = f"SELECT {col_list} FROM {full_table_name}"
            df_duck = src_con.execute(query).df()

            with get_duckdb(use_md=dest_use_md, filename=dest_filename) as dest_con:
                dest_con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

                if mode == "overwrite":
                    logger.info(f"Overwriting {full_table_name=}")
                    dest_con.execute(
                        f"CREATE OR REPLACE TABLE {full_table_name} AS SELECT * FROM df_duck"
                    )
                else:
                    logger.info(f"Appending {len(df_duck)} rows to {full_table_name=}")
                    dest_con.execute(
                        f"INSERT INTO {full_table_name} ({col_list}) SELECT {col_list} FROM df_duck"
                    )

            logger.info(f"Copied table {full_table_name=} successfully")

    logger.info("DuckDB sync completed successfully")
