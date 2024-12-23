import duckdb

from datetime import datetime

from common.secrets import read_secret
from common.texts import remove_extra_spacing
from common.logs import get_logger

CON = None
DB_DUCKDB = "md:villoro"
SECRET_MD = "MOTHERDUCK_TOKEN"


def init_duckdb():
    """Connect to duckdb"""

    logger = get_logger()

    global CON
    if CON is None:
        logger.info(f"Connecting to {DB_DUCKDB=}")
        CON = duckdb.connect(DB_DUCKDB)

    return CON


def query_md(query, silent=False):
    con = init_duckdb()
    logger = get_logger()

    if not silent:
        logger.info(f"Querying motherduck query='{remove_extra_spacing(query)}'")

    return con.execute(query)


def table_exists(schema, table):
    """
    Check if a table exists in a DuckDB/MotherDuck database.

    Args:
        schema: Schema name.
        table: Table name.

    Returns:
        bool: True if the table exists, False otherwise.
    """

    df_tables = query_md(f"SHOW ALL TABLES", silent=True).df()
    table_names = (df_tables["schema"] + "." + df_tables["name"]).values
    return f"{schema}.{table}" in table_names


def _merge_table(df_input, schema, table, pk):
    con = init_duckdb()
    logger = get_logger()

    if not pk:
        raise ValueError("Primary key (pk) must be provided for merge mode")

    table_name = f"{schema}.{table}"
    con.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx__{table}__{pk} ON {table_name} ({pk})")

    logger.info(f"Merging data into {table_name=} using {pk=}")

    temp_table_name = f"_temp_{table}"
    logger.info(f"Creating temporal table '{temp_table_name}'")
    con.execute(f"CREATE TEMPORARY TABLE {temp_table_name} AS SELECT * FROM df_input")

    cols = [f"{x}=EXCLUDED.{x}" for x in df_input.columns if x not in [pk, "_n_updates"]]
    merge_query = f"""
    INSERT INTO {table_name}
    SELECT * FROM {temp_table_name}
    ON CONFLICT ({pk}) DO UPDATE SET
      _n_updates = {table_name}._n_updates + 1,
      {', '.join(cols)}
    """
    con.execute(merge_query)
    con.execute(f"DROP TABLE IF EXISTS {temp_table_name}")


def write_df(df_input, schema, table, mode="overwrite", pk=None):
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
    df_md["_exported_at"] = datetime.now()
    df_md["_n_updates"] = 0

    table_name = f"{schema}.{table}"
    logger.info(f"Writting {len(df_input)} rows to {table_name=} ({mode=})")

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    if mode == "overwrite":
        logger.info(f"Overwriting {table_name=}")
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_md")

    elif mode == "append":
        logger.info(f"Appending data to {table_name=}")
        con.execute(f"INSERT INTO {table_name} SELECT * FROM df_md")

    elif mode == "merge":
        if table_exists(schema, table):
            _merge_table(df_md, schema, table, pk)

        else:
            logger.info(f"Creating {table_name=} since it doesn't exist")
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_md")

    else:
        raise ValueError(f"Unsupported {mode=}")
