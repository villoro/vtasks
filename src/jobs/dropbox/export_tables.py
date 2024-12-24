from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional

from common.dropbox import get_vdropbox
from common.duck import write_df
from common.logs import get_logger
from common.paths import read_yaml
from prefect import flow
from prefect import task
from pydantic import BaseModel

BASE_PATH = str(Path(__file__).parent)
TABLES_FILE = f"{BASE_PATH}/tables.yaml"
FLOW_NAME = "vtasks.dropbox.export_tables"


class Column(BaseModel):
    name_in: str
    name_out: str


class DropboxJob(BaseModel):
    path_in: str
    format_in: Literal["csv", "parquet"]
    kwargs_in: Optional[Dict[str, Any]]
    columns: Optional[List[Column]]
    schema_out: str = "raw__dropbox"
    table_out: str
    mode: Literal["append", "overwrite", "merge"]
    pk: str = None
    drop_source: bool = False


def export_table(vdp, table):
    logger = get_logger()

    if not vdp.file_exists(table.path_in):
        logger.warning(f"{table.path_in=} doesn't exist, export skipped")
        return False

    if table.format_in == "csv":
        df = vdp.read_csv(table.path_in, **table.kwargs_in)
    elif table.format_in == "parquet":
        df = vdp.read_parquet(table.path_in, **table.kwargs_in)

    if table.columns:
        cols = {x.name_in: x.name_out for x in table.columns}
        logger.info(f"Renaming columns based on {cols=}")
        df.columns = df.columns.astype(str)
        df = df.rename(columns=cols)[cols.values()]

    df["_source"] = f"dropbox:/{table.path_in}"
    write_df(df, table.schema_out, table.table_out, mode=table.mode, pk=table.pk)

    if table.drop_source:
        logger.info(f"Deleting {table.path_in=} since {table.drop_source=}")
        vdp.delete(table.path_in)

    return True


@flow(name=FLOW_NAME)
def export_tables():
    logger = get_logger()
    data = read_yaml(TABLES_FILE)
    tables = [DropboxJob(**x) for x in data]

    vdp = get_vdropbox()

    logger.info(f"Exporting {len(tables)} tables")

    for table in tables:
        task(name=f"{FLOW_NAME}.{table.table_out}")(export_table)(vdp, table)
