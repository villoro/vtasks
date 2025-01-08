from pathlib import Path

from prefect import flow
from prefect import task
from pydantic import BaseModel

from src.common.duck import write_df
from src.common.gsheets import read_gdrive_sheet
from src.common.logs import get_logger
from src.common.paths import read_yaml

BASE_PATH = str(Path(__file__).parent)
TABLES_FILE = f"{BASE_PATH}/tables.yaml"
FLOW_NAME = "vtasks.gsheets.export_tables"


class GsheetJob(BaseModel):
    doc_in: str
    sheet_in: str
    schema_out: str = "raw__gsheets"
    table_out: str
    mode: str = "overwrite"


def export_table(table):
    df = read_gdrive_sheet(table.doc_in, table.sheet_in)
    df["_source"] = f"gsheet.{table.doc_in}.{table.sheet_in}"
    write_df(df, table.schema_out, table.table_out, mode=table.mode)


@flow(name=FLOW_NAME)
def export_gsheets_tables():
    logger = get_logger()
    data = read_yaml(TABLES_FILE)
    tables = [GsheetJob(**x) for x in data]

    logger.info(f"Exporting {len(tables)} tables")

    for table in tables:
        task(name=f"{FLOW_NAME}.{table.table_out}")(export_table)(table)
