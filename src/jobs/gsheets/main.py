from pathlib import Path

from pydantic import BaseModel

from common.logs import get_logger
from common.paths import read_yaml
from common.gsheets import read_gdrive_sheet
from common.duck import write_df

BASE_PATH = str(Path(__file__).parent)
TABLES_FILE = f"{BASE_PATH}/tables.yaml"


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


def export_tables():
    logger = get_logger()
    data = read_yaml(TABLES_FILE)
    tables = [GsheetJob(**x) for x in data]

    logger.info(f"Exporting {len(tables)} tables")

    for table in tables:
        export_table(table)
