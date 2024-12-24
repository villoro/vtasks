import backoff
import gspread
import pandas as pd
from common.logs import get_logger
from common.paths import get_path
from common.secrets import export_secret

PATH_GSPREADSHEET_KEY = get_path("auth/gspreadsheets.json")
SECRET_NAME = "GSPREADSHEET_JSON"

GDRIVE = None


def init_gdrive(force=False):
    """Export gdrive json auth"""

    logger = get_logger()

    global GDRIVE
    if GDRIVE is None or force:
        logger.info(f"Initializing Gdrive ({PATH_GSPREADSHEET_KEY=}, {SECRET_NAME=})")
        export_secret(PATH_GSPREADSHEET_KEY, SECRET_NAME)

        GDRIVE = gspread.service_account(filename=PATH_GSPREADSHEET_KEY)


def _get_gdrive_sheet(doc, sheet, max_tries=5):
    """
    Get a Google Drive spreadsheet.

    Args:
        doc:    name of the document
        sheet:  name of the sheet inside the document
    """

    logger = get_logger()
    init_gdrive()

    logger.info(f"Getting google spreadsheet '{doc}.{sheet}'")

    def log_failure(details):
        tries = details["tries"]
        exception = details["exception"]
        logger.warning(
            f"[Attempt {tries}] " f"{exception=} when trying to get '{doc}.{sheet}'"
        )

    @backoff.on_exception(
        backoff.expo, ConnectionError, max_tries=max_tries, on_backoff=log_failure
    )
    def fetch_sheet():
        spreadsheet = GDRIVE.open(doc)
        return spreadsheet.worksheet(sheet)

    try:
        return fetch_sheet()
    except ConnectionError as e:
        msg_error = f"Too many attempts when getting '{doc}.{sheet}' {max_tries=}"
        logger.error(msg_error)
        raise e


def read_gdrive_sheet(doc, sheet):
    logger = get_logger()
    gsheet = _get_gdrive_sheet(doc, sheet)

    logger.info(f"Reading data from google spreadsheet '{doc}.{sheet}'")
    data = gsheet.get_all_records()

    return pd.DataFrame(data)
