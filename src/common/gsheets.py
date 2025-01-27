import backoff
import gspread
import pandas as pd

from src.common.logs import get_logger
from src.common.paths import get_path
from src.common.secrets import export_secret

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


def _retry_on_exception(
    func, max_tries=5, exception=ConnectionError, logger=None, final_log_message=None
):
    """
    Retry logic for a function call with backoff.

    Args:
        func: Callable to retry
        max_tries: Maximum number of retry attempts
        exception: Exception to trigger retries
        logger: Logger instance for logging (optional)
        final_log_message: Message to log when retries are exhausted (optional)
    """

    def log_failure(details):
        tries = details["tries"]
        exc = details["exception"]
        if logger:
            logger.warning(f"[Attempt {tries}] {exc=} when calling {func.__name__}")

    @backoff.on_exception(
        backoff.expo, exception, max_tries=max_tries, on_backoff=log_failure
    )
    def wrapped_func(*args, **kwargs):
        return func(*args, **kwargs)

    def call_with_logging(*args, **kwargs):
        try:
            return wrapped_func(*args, **kwargs)
        except exception as exc:
            if logger and final_log_message:
                logger.error(final_log_message.format(exc=exc, max_tries=max_tries))
            raise exc

    return call_with_logging


def _get_gdrive_sheet(doc, sheet, max_tries=5):
    """
    Get a Google Drive spreadsheet.

    Args:
        doc:    name of the document
        sheet:  name of the sheet inside the document
    """
    logger = get_logger()
    init_gdrive()

    logger.info(f"Getting Google spreadsheet '{doc}.{sheet}'")

    def fetch_sheet():
        spreadsheet = GDRIVE.open(doc)
        return spreadsheet.worksheet(sheet)

    final_message = f"Too many attempts when getting '{doc}.{sheet}' {max_tries=}"
    fetch_sheet = _retry_on_exception(
        fetch_sheet, max_tries, logger=logger, final_log_message=final_message
    )

    return fetch_sheet()


def read_gdrive_sheet(doc, sheet, max_tries=5):
    """
    Read data from a Google Drive sheet and return it as a DataFrame.

    Args:
        doc:    name of the document
        sheet:  name of the sheet inside the document
    """
    logger = get_logger()
    gsheet = _get_gdrive_sheet(doc, sheet, max_tries)

    logger.info(f"Reading data from 'gsheet.{doc}.{sheet}'")

    def fetch_data():
        return gsheet.get_all_records()

    final_message = (
        f"Too many attempts when reading 'gsheet.{doc}.{sheet}' {max_tries=}"
    )
    fetch_data = _retry_on_exception(
        fetch_data, max_tries, logger=logger, final_log_message=final_message
    )

    data = fetch_data()

    logger.info(f"Data read from 'gsheet.{doc}.{sheet}'")
    return pd.DataFrame(data)
