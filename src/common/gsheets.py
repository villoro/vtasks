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


def retry_on_exception(max_tries=5, exception=ConnectionError):
    """Decorator to apply retry logic to a function with backoff."""

    logger = get_logger()

    def decorator(func):
        def log_failure(details):
            tries = details["tries"]
            exc = details["exception"]
            logger.warning(f"[Attempt {tries}] {exc=} when calling {func.__name__}")

        @backoff.on_exception(
            backoff.expo, exception, max_tries=max_tries, on_backoff=log_failure
        )
        def wrapped_func(*args, **kwargs):
            return func(*args, **kwargs)

        def wrapper(*args, **kwargs):
            try:
                return wrapped_func(*args, **kwargs)
            except exception as exc:
                logger.error(f"Too many attempts for '{func.__name__}' ({max_tries=})")
                raise exc

        return wrapper

    return decorator


def _get_gdrive_sheet(doc, sheet, max_tries=5):
    """Get a Google Drive spreadsheet."""
    logger = get_logger()
    init_gdrive()

    logger.info(f"Getting Google spreadsheet '{doc}.{sheet}'")

    @retry_on_exception(max_tries=max_tries)
    def _fetch_sheet():
        spreadsheet = GDRIVE.open(doc)
        return spreadsheet.worksheet(sheet)

    return _fetch_sheet()


def read_gdrive_sheet(doc, sheet, max_tries=5):
    """Read data from a Google Drive sheet and return it as a DataFrame."""
    logger = get_logger()
    gsheet = _get_gdrive_sheet(doc, sheet, max_tries)

    logger.info(f"Reading data from 'gsheet://{doc}.{sheet}'")

    @retry_on_exception(max_tries=max_tries)
    def _fetch_data():
        return gsheet.get_all_records()

    data = _fetch_data()

    logger.info(f"Data read from 'gsheet://{doc}.{sheet}'")
    return pd.DataFrame(data)


def update_cell(doc, sheet, cell, value, max_tries=5):
    """Update a cell on google spreadsheet"""

    logger = get_logger()
    gsheet = _get_gdrive_sheet(doc, sheet, max_tries)

    logger.info(f"Updating {cell=} in 'gsheet://{doc}.{sheet}'")

    @retry_on_exception(max_tries=max_tries)
    def _update_cell():
        gsheet.update(cell, value)

    _update_cell()

    logger.info(f"{cell=} successfully updated in 'gsheet://{doc}.{sheet}'")
