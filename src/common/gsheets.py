import time

import backoff
import gspread
import pandas as pd
from gspread.exceptions import APIError

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


def retry_on_exception(max_tries=5):
    """Decorator to apply retry logic to a function with backoff."""

    logger = get_logger()

    def decorator(func):
        def log_failure(details):
            tries = details["tries"]
            exc = details["exception"]
            logger.warning(f"[Attempt {tries}] {exc=} when calling {func.__name__}")

        @backoff.on_exception(
            backoff.expo,
            (APIError, ConnectionError),
            max_tries=max_tries,
            on_backoff=log_failure,
        )
        def wrapped_func(*args, **kwargs):
            return func(*args, **kwargs)

        def wrapper(*args, **kwargs):
            try:
                return wrapped_func(*args, **kwargs)
            except (APIError, ConnectionError) as exc:
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


def read_gdrive_sheet(doc, sheet, with_index=False, max_tries=5):
    """Read data from a Google Drive sheet and return it as a DataFrame."""
    logger = get_logger()
    gsheet = _get_gdrive_sheet(doc, sheet, max_tries)

    logger.info(f"Reading data from 'gsheet://{doc}.{sheet}'")

    @retry_on_exception(max_tries=max_tries)
    def _fetch_data():
        time.sleep(1)  # Throttle requests
        return gsheet.get_all_records()

    data = _fetch_data()

    logger.info(f"Data read from 'gsheet://{doc}.{sheet}'")
    df = pd.DataFrame(data)

    if with_index:
        return df.set_index(df.columns[0])

    return df


def update_cell(doc, sheet, cell, value, max_tries=5):
    """Update a cell on google spreadsheet"""

    logger = get_logger()
    gsheet = _get_gdrive_sheet(doc, sheet, max_tries)

    logger.info(f"Updating {cell=} in 'gsheet://{doc}.{sheet}'")

    @retry_on_exception(max_tries=max_tries)
    def _update_cell():
        gsheet.update(cell, [[value]])

    _update_cell()

    logger.info(f"{cell=} successfully updated in 'gsheet://{doc}.{sheet}'")


def _get_coordinates(df):
    """Get gdrive coordinates as a pandas dataframe"""

    df_index = df.copy()

    def index_to_letter(x):
        """Get column letter (Chr(65) = 'A')"""
        return chr(65 + x + 1)

    n_rows = df_index.shape[0]
    numbers = pd.Series([str(x + 2) for x in range(n_rows)], index=df_index.index)

    for i, col in enumerate(df_index.columns):
        df_index[col] = index_to_letter(i) + numbers

    return df_index


def _get_columns(df, columns=None):
    """If columns is None, use them all"""

    # If no columns are passed, update them all
    if columns is None:
        columns = df.columns.tolist()

    # Make sure columns is a list
    if not isinstance(columns, list):
        columns = [columns]

    return columns


def _get_range_to_update(df, mfilter, columns):
    """Gets the range that needs to be updated"""

    # Extract range from coordinates and filter
    coordinates = _get_coordinates(df).loc[mfilter, columns]

    if isinstance(coordinates, pd.Series):
        return f"{coordinates.iloc[0]}:{coordinates.iloc[-1]}"

    return f"{coordinates.iloc[0, 0]}:{coordinates.iloc[-1, -1]}"


def _get_values_to_update(df, mfilter, columns):
    # Filter data to be updated
    values = df.loc[mfilter, columns].values.tolist()

    # Make sure that values is a list of lists
    if not isinstance(values[0], list):
        values = [values]

    return values


def df_to_gspread(doc, sheet, df, mfilter, cols=None, max_tries=5):
    """Update a google spreadsheet based on a pandas dataframe row"""

    logger = get_logger()

    gsheet = _get_gdrive_sheet(doc, sheet, max_tries)

    logger.info(f"Preparing update for 'gsheet://{doc}.{sheet}' ({mfilter=}, {cols=})")

    columns = _get_columns(df, cols)
    mrange = _get_range_to_update(df, mfilter, columns)
    values = _get_values_to_update(df, mfilter, columns)

    # Update values in gspreadsheet
    logger.info(f"Updating 'gsheet://{doc}.{sheet}' ({mrange=})")

    @retry_on_exception(max_tries=max_tries)
    def _update_cells():
        gsheet.update(mrange, values)

    _update_cells()

    logger.info(f"{mrange=} successfully updated in 'gsheet://{doc}.{sheet}'")
