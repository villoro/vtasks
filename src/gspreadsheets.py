import gspread
import pandas as pd

from requests.exceptions import ConnectionError
from time import sleep

from utils import PATH_ROOT
from utils import export_secret
from utils import get_secret
from utils import log

PATH_GDRIVE_KEY = f"{PATH_ROOT}/auth/gspreadsheets.json"

GDRIVE = None


def init_gdrive(force=False):
    """ Export gdrive json auth """

    # Init GDRIVE if it has not been init
    global GDRIVE
    if GDRIVE is None or force:

        export_secret(PATH_GDRIVE_KEY, "GSPREADSHEET_JSON")

        GDRIVE = gspread.service_account(filename=PATH_GDRIVE_KEY)


def get_gdrive_sheet(spreadsheet_name, sheet_name, retries=3):
    """
        Get a google drive spreadsheet

        Args:
            spreadsheet_name:   name of the document
            sheet_name:         name of the sheet inside the document
    """

    init_gdrive()

    msg_error = "ConnectionError ({}) when trying to get '{}/{}'. Details: {}"

    # Open sheet in a way we can have some retries
    for x in range(retries):
        try:
            # Get the spreadsheet
            spreadsheet = GDRIVE.open(spreadsheet_name)
            return spreadsheet.worksheet(sheet_name)

        except ConnectionError as e:
            log.warning(msg_error.format(x, spreadsheet_name, sheet_name, e))
            # Sleep to avoid query limitations
            sleep(x * 10)

            # Init gdrive again just in case
            init_gdrive(force=True)

    log.error(msg_error.format("last_attempt", spreadsheet_name, sheet_name, e))
    raise ValueError("Too many reading attemps in 'get_gdrive_sheet'")


def read_df_gdrive(spreadsheet_name, sheet_name, cols_to_numeric=[]):
    """
        Reads a google spreadsheet

        Args:
            spreadsheet_name:   name of the document
            sheet_name:         name of the sheet inside the document
            index_as_datetime:  wether to cast the index as datetime or not
            cols_to_numeric:    columns that must be transformed to numeric.
                                    if None all will be transformed
            fillna:             wether to fill NA with 0 or not
    """

    sheet = get_gdrive_sheet(spreadsheet_name, sheet_name)

    # Create dataframe
    data = sheet.get_all_records()
    df = pd.DataFrame(data)

    index_col = df.columns[0]

    if index_col == "Date":
        df[index_col] = pd.to_datetime(df[index_col])

    # Set first column as index
    df = df.set_index(index_col)

    if cols_to_numeric is None:
        return df

    if cols_to_numeric == "all":
        cols_to_numeric = df.columns

    # Cast cols to numeric
    for col in cols_to_numeric:

        # Get rid of unwanted symbols
        for find, replace in [(".", ""), (",", "."), (" €", "")]:
            df[col] = df[col].str.replace(find, replace, regex=False)

        # Transform to numeric
        df[col] = pd.to_numeric(df[col])

    return df


def get_coordinates(df):
    """ Get gdrive coordinates as a pandas dataframe """

    df_index = df.copy()

    # Get column letter (Chr(65) = 'A')
    index_to_letter = lambda x: chr(65 + x + 1)

    n_rows = df_index.shape[0]
    numbers = pd.Series([str(x + 2) for x in range(n_rows)], index=df_index.index)

    for i, col in enumerate(df_index.columns):
        df_index[col] = index_to_letter(i) + numbers

    return df_index


def df_to_gspread(spreadsheet_name, sheet_name, df, mfilter, columns=None):
    """
        Update a google spreadsheet based on a pandas dataframe row

        Args:
            spreadsheet_name:   name of the document
            sheet_name:         name of the sheet inside the document
            df:                 pandas dataframe
            mfilter:            rows that will be updated
            columns:            which columns to update
    """

    # Get worksheet
    wks = get_gdrive_sheet(spreadsheet_name, sheet_name)

    # If no columns are passed, update them all
    if columns is None:
        columns = df.columns.tolist()

    # Make sure columns is a list
    if not isinstance(columns, list):
        columns = [columns]

    # Extract range from coordinates and filter
    coordinates = get_coordinates(df).loc[mfilter, columns]

    if isinstance(coordinates, pd.Series):
        mrange = f"{coordinates.iloc[0]}:{coordinates.iloc[-1]}"
    else:
        mrange = f"{coordinates.iloc[0, 0]}:{coordinates.iloc[-1, -1]}"

    # Filter data to be updated
    values = df.loc[mfilter, columns].values.tolist()

    # Make sure that values is a list of lists
    if not isinstance(values[0], list):
        values = [values]

    # Update values in gspreadsheet
    log.info(f"Updating {spreadsheet_name}/{sheet_name}/{mrange}")
    wks.update(mrange, values)
