import gspread
import pandas as pd

from os import path

from utils import PATH_ROOT
from utils import log

PATH_GDRIVE_KEY = f"{PATH_ROOT}/gdrive.json"

GDRIVE = None


def init_gdrive():
    """ Export gdrive json auth """

    # Init GDRIVE if it has not been init
    global GDRIVE
    if GDRIVE is None:

        if not path.exists(PATH_GDRIVE_KEY):

            log.info(f"Exporting '{PATH_GDRIVE_KEY}'")

            with open(PATH_GDRIVE_KEY, "w") as file:
                file.write(get_secret("GDRIVE"))

        GDRIVE = gspread.service_account(filename=PATH_GDRIVE_KEY)


def get_gdrive_sheet(spreadsheet_name, sheet_name):
    """
        Get a google drive spreadsheet

        Args:
            spreadsheet_name:   name of the document
            sheet_name:         name of the sheet inside the document
    """

    init_gdrive()

    # Open sheet
    spreadsheet = GDRIVE.open(spreadsheet_name)
    return spreadsheet.worksheet(sheet_name)


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
