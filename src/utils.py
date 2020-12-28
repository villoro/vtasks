import functools
import sys

from datetime import date
from os import path
from pathlib import Path
from time import time

import gspread
import pandas as pd

from loguru import logger as log
from vcrypto import Cipher
from vdropbox import Vdropbox


# Base path of the repo.
# It need to go 2 times up since this file has the following relative path:
#   /src/utils.py
PATH_ROOT = Path(__file__).parent.parent

LOG_PATH = f"logs/{date.today():%Y_%m}/{date.today():%Y_%m_%d}.log"

CIPHER = None


def get_path(path_relative):
    """ Returns absolute path using PATH_ROOT """

    return str(PATH_ROOT / path_relative)


CONFIG = {
    "handlers": [
        {"sink": sys.stdout, "level": "INFO"},
        {"sink": get_path(LOG_PATH), "level": "INFO",},
    ]
}


log.configure(**CONFIG)
log.enable("vtasks")


def get_secret(key):
    """ Retrives one encrypted secret """

    global CIPHER
    if CIPHER is None:
        CIPHER = Cipher(secrets_file=get_path("secrets.yaml"), environ_var_name="VTASKS_TOKEN")

    return CIPHER.get_secret(key)


def get_vdropbox(secret_name):
    """ Creates a vdropbox instance """

    return Vdropbox(get_secret(secret_name), log=log)


def timeit(func):
    """ Timing decorator """

    @functools.wraps(func)
    def timed_execution(*args, **kwa):
        """ Outputs the execution time of a function """
        t0 = time()
        result = func(*args, **kwa)

        total_time = time() - t0

        if total_time < 60:
            log.info(f"{func.__name__} done in {total_time:.2f} seconds")
        else:
            log.info(f"{func.__name__} done in {total_time/60:.2f} minutes")

        return result

    return timed_execution


PATH_GDRIVE_KEY = f"{PATH_ROOT}/gdrive.json"


def export_gdrive_auth():
    """ Export gdrive json auth """

    log.info(f"Exporting '{PATH_GDRIVE_KEY}'")

    with open(PATH_GDRIVE_KEY, "w") as file:
        file.write(get_secret("GDRIVE"))


GDRIVE = None


def read_df_gdrive(
    spreadsheet_name, sheet_name, index_as_datetime=True, cols_to_numeric=None, fillna=True
):
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

    # Init GDRIVE if it has not been init
    global GDRIVE
    if GDRIVE is None:

        if not path.exists(PATH_GDRIVE_KEY):
            export_gdrive_auth()

        GDRIVE = gspread.service_account(filename=PATH_GDRIVE_KEY)

    # Open sheet
    spreadsheet = GDRIVE.open(spreadsheet_name)
    sheet = spreadsheet.worksheet(sheet_name)

    # Create dataframe
    data = sheet.get_all_records()
    df = pd.DataFrame(data)

    index_col = df.columns[0]

    if index_as_datetime:
        df[index_col] = pd.to_datetime(df[index_col])

    # Set first column as index
    df = df.set_index(index_col)

    if cols_to_numeric is None:
        cols_to_numeric = df.columns

    # Cast cols to numeric
    for col in cols_to_numeric:
        df[col] = pd.to_numeric(
            df[col].str.replace(".", "").str.replace(",", ".").str.replace(" €", "")
        )

    # Fill NA with 0 for all numeric cols
    if fillna:
        df.loc[:, cols_to_numeric] = df.loc[:, cols_to_numeric].fillna(0)

    return df
