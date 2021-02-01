import cryptocompare
import pandas as pd

from datetime import date
from prefect import task

import gspreadsheets as gsh

from expensor.constants import DF_WORTH
from expensor.constants import FILE_DATA
from utils import log
from utils import timeit

SPREADSHEET_CRYPTO = "crypto_data"
SHEET_PRICES = "prices"
SHEET_VALUE = "value"


def get_crypto_prices(cryptos):
    """ Get latest prices of a list of cryptos """

    log.info("Retriving crypto prices")

    # Query cryptos
    data = cryptocompare.get_price([*cryptos])

    # Create a dict with prices
    return {i: x["EUR"] for i, x in data.items()}


def update_crypto_prices(mfilter):
    """ Update latest cryptos prices """

    df = gsh.read_df_gdrive(SPREADSHEET_CRYPTO, SHEET_PRICES, "all")

    # Update prices
    values = get_crypto_prices(df.columns)
    df.loc[mfilter] = pd.Series(values)

    # Update gspreadsheet
    gsh.df_to_gspread(SPREADSHEET_CRYPTO, SHEET_PRICES, df, mfilter)


def update_expensor(mfilter):
    """ Update expensor cryptos worth based on crypto values"""

    col_crypto = "kraken"

    # Get worths
    df = gsh.read_df_gdrive(FILE_DATA, DF_WORTH, "all")

    # Get worth of actual month
    prices = gsh.read_df_gdrive(SPREADSHEET_CRYPTO, SHEET_VALUE, "all")

    # Update kraken value
    df.at[mfilter, col_crypto] = prices.at[mfilter, "Total"]

    gsh.df_to_gspread(FILE_DATA, DF_WORTH, df, mfilter, col_crypto)


@task
@timeit
def update_cryptos(mdate):

    mfilter = mdate.strftime("%Y-%m-01")

    update_crypto_prices(mfilter)
    update_expensor(mfilter)
