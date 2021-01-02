import cryptocompare
import pandas as pd

from datetime import date
from prefect import task

import gspreadsheets as gsh

from utils import log
from utils import timeit

SPREADSHEET = "crypto_data"
SHEET = "prices"


def get_crypto_prices(cryptos):
    """ Get latest prices of a list of cryptos """

    log.info("Retriving crypto prices")

    # Query cryptos
    data = cryptocompare.get_price([*cryptos])

    # Create a dict with prices
    return {i: x["EUR"] for i, x in data.items()}


@task
@timeit
def update_crypto_prices(mdate):
    """ Update latest cryptos prices """

    mfilter = mdate.strftime("%Y-%m-01")

    df = gsh.read_df_gdrive(SPREADSHEET, SHEET, "all")

    # Update prices
    values = get_crypto_prices(df.columns)
    df.loc[mfilter] = pd.Series(values)

    # Update gspreadsheet
    gsh.update_gspread(SPREADSHEET, SHEET, df, mfilter)
