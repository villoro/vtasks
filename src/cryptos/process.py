import cryptocompare
import pandas as pd

from datetime import date

import gspreadsheets as gsh

from utils import log

SPREADSHEET = "crypto_data"
SHEET = "prices"


def get_crypto_prices(cryptos):
    """ Get latest prices of a list of cryptos """

    log.info("Retriving crypto prices")

    # Query cryptos
    data = cryptocompare.get_price([*cryptos])

    # Create a dict with prices
    return {i: x["EUR"] for i, x in data.items()}


def update_prices(mdate):
    """ Update latest cryptos prices """

    mfilter = mdate.strftime("%Y-%m-01")

    df = gsh.read_df_gdrive(SPREADSHEET, SHEET, "all")

    # Update prices
    values = get_crypto_prices(df.columns)
    new_prices = pd.DataFrame(values, index=[mfilter])
    df.update(new_prices)

    # Update gspreadsheet
    gsh.update_gspread(SPREADSHEET, SHEET, df, mfilter)
