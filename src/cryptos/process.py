import cryptocompare
import pandas as pd

from datetime import date

import gspreadsheets as gsh

from cryptos.kraken import get_balances
from expensor.constants import DF_WORTH
from expensor.constants import FILE_DATA
from prefect_task import vtask
from utils import log

SPREADSHEET_CRYPTO = "crypto_data"
SHEET_PRICES = "prices"
SHEET_VALUE = "value"
SHEET_VOL_KRAKEN = "vol_kraken"
SHEET_SUMMARY = "summary"

# Crypto id: cell in summary
MAIN_CRYPTOS = {"BTC": "B9", "ETH": "C9"}


def get_market_cap(cryptos, order_magnitude=10 ** 9):
    """Get market capitalization of the asked coins"""

    data = cryptocompare.get_price(cryptos, full=True)

    return {key: values["EUR"]["MKTCAP"] / order_magnitude for key, values in data["RAW"].items()}


def update_market_cap():
    """Update market capitalization in google spreadsheet"""
    volumes = get_market_cap(list(MAIN_CRYPTOS))

    for crypto, cell in MAIN_CRYPTOS.items():
        gsh.update_cell(SPREADSHEET_CRYPTO, SHEET_SUMMARY, cell, volumes[crypto])


def get_crypto_prices(cryptos):
    """Get latest prices of a list of cryptos"""

    log.info("Retriving crypto prices")

    # Query cryptos
    data = cryptocompare.get_price([*cryptos])

    # Create a dict with prices
    return {i: x["EUR"] for i, x in data.items()}


def update_crypto_prices(mfilter):
    """Update latest cryptos prices"""

    df = gsh.read_df_gdrive(SPREADSHEET_CRYPTO, SHEET_PRICES, "all")

    # Update prices
    values = get_crypto_prices(df.columns)
    df.loc[mfilter] = pd.Series(values)

    # Update gspreadsheet
    gsh.df_to_gspread(SPREADSHEET_CRYPTO, SHEET_PRICES, df, mfilter)


def update_kraken_balances(mfilter):
    """Update balances from kraken"""

    df = gsh.read_df_gdrive(SPREADSHEET_CRYPTO, SHEET_VOL_KRAKEN, "all")

    # Retrive from kraken API and update
    df.loc[mfilter] = get_balances()

    # Update gspreadsheet
    gsh.df_to_gspread(SPREADSHEET_CRYPTO, SHEET_VOL_KRAKEN, df, mfilter)


def update_expensor(mfilter):
    """Update expensor cryptos worth based on crypto values"""

    col_crypto = "kraken"

    # Get worths
    df = gsh.read_df_gdrive(FILE_DATA, DF_WORTH, "all")

    # Get worth of actual month
    prices = gsh.read_df_gdrive(SPREADSHEET_CRYPTO, SHEET_VALUE, "all")

    # Update kraken value
    df.at[mfilter, col_crypto] = prices.at[mfilter, "Total"]

    gsh.df_to_gspread(FILE_DATA, DF_WORTH, df, mfilter, col_crypto)


@vtask
def update_cryptos(mdate):

    mfilter = mdate.strftime("%Y-%m-01")

    update_market_cap()

    update_crypto_prices(mfilter)
    update_kraken_balances(mfilter)
    update_expensor(mfilter)
