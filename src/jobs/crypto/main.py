from datetime import date

import cryptocompare
import pandas as pd
from prefect import flow
from prefect import task

from src.common import gsheets


# crypto_data spreadsheet info
SPREADSHEET_CRYPTO = "crypto_data"
SHEET_PRICES = "prices"
SHEET_VALUE = "value"
SHEET_VOL_KRAKEN = "vol_kraken"
SHEET_SUMMARY = "summary"

# expensor_data spreadsheet info
SPREADSHEET_EXPENSOR = "expensor_data"
SHEET_WORTH = "worth_m"
COL_CRYPTO = "crypto"


# Crypto id: cell in summary
MAIN_CRYPTOS = {"BTC": "B9", "ETH": "C9"}

FLOW_NAME = "crypto"


def get_market_cap(cryptos, order_magnitude=10**9):
    """Get market capitalization of the asked coins"""

    data = cryptocompare.get_price(cryptos, full=True)

    return {
        key: values["EUR"]["MKTCAP"] / order_magnitude
        for key, values in data["RAW"].items()
    }


@task(name=f"{FLOW_NAME}.update_market_cap")
def update_market_cap():
    """Update market capitalization in google spreadsheet"""
    volumes = get_market_cap(list(MAIN_CRYPTOS))

    for crypto, cell in MAIN_CRYPTOS.items():
        gsheets.update_cell(SPREADSHEET_CRYPTO, SHEET_SUMMARY, cell, volumes[crypto])


def get_crypto_prices(cryptos):
    """Get latest prices of a list of cryptos"""

    # Query cryptos
    data = cryptocompare.get_price([*cryptos])

    # Create a dict with prices
    return {i: x["EUR"] for i, x in data.items()}


@task(name=f"{FLOW_NAME}.update_crypto_prices")
def update_crypto_prices(mfilter):
    """Update latest cryptos prices"""

    df = gsheets.read_gdrive_sheet(SPREADSHEET_CRYPTO, SHEET_PRICES, with_index=True)

    # Update prices
    values = get_crypto_prices(df.columns)
    df.loc[mfilter] = pd.Series(values)

    # Update gspreadsheet
    gsheets.df_to_gspread(SPREADSHEET_CRYPTO, SHEET_PRICES, df, mfilter)


def to_number(x):
    return float(x.replace(".", "").replace(",", ".").replace(" €", ""))


@task(name=f"{FLOW_NAME}.update_expensor")
def update_expensor(mfilter):
    """Update expensor cryptos worth based on crypto values"""

    # Get worths
    df = gsheets.read_gdrive_sheet(SPREADSHEET_EXPENSOR, SHEET_WORTH, with_index=True)

    # Get worth of actual month
    prices = gsheets.read_gdrive_sheet(SPREADSHEET_CRYPTO, SHEET_VALUE, with_index=True)

    # Update kraken value
    value = prices.at[mfilter, "Total"]
    df.at[mfilter, COL_CRYPTO] = to_number(value)

    gsheets.df_to_gspread(SPREADSHEET_EXPENSOR, SHEET_WORTH, df, mfilter, COL_CRYPTO)


@flow(name=FLOW_NAME)
def crypto():
    mfilter = date.today().strftime("%Y/%m")

    update_market_cap()

    update_crypto_prices(mfilter)
    update_expensor(mfilter)


if __name__ == "__main__":
    crypto()
