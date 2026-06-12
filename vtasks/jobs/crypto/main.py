from datetime import date

import pandas as pd
from prefect import flow
from prefect import task

from vtasks.common import gsheets
from vtasks.jobs.crypto.api import CoinGecko


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


@task(name=f"{FLOW_NAME}.update_market_cap")
def update_market_cap():
    """Update market capitalization in google spreadsheet"""
    volumes = CoinGecko().get_market_cap(list(MAIN_CRYPTOS))

    for crypto, cell in MAIN_CRYPTOS.items():
        gsheets.update_cell(SPREADSHEET_CRYPTO, SHEET_SUMMARY, cell, volumes[crypto])


@task(name=f"{FLOW_NAME}.update_crypto_prices")
def update_crypto_prices(mfilter):
    """Update latest cryptos prices"""

    df = gsheets.read_gdrive_sheet(SPREADSHEET_CRYPTO, SHEET_PRICES, with_index=True)

    # Update prices
    values = CoinGecko().get_prices(df.columns)
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

    gsheets.df_to_gspread(
        SPREADSHEET_EXPENSOR, SHEET_WORTH, df, mfilter, COL_CRYPTO, do_clean=False
    )


@flow(name=FLOW_NAME)
def crypto():
    mfilter = date.today().strftime("%Y/%m")

    task(name=f"{FLOW_NAME}.ping")(CoinGecko().ping)()
    update_market_cap()

    update_crypto_prices(mfilter)
    update_expensor(mfilter)


if __name__ == "__main__":
    crypto()
