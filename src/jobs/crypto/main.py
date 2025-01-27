import cryptocompare
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

# Crypto id: cell in summary
MAIN_CRYPTOS = {"BTC": "B9", "ETH": "C9"}

FLOW_NAME = "vtasks.crypto"


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
