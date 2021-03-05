import krakenex

from pykrakenapi import KrakenAPI

from utils import get_secret
from utils import log

PAIRS = {
    "BCH": "BCH",
    "XXBT": "BTC",
    "DASH": "DASH",
    "ETH": "ETH",
    "XLTC": "LTC",
    "XXMR": "XMR",
    "XXRP": "XRP",
    "ZEUR": "EUR",
}


def get_api():
    """ Get krakenx API object """

    log.debug("Getting kraken API object")

    kx_api = krakenex.API(get_secret("kraken_key"), get_secret("kraken_secret"))
    return KrakenAPI(kx_api)


def get_balances(api=None):
    """ Retrive balances as a pandas serie """

    # Allow lazy loading
    if api is None:
        api = get_api()

    log.info("Getting balances from kraken")

    # Get balances as a pandas serie
    serie = api.get_account_balance()["vol"]

    # Combine all ETH balances
    serie["ETH"] = serie["XETH"] + serie["ETH2"] + serie["ETH2.S"]

    # Keep only assets in PAIRS while renaming
    return serie[[*PAIRS]].rename(PAIRS).apply(lambda x: round(x, 6))
