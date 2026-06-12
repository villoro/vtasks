import backoff
import requests

from vtasks.common.logs import get_logger

BASE_URL = "https://api.coingecko.com/api/v3"

DEFAULT_CURRENCY = "eur"

# CoinGecko uses coin ids (e.g. 'bitcoin') instead of symbols (e.g. 'BTC')
SYMBOL_TO_ID = {
    "BCH": "bitcoin-cash",
    "BTC": "bitcoin",
    "DASH": "dash",
    "ETH": "ethereum",
    "LTC": "litecoin",
    "XMR": "monero",
    "XRP": "ripple",
}


class CoinGecko:
    """Helper to interact with the CoinGecko API"""

    def __init__(self, currency=DEFAULT_CURRENCY):
        self.currency = currency

    @backoff.on_exception(
        backoff.expo, requests.exceptions.RequestException, max_tries=5
    )
    def _query(self, endpoint, params=None):
        """Raw function for querying CoinGecko"""

        logger = get_logger()

        url = f"{BASE_URL}/{endpoint}"
        logger.debug(f"Querying {url=} {params=}")

        res = requests.get(url, params=params)
        res.raise_for_status()

        return res.json()

    def _to_ids(self, symbols):
        """Map crypto symbols (BTC) to CoinGecko ids (bitcoin)"""

        missing = [x for x in symbols if x not in SYMBOL_TO_ID]
        if missing:
            raise ValueError(
                f"Missing CoinGecko id mapping for {missing} (see SYMBOL_TO_ID)"
            )

        return {SYMBOL_TO_ID[x]: x for x in symbols}

    def ping(self):
        """Check that the API is reachable"""

        logger = get_logger()
        logger.info("Pinging CoinGecko")

        self._query("ping")

        logger.info("CoinGecko ping successful")
        return True

    def _simple_price(self, symbols, include_market_cap=False):
        ids = self._to_ids(symbols)

        data = self._query(
            "simple/price",
            {
                "ids": ",".join(ids),
                "vs_currencies": self.currency,
                "include_market_cap": str(include_market_cap).lower(),
            },
        )
        # Return keyed by symbol instead of CoinGecko id
        return {symbol: data[cid] for cid, symbol in ids.items()}

    def get_prices(self, cryptos):
        """Get the latest price of a list of cryptos in `self.currency`"""

        data = self._simple_price(cryptos)
        return {symbol: values[self.currency] for symbol, values in data.items()}

    def get_market_cap(self, cryptos, order_magnitude=10**9):
        """Get market capitalization of the asked coins in `self.currency`"""

        data = self._simple_price(cryptos, include_market_cap=True)
        return {
            symbol: values[f"{self.currency}_market_cap"] / order_magnitude
            for symbol, values in data.items()
        }
