import requests

from vtasks.common.logs import get_logger
from vtasks.common.secrets import read_secret

BASE_URL = "https://min-api.cryptocompare.com/data"
TOKEN_NAME = "CRYPTOCOMPARE_API_KEY"

DEFAULT_CURRENCY = "EUR"


class CryptoCompare:
    """Helper to interact with the CryptoCompare API"""

    def __init__(self, currency=DEFAULT_CURRENCY):
        self.currency = currency
        self.token = read_secret(TOKEN_NAME)

    def _query(self, endpoint, params):
        """Raw function for querying CryptoCompare"""

        logger = get_logger()

        url = f"{BASE_URL}/{endpoint}"
        logger.debug(f"Querying {url=} {params=}")

        headers = {"authorization": f"Apikey {self.token}"}
        res = requests.get(url, params=params, headers=headers)
        res.raise_for_status()

        data = res.json()

        # CryptoCompare returns 200 even on errors, signaling them in the body
        if isinstance(data, dict) and data.get("Response") == "Error":
            raise RuntimeError(f"CryptoCompare error: {data.get('Message')}")
        if isinstance(data, dict) and data.get("Err"):
            raise RuntimeError(f"CryptoCompare error: {data['Err'].get('message')}")

        return data

    def ping(self):
        """Check that the API is reachable and the credentials are valid"""

        logger = get_logger()
        logger.info("Pinging CryptoCompare")

        self._query("price", {"fsym": "BTC", "tsyms": self.currency})

        logger.info("CryptoCompare ping successful")
        return True

    def get_prices(self, cryptos):
        """Get the latest price of a list of cryptos in `self.currency`"""

        data = self._query(
            "pricemulti", {"fsyms": ",".join(cryptos), "tsyms": self.currency}
        )
        return {coin: values[self.currency] for coin, values in data.items()}

    def get_market_cap(self, cryptos, order_magnitude=10**9):
        """Get market capitalization of the asked coins in `self.currency`"""

        data = self._query(
            "pricemultifull", {"fsyms": ",".join(cryptos), "tsyms": self.currency}
        )
        return {
            coin: values[self.currency]["MKTCAP"] / order_magnitude
            for coin, values in data["RAW"].items()
        }
