import requests

from vtasks.common.logs import get_logger
from vtasks.common.secrets import read_secret

BASE_URL = "https://api.indexacapital.com"
TOKEN_NAME = "INDEXA_TOKEN"

ACCOUNTS = {"indexa": "HVGLMEL8", "indexa_pp": "PYDTR6X6"}


def _query_indexa(endpoint):
    """Raw function for querying indexa"""

    logger = get_logger()

    url = f"{BASE_URL}/{endpoint}"
    logger.debug(f"Querying {url=}")

    token = read_secret(TOKEN_NAME)

    res = requests.get(url, headers={"X-AUTH-TOKEN": token})
    res.raise_for_status()

    return res.json()


def get_accounts():
    """Get user accounts"""

    accounts = _query_indexa("users/me")["accounts"]
    return [x["account_number"] for x in accounts]


def query_invested_and_worth(account):
    """Gets the money invested and the actual worth of an account"""

    data = _query_indexa(f"accounts/{account}/performance")

    invested = data["return"]["investment"]
    worth = data["return"]["total_amount"]

    return {"invested": round(invested, 2), "worth": round(worth, 2)}
