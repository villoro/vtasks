import pandas as pd
import requests
import utils as u

BASE_URL = "https://api.indexacapital.com"
TOKEN_NAME = "INDEXA_TOKEN"


def query_indexa(endpoint):
    """ Raw function for querying indexa """

    token = u.get_secret(TOKEN_NAME)

    res = requests.get(f"{BASE_URL}/{endpoint}", headers={"X-AUTH-TOKEN": token})
    res.raise_for_status()

    return res.json()


def get_accounts():
    """ Get user accounts """

    accounts = query_indexa("users/me")["accounts"]
    return [x["account_number"] for x in accounts]


def get_invested_and_worth(account):
    """ Gets the money invested and the actual worth of an account """

    data = query_indexa(f"accounts/{account}/performance")

    invested = data["return"]["investment"]
    worth = data["return"]["total_amount"]

    return round(invested, 2), round(worth, 2)
