import pandas as pd
import requests
import utils as u

from datetime import date
from prefect import task

import gspreadsheets as gsh

from expensor.constants import DF_INVEST
from expensor.constants import DF_WORTH
from expensor.constants import FILE_DATA
from utils import log
from utils import timeit

BASE_URL = "https://api.indexacapital.com"
TOKEN_NAME = "INDEXA_TOKEN"

ACCOUNTS = {"indexa": "HVGLMEL8", "indexa_pp": "PYDTR6X6"}


def query_indexa(endpoint):
    """ Raw function for querying indexa """

    url = f"{BASE_URL}/{endpoint}"
    log.info(f"Querying '{url}'")

    token = u.get_secret(TOKEN_NAME)

    res = requests.get(url, headers={"X-AUTH-TOKEN": token})
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

    return {"invested": round(invested, 2), "worth": round(worth, 2)}


@task
@timeit
def update_indexa(mdate):

    mfilter = mdate.strftime("%Y-%m-01")

    # Get indexa info
    portfolio = {}
    for name, account in ACCOUNTS.items():
        portfolio[name] = get_invested_and_worth(account)

    # Get dataframes from gdrive
    df_invest = gsh.read_df_gdrive(FILE_DATA, DF_INVEST, "all")
    df_worth = gsh.read_df_gdrive(FILE_DATA, DF_WORTH, "all")

    # Update dataframes
    for name, data in portfolio.items():
        df_invest.at[mfilter, name] = data["invested"]
        df_worth.at[mfilter, name] = data["worth"]

    # Update gdrive values
    cols = [*portfolio.keys()]
    gsh.update_gspread(FILE_DATA, DF_INVEST, df_invest, mfilter, cols)
    gsh.update_gspread(FILE_DATA, DF_WORTH, df_worth, mfilter, cols)
