import pandas as pd
import requests
import utils as u

from datetime import date

import gspreadsheets as gsh

from prefect import flow
from prefect import get_run_logger
from prefect import task

from expensor.constants import DF_INVEST
from expensor.constants import DF_WORTH
from expensor.constants import FILE_DATA

BASE_URL = "https://api.indexacapital.com"
TOKEN_NAME = "INDEXA_TOKEN"

ACCOUNTS = {"indexa": "HVGLMEL8", "indexa_pp": "PYDTR6X6"}


def query_indexa(endpoint):
    """Raw function for querying indexa"""

    # log = get_run_logger()

    url = f"{BASE_URL}/{endpoint}"
    # log.info(f"Querying '{url}'")

    token = u.get_secret(TOKEN_NAME)

    res = requests.get(url, headers={"X-AUTH-TOKEN": token})
    res.raise_for_status()

    return res.json()


def get_accounts():
    """Get user accounts"""

    accounts = query_indexa("users/me")["accounts"]
    return [x["account_number"] for x in accounts]


def query_invested_and_worth(account):
    """Gets the money invested and the actual worth of an account"""

    data = query_indexa(f"accounts/{account}/performance")

    invested = data["return"]["investment"]
    worth = data["return"]["total_amount"]

    return {"invested": round(invested, 2), "worth": round(worth, 2)}


@task(name="vtasks.indexa.query")
def query_portfolio():
    """query portfolio info form indexa"""
    portfolio = {}
    for name, account in ACCOUNTS.items():
        portfolio[name] = query_invested_and_worth(account)

    return portfolio


@task(name="vtasks.indexa.read")
def read_invested_and_worth():
    """get dataframes from gdrive"""
    df_invest = gsh.read_df_gdrive(FILE_DATA, DF_INVEST, "all")
    df_worth = gsh.read_df_gdrive(FILE_DATA, DF_WORTH, "all")

    return df_invest, df_worth


@task(name="vtasks.indexa.update")
def update_invested_and_worth(mfilter, portfolio, df_invest_in, df_worth_in):
    """update data"""

    df_invest = df_invest_in.copy()
    df_worth = df_worth_in.copy()

    # Update dataframes
    for name, data in portfolio.items():
        df_invest.at[mfilter, name] = data["invested"]
        df_worth.at[mfilter, name] = data["worth"]

    # Update gdrive values
    cols = [*portfolio.keys()]
    gsh.df_to_gspread(FILE_DATA, DF_INVEST, df_invest, mfilter, cols)
    gsh.df_to_gspread(FILE_DATA, DF_WORTH, df_worth, mfilter, cols)


@flow(name="vtasks.indexa")
def indexa(mdate):

    mfilter = mdate.strftime("%Y-%m-01")

    portfolio = query_portfolio()
    df_invest, df_worth = read_invested_and_worth()
    update_invested_and_worth(mfilter, portfolio, df_invest, df_worth)
