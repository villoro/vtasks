from datetime import date

from prefect import flow
from prefect import task

from vtasks.common import gsheets
from vtasks.jobs.indexa import indexa


# expensor_data spreadsheet info
SPREADSHEET = "expensor_data"
SHEET_WORTH = "worth_m"
SHEET_INVEST = "invest_m"

FLOW_NAME = "indexa"


@task(name=f"{FLOW_NAME}.query_portfolio")
def query_portfolio():
    """query portfolio info form indexa"""
    portfolio = {}
    for name, account in indexa.ACCOUNTS.items():
        portfolio[name] = indexa.query_invested_and_worth(account)

    return portfolio


@task(name=f"{FLOW_NAME}.read_invested_and_worth")
def read_invested_and_worth():
    """get dataframes from gdrive"""
    df_invest = gsheets.read_gdrive_sheet(SPREADSHEET, SHEET_INVEST, with_index=True)
    df_worth = gsheets.read_gdrive_sheet(SPREADSHEET, SHEET_WORTH, with_index=True)

    return df_invest, df_worth


@task(name=f"{FLOW_NAME}.update_invested_and_worth")
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
    gsheets.df_to_gspread(SPREADSHEET, SHEET_INVEST, df_invest, mfilter, cols)
    gsheets.df_to_gspread(SPREADSHEET, SHEET_WORTH, df_worth, mfilter, cols)


@flow(name=FLOW_NAME)
def indexa_all():
    mfilter = date.today().strftime("%Y/%m")

    portfolio = query_portfolio()
    df_invest, df_worth = read_invested_and_worth()
    update_invested_and_worth(mfilter, portfolio, df_invest, df_worth)


if __name__ == "__main__":
    indexa_all()
