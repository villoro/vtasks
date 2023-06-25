from multiprocessing import Pool

import pandas as pd

from prefect import flow
from prefect import task

import utils as u

from . import constants as c
from .extract import extract_data
from .report import create_report
from gspreadsheets import read_df_gdrive


MIN_DATE = "2015-12-01"


def get_total_worth(dfs):
    """Append worth and liquid into total worth"""
    df_liquid = dfs[c.DF_LIQUID].drop(columns="Total")
    df_worth = dfs[c.DF_WORTH].drop(columns="Total")

    # Crop to where both have data
    min_index = max(df_worth.index.min(), df_liquid.index.min())

    df = pd.concat([df_liquid, df_worth], axis=1).loc[min_index:]
    df["Total"] = df.sum(axis=1)

    return df


@task(name="vtasks.expensor.read")
def get_data():
    """Retrive dataframes"""

    log = u.get_log()

    # Get dfs
    log.debug("Reading excels from gdrive")
    dfs = {x: read_df_gdrive(c.FILE_DATA, x, cols) for x, cols in c.DFS_ALL_FROM_DATA.items()}

    # Add transactions
    log.debug("Reading data from dropbox")
    vdp = u.get_vdropbox()
    dfs[c.DF_TRANS] = vdp.read_excel(c.FILE_TRANSACTIONS).set_index(c.COL_DATE)

    # Add total worth
    dfs[c.DF_TOTAL_WORTH] = get_total_worth(dfs)

    return dfs


@task(name="vtasks.expensor.report")
def create_one_report(dfs, mdate):
    """Creates a report for one month"""

    log = u.get_log()

    data = extract_data(dfs, mdate, export_data=False)
    create_report(mdate, data=data)

    log.info(f"Report {mdate:%Y-%m} created")


@flow(**u.get_prefect_args("vtasks.expensor"))
def expensor(mdate):
    log = u.get_log()

    mdate = pd.to_datetime(mdate)
    # Reversed since first we want the latest month
    all_dates = pd.date_range(start=MIN_DATE, end=mdate, freq="MS").to_list()[::-1]

    dfs = get_data()

    log.info(f"Doing reports without parallelization")
    for x in all_dates:
        create_one_report(dfs, x)
