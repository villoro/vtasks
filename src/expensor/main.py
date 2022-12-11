from multiprocessing import Pool

import pandas as pd

from prefect import flow, task, get_run_logger

from . import constants as c
from .extract import extract_data
from .report import create_report
from gspreadsheets import read_df_gdrive
from utils import get_vdropbox

MIN_DATE = "2015-12-01"


@task(name="vtasks.expensor.read")
def get_data():
    """Retrive dataframes"""

    log = get_run_logger()

    # Get dfs
    log.debug("Reading excels from gdrive")
    dfs = {x: read_df_gdrive(c.FILE_DATA, x, cols) for x, cols in c.DFS_ALL_FROM_DATA.items()}

    # Add transactions
    log.debug("Reading data from dropbox")
    vdp = get_vdropbox()
    dfs[c.DF_TRANS] = vdp.read_excel(c.FILE_TRANSACTIONS).set_index(c.COL_DATE)

    return dfs


@task(name="vtasks.expensor.report")
def create_one_report(dfs, mdate):
    """Creates a report for one month"""

    log = get_run_logger()

    data = extract_data(dfs, mdate, export_data=False)
    create_report(mdate, data=data)

    log.info(f"Report {mdate:%Y-%m} created")


@flow(name="vtasks.expensor")
def expensor(mdate):

    log = get_run_logger()

    mdate = pd.to_datetime(mdate)
    # Reversed since first we want the latest month
    all_dates = pd.date_range(start=MIN_DATE, end=mdate, freq="MS").to_list()[::-1]

    dfs = get_data()

    log.info(f"Doing reports without parallelization")
    for x in all_dates:
        create_one_report(dfs, x)