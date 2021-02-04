"""
    Create the raw data for the reprot
"""

from multiprocessing import Pool

import pandas as pd

from prefect import task

from . import constants as c
from . import create_report
from . import extract_data
from gspreadsheets import read_df_gdrive
from utils import get_vdropbox
from utils import log
from utils import timeit

MIN_DATE = "2015-12-01"
NUM_OF_JOBS_DEFAULT = 1  # If 1 or lower no multiprocessing


def get_data():
    """ Retrive dataframes """

    # Get dfs
    log.debug("Reading excels from gdrive")
    dfs = {x: read_df_gdrive(c.FILE_DATA, x, cols) for x, cols in c.DFS_ALL_FROM_DATA.items()}

    # Add transactions
    log.debug("Reading data from dropbox")
    vdp = get_vdropbox()
    dfs[c.DF_TRANS] = vdp.read_excel(c.FILE_TRANSACTIONS).set_index(c.COL_DATE)

    return dfs


def create_one_report(dfs, mdate):
    """ Creates a report for one month """

    data = extract_data.main(dfs, mdate, export_data=False)
    create_report.main(mdate, data=data)

    log.success(f"Report {mdate:%Y-%m} created")


@task
@timeit
def expensor(mdate, df_trans, pro, n_jobs=NUM_OF_JOBS_DEFAULT, **kwa):

    # TODO: use df_trans input argument instead of reading it from dropbox

    mdate = pd.to_datetime(mdate)
    # Reversed since first we want the latest month
    all_dates = pd.date_range(start=MIN_DATE, end=mdate, freq="MS").to_list()[::-1]

    dfs = get_data()

    # Do n_jobs in parallel
    if n_jobs > 1:
        log.info(f"Doing reports in parallel with {n_jobs} jobs")

        # Create tuples with arguments
        args = [(dfs, x) for x in all_dates]

        with Pool(n_jobs) as p:
            p.starmap(create_one_report, args)

    # No parallelization
    else:
        log.info(f"Doing reports without parallelization")
        for x in all_dates:
            create_one_report(dfs, x)
