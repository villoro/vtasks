"""
    Create the raw data for the reprot
"""

from multiprocessing import Pool

import pandas as pd

from . import create_report
from . import extract_data
from prefect import task
from utils import log
from utils import timeit

MIN_DATE = "2015-12-01"
NUM_OF_JOBS_DEFAULT = 10  # If 1 or lower no multiprocessing


def create_one_report(mdate):
    """ Creates a report for one month """

    data = extract_data.main(mdate, export_data=False)
    create_report.main(mdate, data=data)

    log.success(f"Report {mdate:%Y-%m} created")


@task
@timeit
def reports(mdate, df_trans, pro, n_jobs=NUM_OF_JOBS_DEFAULT):

    # TODO: use df_trans input argument instead of reading it from dropbox

    mdate = pd.to_datetime(mdate)
    all_dates = pd.date_range(start=MIN_DATE, end=mdate, freq="MS")

    # Do n_jobs in parallel
    if n_jobs > 1:
        log.info(f"Doing reports in parallel with {n_jobs} jobs")

        with Pool(n_jobs) as p:
            p.map(create_one_report, all_dates)

    # No parallelization
    else:
        log.info(f"Doing reports without parallelization")
        for x in all_dates.tolist():
            create_one_report(x)
