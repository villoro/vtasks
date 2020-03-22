"""
    Create the raw data for the reprot
"""

from multiprocessing import Pool

import pandas as pd

from global_utilities import log
from . import extract_data
from . import create_report

MIN_DATE = "2015-12-01"
NUM_OF_JOBS_DEFAULT = 5  # If 1 or lower no multiprocessing


def create_one_report(mdate):
    """ Creates a report for one month """

    extract_data.main(mdate)
    create_report.main(mdate)

    log.success(f"Report {mdate:%Y-%m} created")


def main(mdate, n_jobs=NUM_OF_JOBS_DEFAULT):

    mdate = pd.to_datetime(mdate)
    all_dates = pd.date_range(start=MIN_DATE, end=mdate, freq="MS")

    # Do n_jobs in parallel
    if n_jobs > 1:
        log.info(f"Doing reports in parallel with {n_jobs} jobs")

        with Pool(5) as p:
            p.map(create_one_report, all_dates)

    # No parallelization
    else:
        log.info(f"Doing reports without parallelization")

        for x in all_dates.tolist():
            create_one_report(x)
