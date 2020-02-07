import pandas as pd

from global_utilities import log
from . import extract_data
from . import create_report

MIN_DATE = "2015-12-01"


def main(mdate):

    mdate = pd.to_datetime(mdate)
    all_dates = pd.date_range(start=MIN_DATE, end=mdate, freq="MS")

    # Iterate all dates
    for x in all_dates.tolist():
        extract_data.main(x)
        create_report.main(x)

        log.success(f"Report {x:%Y-%m} created")
