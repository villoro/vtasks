from pandas import to_datetime

from . import extract_data
from . import create_report


def main(mdate):

    mdate = to_datetime(mdate)

    extract_data.main(mdate)
    create_report.main(mdate)
