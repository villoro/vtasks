from datetime import datetime

from . import extract_data
from . import create_report


def main(mdate):

    # Cast mdate to datetime
    mdate = datetime.strptime(mdate, "%Y_%m_%d")

    extract_data.main(mdate)
    create_report.main(mdate)
