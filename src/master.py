from argparse import ArgumentParser
from datetime import date

from prefect import Flow
from prefect import Parameter
from prefect.utilities import logging

import utils as u

from utils.log import log

from flights import flights
from flights import merge_flights_history
from money_lover import money_lover
from reports import reports
from reports.constants import VAR_DROPBOX_TOKEN

# Replace loguru log
logging.get_logger = lambda x: log

with Flow("do_all") as flow:
    mdate = Parameter("mdate")
    pro = Parameter("pro")

    # Reports part
    df_trans = money_lover(mdate, export_data=True)
    reports(mdate, df_trans=df_trans, pro=pro)

    # Flights part
    flights(mdate)
    merge_flights_history(mdate)


def detect_env():
    """ Detect if it is PRO environment """

    parser = ArgumentParser()
    parser.add_argument("--pro", help="Wether it is PRO or not (DEV)", default=False, type=bool)

    args = parser.parse_args()

    return args.pro


def download_log(dbx):
    """ Get log info from dropbox before running the script """

    if u.dropbox.file_exists(dbx, u.log_path):
        data = u.dropbox.read_textfile(dbx, u.log_path)

        # Add a new line between runs
        data += "\n"

        with open(u.uos.get_path(u.log_path), "w") as file:
            file.write(data)

        log.info("Log retrived from dropbox")

    else:
        log.info("Log not downloaded (first run of the day)")


def copy_log(dbx):
    """ Copy log to dropbox """

    log.info(f"Copying '{u.log_path}' to dropbox")

    with open(u.uos.get_path(u.log_path)) as file:
        data = file.read()

    u.dropbox.write_textfile(dbx, data, f"/{u.log_path}")


def run_etl():
    """ Run the ETL for today """

    # Get dropbox connector
    dbx = u.dropbox.get_dbx_connector(VAR_DROPBOX_TOKEN)

    download_log(dbx)

    pro = detect_env()

    if pro:
        log.info("Working on PRO")
    else:
        log.info("Working on DEV")

    log.info("Starting vtasks")
    u.timeit(flow.run)(mdate=date.today(), pro=pro)
    log.info("End of vtasks")

    if pro:
        copy_log(dbx)


if __name__ == "__main__":
    run_etl()
