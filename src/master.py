from argparse import ArgumentParser
from datetime import date


from prefect import Flow
from prefect import Parameter
from prefect.utilities import logging

import utils as u

from expensor import reports
from flights import flights
from flights import merge_flights_history
from money_lover import money_lover
from utils import log

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


def download_log(vdp):
    """ Get log info from dropbox before running the script """

    if vdp.file_exists(u.LOG_PATH_DROPBOX):
        log.info("Downloading log from dropbox")

        data = vdp.read_file(u.LOG_PATH_DROPBOX)

        # Add a new line between runs
        data += "\n"

        with open(u.get_path(u.LOG_PATH), "w") as file:
            file.write(data)

    else:
        log.info("Log not downloaded (first run of the day)")


def copy_log(vdp):
    """ Copy log to dropbox """

    log.info(f"Copying '{u.LOG_PATH}' to dropbox")

    with open(u.get_path(u.LOG_PATH)) as file:
        data = file.read()

    vdp.write_file(data, u.LOG_PATH_DROPBOX)


def run_etl():
    """ Run the ETL for today """

    # Get dropbox connector
    vdp = u.get_vdropbox()

    download_log(vdp)

    pro = detect_env()

    if pro:
        log.info("Working on PRO")
    else:
        log.info("Working on DEV")

    log.info("Starting vtasks")
    u.timeit(flow.run)(mdate=date.today(), pro=pro)
    log.info("End of vtasks")

    if pro:
        copy_log(vdp)


if __name__ == "__main__":
    run_etl()
