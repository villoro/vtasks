from argparse import ArgumentParser
from datetime import date

from prefect import Flow
from prefect import Parameter
from prefect.utilities import logging

import global_utilities as gu

from global_utilities.log import log

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


def copy_log():
    """ Copy log to dropbox """

    log.info(f"Copying '{gu.log_path}' to dropbox")

    dbx = gu.dropbox.get_dbx_connector(VAR_DROPBOX_TOKEN)

    with open(gu.uos.get_path(gu.log_path)) as file:
        data = file.read()

    gu.dropbox.write_textfile(dbx, data, f"/{gu.log_path}")


def run_etl():
    """ Run the ETL for today """

    pro = detect_env()

    if pro:
        log.info("Working on PRO")
    else:
        log.info("Working on DEV")

    log.info("Starting vtasks")
    flow.run(mdate=date.today(), pro=pro)
    log.info("End of vtasks")

    if pro:
        copy_log()


if __name__ == "__main__":
    run_etl()
