from argparse import ArgumentParser
from datetime import date


from prefect import Flow
from prefect import Parameter
from prefect.utilities import logging

import utils as u

from backups import backup_files
from backups import clean_backups
from cryptos import update_cryptos
from expensor import expensor
from flights import flights
from flights import merge_flights_history
from indexa import update_indexa
from money_lover import money_lover
from utils import log
from vbooks import vbooks

# Replace prefect log with loguru log
logging.get_logger = lambda x: log

with Flow("do_all") as flow:
    mdate = Parameter("mdate")
    pro = Parameter("pro")

    # Backups
    backup_files()
    clean_backups()

    # Crypto + Indexa
    dummy_crypto = update_cryptos(mdate)
    dummy_indexa = update_indexa(mdate)

    # Expensor
    df_trans = money_lover(mdate, export_data=True)
    expensor(
        mdate=mdate,
        df_trans=df_trans,
        pro=pro,
        dummy_crypto=dummy_crypto,
        dummy_indexa=dummy_indexa,
    )

    # Flights
    flights(mdate)
    merge_flights_history(mdate)

    # Vbooks
    vbooks()


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

        with open(u.get_path(u.LOG_PATH), "w", encoding="utf8") as file:
            file.write(data)

    else:
        log.info("Log not downloaded (first run of the day)")


def copy_log(vdp):
    """ Copy log to dropbox """

    log.info(f"Copying '{u.LOG_PATH}' to dropbox")

    with open(u.get_path(u.LOG_PATH), encoding="utf8") as file:
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
    result = u.timeit(flow.run)(mdate=date.today(), pro=pro)
    log.info("End of vtasks")

    copy_log(vdp)

    if not result.is_successful():
        log.error("ETL has failed")
        raise ValueError("ETL has failed")


if __name__ == "__main__":
    run_etl()
