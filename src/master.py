from datetime import date

from prefect import Flow
from prefect import Parameter
from prefect.utilities import logging

import utils as u

from archive import archive
from backups import backup_files
from backups import clean_backups
from cryptos import update_cryptos
from expensor import expensor
from flights import flights
from flights import merge_flights_history
from gcalendar import export_calendar_events
from gcalendar import gcal_report
from indexa import update_indexa
from money_lover import money_lover
from utils import detect_env
from utils import log
from vbooks import vbooks

# Replace prefect log with loguru log
logging.get_logger = lambda x: log

with Flow("do_all") as flow:
    mdate = Parameter("mdate")

    # Archive documents
    archive()

    # Calendar
    gcal_report(mdate=mdate, upstream_tasks=[export_calendar_events(mdate=mdate)])

    # Backups
    backup_files(upstream_tasks=[export_calendar_events(mdate=mdate)])
    clean_backups(upstream_tasks=[backup_files])

    # Expensor
    expensor(
        mdate=mdate,
        upstream_tasks=[update_cryptos(mdate=mdate), update_indexa(mdate=mdate), money_lover],
    )

    # Flights
    flights(mdate=mdate)
    merge_flights_history(mdate=mdate)

    # Vbooks
    vbooks()


def download_log(vdp):
    """Get log info from dropbox before running the script"""

    if vdp.file_exists(u.LOG_PATH_DROPBOX):
        log.info("Downloading log from dropbox")

        data = vdp.read_file(u.LOG_PATH_DROPBOX)

        # Add a new line between runs
        data += "\n"

        with open(u.get_path(u.LOG_PATH), "w", encoding="latin1") as file:
            file.write(data)

    else:
        log.info("Log not downloaded (first run of the day)")


def copy_log(vdp):
    """Copy log to dropbox"""

    log.info(f"Copying '{u.LOG_PATH}' to dropbox")

    with open(u.get_path(u.LOG_PATH), encoding="latin1") as file:
        data = file.read()

    vdp.write_file(data, u.LOG_PATH_DROPBOX)


def run_etl():
    """Run the ETL for today"""

    # Get dropbox connector
    vdp = u.get_vdropbox()

    download_log(vdp)

    detect_env()

    log.info("Starting vtasks")
    result = u.timeit(flow.run)(mdate=date.today())
    log.info("End of vtasks")

    copy_log(vdp)

    if not result.is_successful():
        log.error("ETL has failed")
        raise ValueError("ETL has failed")


if __name__ == "__main__":
    run_etl()
