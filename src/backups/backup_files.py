import re

from datetime import date
from datetime import timedelta

from prefect import task

from .config import URIS
from utils import get_files_from_regex
from utils import get_vdropbox
from utils import log
from utils import timeit

YEAR = f"{date.today():%Y}"
DAY = f"{date.today():%Y_%m_%d}"


def get_update_at(vdp, filename):
    """ Get the date when a file was updated """

    metadata = vdp.dbx.files_get_metadata(filename)
    return metadata.client_modified.date()


def updated_yesterday(vdp, filename):
    """ True if the file has been updated after the start of yesterday """

    updated_at = get_update_at(vdp, filename)

    return updated_at > date.today() - timedelta(1)


def one_backup(vdp, uri):
    """ Back up a list of files from a folder """

    path, filenames = get_files_from_regex(vdp, uri)

    # Backup all files
    for filename in filenames:
        origin = f"{path}/{filename}"
        dest = f"{path}/Backups/{YEAR}/{DAY} {filename}"

        if updated_yesterday(vdp, origin):

            if not vdp.file_exists(dest):
                log.info(f"Copying '{origin}' to '{dest}'")

                vdp.dbx.files_copy(origin, dest)

            else:
                log.debug(f"File '{origin}' has already been backed up")

        else:
            log.debug(f"Skipping '{origin}' since has not been updated")


@task
@timeit
def backup_files():
    """ Back up all files from URIS """

    vdp = get_vdropbox()

    for uri in URIS:
        log.info(f"Scanning '{uri}'")
        one_backup(vdp, uri)
