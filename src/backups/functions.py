import re

from datetime import date
from datetime import timedelta

from utils import get_vdropbox
from utils import log

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


def get_files_to_backup(vdp, uri):
    """ Get a path and a list of files form a regex """

    # Extract path and regex
    path = uri.split("/")
    regex = path.pop()
    path = "/".join(path)

    filenames = [x for x in vdp.ls(path) if re.search(regex, x)]

    return path, filenames


def backup(uri):
    """ Backs up some files """

    vdp = get_vdropbox()

    path, filenames = get_files_to_backup(vdp, uri)

    # Backup all files
    for filename in filenames:
        origin = f"{path}/{filename}"
        dest = f"{path}/{YEAR}/{DAY} {filename}"

        log.info(f"Backing up '{origin}'")

        vdp.dbx.files_copy(origin, dest)
