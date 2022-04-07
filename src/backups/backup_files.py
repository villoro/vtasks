import re

from datetime import date
from datetime import timedelta


from .files import files_regexs
from prefect_task import vtask
from utils import get_files_from_regex
from utils import get_path
from utils import get_vdropbox
from utils import log

YEAR = f"{date.today():%Y}"
DAY = f"{date.today():%Y_%m_%d}"


def get_update_at(vdp, filename):
    """Get the date when a file was updated"""

    metadata = vdp.dbx.files_get_metadata(filename)
    return metadata.client_modified.date()


def one_backup(vdp, path, regex):
    """Back up a list of files from a folder"""

    # Backup all files
    for path, filename, _ in get_files_from_regex(vdp, path, regex):

        origin = f"{path}/{filename}"
        log.debug(f"Trying to backup '{origin}")
        updated_at = get_update_at(vdp, origin)

        if updated_at >= date.today() - timedelta(1):

            dest = f"{path}/Backups/{YEAR}/{updated_at:%Y_%m_%d} {filename}"

            if not vdp.file_exists(dest):
                log.info(f"Copying '{origin}' to '{dest}'")

                vdp.dbx.files_copy(origin, dest)

            else:
                log.debug(f"File '{origin}' has already been backed up")

        else:
            log.debug(f"Skipping '{origin}' since has not been updated")


@vtask
def backup_files():
    """Back up all files from URIS"""

    vdp = get_vdropbox()

    for kwargs in files_regexs:
        log.info("Scanning '{path}/{regex}'".format(**kwargs))
        one_backup(vdp, **kwargs)
