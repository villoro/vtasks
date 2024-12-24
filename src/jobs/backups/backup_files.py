import re

from datetime import date
from datetime import timedelta

from prefect import task

from jobs.backups.tasks import BACKUP_TASKS
from common.logs import get_logger
from common.dropbox import get_vdropbox, scan_folder_and_subfolders_by_regex

YEAR = f"{date.today():%Y}"
DAY = f"{date.today():%Y_%m_%d}"


def get_update_at(vdp, filename):
    """Get the date when a file was updated"""

    metadata = vdp.dbx.files_get_metadata(filename)
    return metadata.client_modified.date()


def one_backup(vdp, path, regex):
    """Back up a list of files from a folder"""

    logger = get_logger()

    # Backup all files
    for path, filename, _ in scan_folder_and_subfolders_by_regex(path, regex, vdp=vdp):
        origin = f"{path}/{filename}"
        logger.debug(f"Trying to backup '{origin}")
        updated_at = get_update_at(vdp, origin)

        if updated_at >= date.today() - timedelta(1):
            dest = f"{path}/Backups/{YEAR}/{updated_at:%Y_%m_%d} {filename}"

            if not vdp.file_exists(dest):
                logger.info(f"Copying '{origin}' to '{dest}'")
                vdp.dbx.files_copy(origin, dest)

            else:
                logger.debug(f"File '{origin}' has already been backed up")

        else:
            logger.debug(f"Skipping '{origin}' since has not been updated")


@task(name="vtasks.backup.backup_files")
def backup_files():
    """Back up all files from URIS"""

    logger = get_logger()
    vdp = get_vdropbox()

    for task in BACKUP_TASKS:
        logger.info(f"Backing up '{task.dict()}'")
        one_backup(vdp, **task.dict())
