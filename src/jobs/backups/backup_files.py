from datetime import date
from datetime import timedelta

from prefect import flow
from prefect import task

from src.common.dropbox import get_vdropbox
from src.common.dropbox import scan_folder_and_subfolders_by_regex
from src.common.logs import get_logger
from src.jobs.backups.tasks import BACKUP_TASKS

YEAR = f"{date.today():%Y}"
DAY = f"{date.today():%Y_%m_%d}"
FLOW_NAME = "backups.backup_files"


def get_update_at(vdp, filename):
    """Get the date when a file was updated"""

    metadata = vdp.dbx.files_get_metadata(filename)
    return metadata.client_modified.date()


def one_backup(vdp, path, regex):
    """Back up a list of files from a folder"""

    logger = get_logger()

    # Backup all files
    for path, filename in scan_folder_and_subfolders_by_regex(path, regex, vdp=vdp):
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


@flow(name=FLOW_NAME)
def backup_files():
    """Back up all files from URIS"""

    vdp = get_vdropbox()

    for backup_task in BACKUP_TASKS:
        name = f"{FLOW_NAME}.{backup_task.path[1:].replace('/', '_')}"
        task(name=name)(one_backup)(vdp, **backup_task.dict())
