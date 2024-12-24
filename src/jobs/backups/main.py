from prefect import flow

from src.jobs.backups.backup_files import backup_files
from src.jobs.backups.clean_backups import clean_backups
from src.jobs.backups.copy import copy


@flow(name="vtasks.backups")
def backup():
    backup_files()
    clean_backups()
    copy()