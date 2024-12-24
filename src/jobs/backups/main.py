from prefect import flow

from jobs.backups.backup_files import backup_files
from jobs.backups.clean_backups import clean_backups
from jobs.backups.copy import copy


@flow(name="vtasks.backups")
def backup():
    backup_files()
    clean_backups()
    copy()
