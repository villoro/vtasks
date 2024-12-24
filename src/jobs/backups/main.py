from jobs.backups.backup_files import backup_files
from jobs.backups.clean_backups import clean_backups
from jobs.backups.copy import copy
from prefect import flow


@flow(name="vtasks.backups")
def backup():
    backup_files()
    clean_backups()
    copy()
