from prefect import flow

from vtasks.jobs.backups.backup_files import backup_files
from vtasks.jobs.backups.clean_backups import clean_backups
from vtasks.jobs.backups.copy import copy


@flow(name="backups")
def backup_all():
    backup_files()
    clean_backups()
    copy()


if __name__ == "__main__":
    backup_all()
