from prefect import flow

from .backup_files import backup_files
from .clean_backups import clean_backups


@flow(name="vtasks.backup")
def backup():
    backup_files()
    clean_backups()
