from prefect import flow

import utils as u

from .backup_files import backup_files
from .clean_backups import clean_backups
from .copy import copy


@flow(**u.get_prefect_args("vtasks.backup"))
def backup():
    backup_files()
    clean_backups()
    copy()
