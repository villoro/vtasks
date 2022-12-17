from .models import BackupTask
from .models import CopyTask

COPY_TASKS = [
    CopyTask(origin="/Aplicaciones/KeePass/General.kdbx", destination="/Casa/KeePass/Arnau.kdbx")
]

BACKUP_TASKS = [
    BackupTask(path="/Aplicaciones/expensor", regex=r"[\w\s]+\.(yaml|yml)"),
    BackupTask(path="/Aplicaciones/FreeFileSync", regex=r"[\w\s]+\.ffs_gui"),
    BackupTask(path="/Aplicaciones/gcalendar", regex=r"calendar.parquet"),
    BackupTask(path="/Aplicaciones/KeePass", regex=r"[\w\s]+\.kdbx"),
    BackupTask(path="/Aplicaciones/vtasks", regex=r"[\w_]+.parquet"),
    BackupTask(path="/Casa/KeePass", regex=r"[\w\s]+\.kdbx"),
]
