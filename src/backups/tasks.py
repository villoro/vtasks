from .models import BackupTask
from .models import CopyTask

COPY_TASKS = [
    CopyTask(origin="/Aplicaciones/KeePass/General.kdbx", destination="/Casa/KeePass/Arnau.kdbx")
]

BACKUP_TASKS = [
    BackupTask(path="/Aplicaciones/FreeFileSync", regex=r"[\w\s]+\.ffs_gui"),
    BackupTask(path="/Aplicaciones/KeePass", regex=r"[\w\s]+\.kdbx"),
    BackupTask(path="/Casa/KeePass", regex=r"[\w\s]+\.kdbx"),
]
