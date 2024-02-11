from .models import BackupTask
from .models import CopyTask

COPY_TASKS = [
    CopyTask(origin="/Aplicaciones/KeePass/General.kdbx", destination="/Groups/Casa/KeePass/Arnau.kdbx")
    CopyTask(origin="/Aplicaciones/KeePass/General.kdbx", destination="/Groups/KeePass Cocco/Arnau.kdbx")
]

BACKUP_TASKS = [
    BackupTask(path="/Aplicaciones/FreeFileSync", regex=r"[\w\s]+\.ffs_gui"),
    BackupTask(path="/Aplicaciones/KeePass", regex=r"[\w\s]+\.kdbx"),
    BackupTask(path="/Groups/Casa/KeePass", regex=r"[\w\s]+\.kdbx"),
    BackupTask(path="/Groups/KeePass Cocco", regex=r"[\w\s]+\.kdbx"),
]
