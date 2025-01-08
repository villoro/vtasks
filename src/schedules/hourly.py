from prefect import flow

from src.jobs.backups.main import backup_all
from src.jobs.dropbox.export_tables import export_dropbox_tables
from src.jobs.dropbox.money_lover import export_money_lover
from src.jobs.gcal.export import export_all_gcal
from src.jobs.gsheets.export_tables import export_gsheets_tables


@flow(name="vtasks.hourly")
def hourly():
    backup_all()
    export_dropbox_tables()
    export_money_lover()
    export_all_gcal()
    export_gsheets_tables()
