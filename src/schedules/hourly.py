from prefect import flow
from prefect import tags

from src.jobs.backups.main import backup_all
from src.jobs.crypto.main import crypto
from src.jobs.dropbox.export_tables import export_dropbox_tables
from src.jobs.dropbox.money_lover import export_money_lover
from src.jobs.gcal.export import export_all_gcal
from src.jobs.gsheets.export_tables import export_gsheets_tables
from src.jobs.indexa.main import indexa_all
from src.vdbt.python.run import run_dbt


@flow(name="vtasks.hourly.maintain")
def maintain():
    backup_all()


@flow(name="vtasks.hourly.updates")
def updates():
    crypto()
    indexa_all()


@flow(name="vtasks.hourly.export")
def export():
    export_dropbox_tables()
    export_money_lover()
    export_all_gcal()
    export_gsheets_tables()


@flow(name="vtasks.hourly")
def hourly():
    with tags("env:pro"):
        maintain()
        updates()
        export()
        run_dbt()


if __name__ == "__main__":
    hourly()
