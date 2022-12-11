from datetime import date

from prefect import flow

from archive import archive
from backups import backup
from vbooks import vbooks
from gcal import gcal
from money_lover import money_lover


@flow(name="vtasks")
def main(mdate: date):
    archive()
    vbooks()

    _gcal = gcal(mdate)
    backup(wait_for=[_gcal])

    _money_lover = money_lover()


if __name__ == "__main__":
    main(date.today())
