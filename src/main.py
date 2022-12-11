from datetime import date

from prefect import flow

from archive import archive
from backups import backup
from cryptos import crypto
from vbooks import vbooks
from gcal import gcal
from money_lover import money_lover
from expensor import expensor


@flow(name="vtasks")
def main(mdate: date):
    archive()
    vbooks()

    _gcal = gcal(mdate)
    backup(wait_for=[_gcal])

    _money_lover = money_lover()
    _crypto = crypto(mdate)

    expensor(mdate, wait_for=[_money_lover, _crypto])  # _indexa


if __name__ == "__main__":
    main(date.today())
