from datetime import date

from prefect import flow

from archive import archive
from backups import backup
from cryptos import crypto
from expensor import expensor
from gcal import gcal
from indexa import indexa
from money_lover import money_lover
from vbooks import vbooks


@flow(name="vtasks")
def main(mdate: date):
    archive()
    vbooks()

    _gcal = gcal(mdate)
    backup(wait_for=[_gcal])

    _money_lover = money_lover()
    _crypto = crypto(mdate)
    _indexa = indexa(mdate)

    expensor(mdate, wait_for=[_money_lover, _crypto, indexa])


if __name__ == "__main__":
    main(date.today())
