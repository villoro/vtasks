from datetime import date
from datetime import datetime
from time import sleep

from prefect import flow
from prefect import tags

import utils as u

from archive import archive
from backups import backup
from battery import battery
from cryptos import crypto
from expensor import expensor
from gcal import gcal
from indexa import indexa
from money_lover import money_lover
from vbooks import vbooks
from vprefect import vprefect

SLEEP_SECONDS = 5


@flow(**u.get_prefect_args("vtasks"))
def main(mdate: date):

    vbooks()
    archive()
    battery()

    _gcal = gcal(mdate)
    backup(wait_for=[_gcal])

    _money_lover = money_lover()
    _crypto = crypto(mdate)
    _indexa = indexa(mdate)

    expensor(mdate, wait_for=[_money_lover, _crypto, indexa])

    vprefect()


if __name__ == "__main__":

    with tags(f"env:{u.detect_env()}"):
        main(mdate=date.today())

    print(f"Sleeping {SLEEP_SECONDS=}")
    sleep(SLEEP_SECONDS)
