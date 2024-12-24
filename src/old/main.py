from datetime import date

import utils as u
from archive import archive
from backups import backup
from battery import battery
from cryptos import crypto
from expensor import expensor
from gcal import gcal
from indexa import indexa
from money_lover import money_lover
from prefect import flow
from prefect import tags
from vprefect import vprefect
from vprefect.fix_status import complete_uncompleted_flow_runs


@flow(**u.get_prefect_args("vtasks"))
def vtasks(mdate: date):
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
        vtasks(mdate=date.today())

    with tags(f"env:{u.detect_env()}"):
        complete_uncompleted_flow_runs()
