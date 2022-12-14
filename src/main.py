from argparse import ArgumentParser
from datetime import date

from prefect import flow
from prefect import tags

from archive import archive
from backups import backup
from cryptos import crypto
from expensor import expensor
from gcal import gcal
from indexa import indexa
from money_lover import money_lover
from vbooks import vbooks
from vprefect import vprefect


def detect_env():
    """Detect if it is PRO environment"""

    parser = ArgumentParser()
    parser.add_argument("--pro", help="Wether it is PRO or not (DEV)", default=False, type=bool)

    args = parser.parse_args()

    if args.pro:
        return "prod"
    return "dev"


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

    vprefect()


if __name__ == "__main__":
    with tags(f"env:{detect_env()}"):
        main(mdate=date.today())
