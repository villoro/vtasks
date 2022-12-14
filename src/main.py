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


ENV = detect_env()


@flow(name="vtasks", tags=f"env:{ENV}")
def main(mdate: date):

    with tags(f"env:{ENV}"):
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
    main(mdate=date.today())
