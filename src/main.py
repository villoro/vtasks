import subprocess

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
from utils import get_secret
from vbooks import vbooks
from vprefect import vprefect

PREFECT_LOGIN = "prefect cloud login --key {} --workspace villoro/vtasks"


def prefect_login():
    login = PREFECT_LOGIN.format(get_secret("PREFECT_TOKEN"))
    subprocess.run(login, shell=True)


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

    vbooks()
    archive()

    _gcal = gcal(mdate)
    backup(wait_for=[_gcal])

    _money_lover = money_lover()
    _crypto = crypto(mdate)
    _indexa = indexa(mdate)

    expensor(mdate, wait_for=[_money_lover, _crypto, indexa])

    vprefect()


if __name__ == "__main__":

    prefect_login()

    with tags(f"env:{detect_env()}"):
        main(mdate=date.today())
