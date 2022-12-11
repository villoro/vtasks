from datetime import date

from prefect import flow

from archive import archive
from backups import backup
from vbooks import vbooks


@flow(name="vtasks")
def main(mdate: date):
    archive()
    backup()
    vbooks()


if __name__ == "__main__":
    main(date.today())
