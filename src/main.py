from datetime import date

from prefect import flow

from archive import archive
from backups import backup


@flow(name="vtasks")
def main(mdate: date):
    archive()
    backup()


if __name__ == "__main__":
    main(date.today())
