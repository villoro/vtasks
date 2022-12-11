from datetime import date

from prefect import flow

from archive import archive


@flow(name="vtasks")
def main(mdate: date):
    archive()


if __name__ == "__main__":
    main(date.today())
