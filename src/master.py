from datetime import date
from prefect import Flow
from prefect import Parameter
from prefect.utilities import logging

from global_utilities.log import log

from flights import flights
from flights import merge_flights_history
from money_lover import money_lover
from reports import reports

# Replace loguru log
logging.get_logger = lambda x: log

with Flow("do_all") as flow:
    mdate = Parameter("mdate")

    # Reports part
    df_trans = money_lover(mdate, export_data=True)
    reports(mdate, df_trans=df_trans)

    # Flights part
    flights(mdate)
    merge_flights_history(mdate)


def run_etl():
    """ Run the ETL for today """

    log.info("Starting vtasks")
    flow.run(mdate=date.today())
    log.info("End of vtasks")


if __name__ == "__main__":
    run_etl()
