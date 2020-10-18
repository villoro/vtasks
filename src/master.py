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

    # Add a dummy to force the order
    out = money_lover(mdate)
    reports(mdate, dummy=out)
    flights(mdate)
    merge_flights_history(mdate)


if __name__ == "__main__":

    log.info("Starting vtasks")
    flow.run(mdate=date.today())

    log.info("End of vtasks")
