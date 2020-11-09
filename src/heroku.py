from datetime import date

from apscheduler.schedulers.blocking import BlockingScheduler

from global_utilities.log import log
from master import flow


schedule = BlockingScheduler()


@schedule.scheduled_job("interval", minutes=1)
def schedule_flow():
    """ Schedule flow """

    log.info("Starting vtasks")
    flow.run(mdate=date.today())

    log.info("End of vtasks")


if __name__ == "__main__":
    schedule.start()
