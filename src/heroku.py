from datetime import date

from apscheduler.schedulers.blocking import BlockingScheduler
from master import flow


def schedule_flow():
    """ Schedule flow """
    flow.run(mdate=date.today())


schedule = BlockingScheduler()


@schedule.scheduled_job("interval", minutes=5)
def timed_job():
    print("This job is run every three minutes.")


if __name__ == "__main__":
    schedule.start()
