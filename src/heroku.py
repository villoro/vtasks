import os

from apscheduler.schedulers.blocking import BlockingScheduler
from global_utilities.log import log


schedule = BlockingScheduler()


@schedule.scheduled_job("interval", hours=1)
def schedule_flow():
    os.system("poetry run python src/master.py")


if __name__ == "__main__":
	log.info("Scheduling vtasks")
    schedule.start()
