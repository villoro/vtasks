from datetime import timedelta
from time import time

from prefect import Task

from slack import slack_state_handler
from utils import log


def vtask(func):
    """
        Custom decorator for prefect tasks.
        By default it has 3 retries and 10 seconds as retry_delay
    """

    class VTask(Task):

        # Set a default value just in case the task fails
        duration = "no run"

        def run(self, **kwargs):

            t0 = time()
            func(**kwargs)
            total_time = time() - t0

            if total_time < 60:
                self.duration = f"{total_time:.2f} s"
            else:
                self.duration = f"{total_time/60:.2f} min"

            log.info(f"{func.__name__} done in {self.duration}")

    return VTask(
        name=func.__name__,
        max_retries=3,
        retry_delay=timedelta(seconds=10),
        state_handlers=[slack_state_handler],
    )
