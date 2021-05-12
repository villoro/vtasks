from datetime import timedelta
from time import time

from prefect import Task

from slack import slack_state_handler
from utils import timeit


def vtask(func):
    """
        Custom decorator for prefect tasks.
        By default it has 3 retries and 10 seconds as retry_delay
    """

    class VTask(Task):
        def run(self, **kwargs):

            # TODO: improve the double timeing
            t0 = time()
            timeit(func)(**kwargs)
            total_time = time() - t0

            if total_time < 60:
                self.duration = f"{total_time:.2f} seconds"
            else:
                self.duration = f"{total_time/60:.2f} minutes"

    return VTask(
        name=func.__name__,
        max_retries=3,
        retry_delay=timedelta(seconds=10),
        state_handlers=[slack_state_handler],
    )
