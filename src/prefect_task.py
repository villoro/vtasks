from datetime import timedelta

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
            timeit(func)(**kwargs)

    return VTask(
        name=func.__name__,
        max_retries=3,
        retry_delay=timedelta(seconds=10),
        state_handlers=[slack_state_handler],
    )
