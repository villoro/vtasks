import functools

from time import time

from .log import log


def timeit(func):
    """ Timing decorator """

    @functools.wraps(func)
    def timed_execution(*args, **kwa):
        """ Outputs the execution time of a function """
        t0 = time()
        result = func(*args, **kwa)

        total_time = time() - t0

        if total_time < 60:
            log.info(f"{func.__name__} done in {total_time:.2f} seconds")
        else:
            log.info(f"{func.__name__} done in {total_time/60:.2f} minutes")

        return result

    return timed_execution
