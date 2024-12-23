from prefect import flow

import utils as u

from .query import process_flow_runs
from .query import process_task_runs
from .report import create_report


@flow(**u.get_prefect_args("vtasks.vprefect"))
def vprefect():
    process_flow_runs()
    process_task_runs()
    create_report()
