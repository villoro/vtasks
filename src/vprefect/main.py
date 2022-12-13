from prefect import flow

from .query import process_flow_runs, process_task_runs
from .report import create_report


@flow(name="vtasks.vprefect")
def vprefect():
    process_flow_runs()
    process_task_runs()
    create_report()
