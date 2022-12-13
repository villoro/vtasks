from prefect import flow

from .query import process_flow_runs, process_task_runs


@flow(name="vtasks.vprefect")
def vprefect():
    process_flow_runs()
    process_task_runs()
