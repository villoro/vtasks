from prefect import flow, task

from common.dropbox import get_vdropbox
from jobs.backups.tasks import COPY_TASKS

FLOW_NAME = "vtasks.backups.copy"


@flow(name=FLOW_NAME)
def copy():
    vdp = get_vdropbox()

    for copy_task in COPY_TASKS:
        origin = copy_task.origin.replace("/", "_")
        destination = copy_task.destination.replace("/", "_")

        name = f"{FLOW_NAME}.{origin}-{destination}"
        task(name=name)(copy_task.copy)(vdp)
