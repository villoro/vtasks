from prefect import flow
from prefect import task

from vtasks.common.dropbox import get_vdropbox
from vtasks.jobs.backups.tasks import COPY_TASKS

FLOW_NAME = "backups.copy"


@flow(name=FLOW_NAME)
def copy():
    vdp = get_vdropbox()

    for copy_task in COPY_TASKS:
        origin = copy_task.origin.replace("/", "_")
        destination = copy_task.destination.replace("/", "_")

        name = f"{FLOW_NAME}.{origin}-{destination}"
        task(name=name)(copy_task.copy)(vdp)


if __name__ == "__main__":
    copy()
