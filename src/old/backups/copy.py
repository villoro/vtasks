from prefect import task

from .tasks import COPY_TASKS
from utils import get_vdropbox


@task(name="vtasks.backup.copy")
def copy():
    vdp = get_vdropbox()

    for task in COPY_TASKS:
        task.copy(vdp)
