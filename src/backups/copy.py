from prefect import task

from utils import get_vdropbox
from .tasks import COPY_TASKS


@task(name="vtasks.backup.copy")
def copy():
    vdp = get_vdropbox()

    for task in COPY_TASKS:
        task.copy(vdp)
