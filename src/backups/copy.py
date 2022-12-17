from pydantic import BaseModel
from prefect import task

from utils import get_vdropbox


class Task(BaseModel):
    origin: str
    destination: str

    def copy(self, vdp):
        content = vdp.read_file(self.origin, as_binary=True)
        vdp.write_file(content, self.destination, as_binary=True)


TASKS = [Task(origin="/Aplicaciones/KeePass/General.kdbx", destination="/Casa/KeePass/Arnau.kdbx")]


@task(name="vtasks.backup.copy")
def copy():
    vdp = get_vdropbox()

    for task in TASKS:
        task.copy(vdp)
