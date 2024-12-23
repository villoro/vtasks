from pydantic import BaseModel


class CopyTask(BaseModel):
    origin: str
    destination: str

    def copy(self, vdp):
        content = vdp.read_file(self.origin, as_binary=True)
        vdp.write_file(content, self.destination, as_binary=True)


class BackupTask(BaseModel):
    path: str
    regex: str
