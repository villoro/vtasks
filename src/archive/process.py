from prefect import task

from utils import get_files_from_regex
from utils import get_path
from utils import get_vdropbox
from utils import log
from utils import read_yaml
from utils import timeit


FILE_REPLACES = get_path("src/archive/replaces.yaml")
REPLACES = read_yaml(FILE_REPLACES)


def rename_files(vdp, path, regex, output):
    """ Rename files based on regexs """

    for path, file, kwargs in get_files_from_regex(vdp, path, regex):

        origin = f"{path}/{file}"
        dest = f"{path}/{output.format(**kwargs)}"

        log.info(f"Archiving '{origin}'")

        vdp.dbx.files_copy(origin, dest)


@task
@timeit
def archive():

    vdp = get_vdropbox()

    for kwargs in REPLACES:
        rename_files(vdp, **kwargs)
