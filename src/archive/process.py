from datetime import date
from datetime import timedelta

from prefect import task

from utils import get_files_from_regex
from utils import get_path
from utils import get_secret
from utils import get_vdropbox
from utils import log
from utils import read_yaml
from utils import timeit


PATH_OPERATIONS = get_path("src/archive/operations.yaml")
OPERATIONS = read_yaml(PATH_OPERATIONS)


def rename_files(vdp, path, regex, output):
    """ Rename files based on regexs """

    for path, file, kwargs in get_files_from_regex(vdp, path, regex):

        origin = f"{path}/{file}"
        dest = f"{path}/{output.format(**kwargs)}"

        log.info(f"Archiving '{origin}'")

        vdp.dbx.files_copy(origin, dest)
        vdp.delete(origin)


def extract_files(vdp, path, regex, output, pwd, kwargs):
    """ Extract files based on regexs """

    # Evaluate as python expresions
    kwargs = {key: eval(val) for key, val in kwargs.items()}

    for path, file, _ in get_files_from_regex(vdp, path, regex):

        origin = f"{path}/{file}"
        dest = f"{path}/{output.format(**kwargs)}"

        log.info(f"Extracting '{origin}'")

        # Extract from zip
        data = vdp.read_zip(origin, pwd=get_secret(pwd).encode())

        # Export as a file
        vdp.write_file(data, dest, as_binary=True)

        vdp.delete(origin)


@task
@timeit
def archive():

    vdp = get_vdropbox()

    # Rename some files
    for kwargs in OPERATIONS["renames"]:
        rename_files(vdp, **kwargs)

    # Extract some files
    for kwargs in OPERATIONS["extractions"]:
        extract_files(vdp, **kwargs)
