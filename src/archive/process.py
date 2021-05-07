from datetime import date
from datetime import timedelta


from utils import get_files_from_regex
from utils import get_path
from utils import get_secret
from utils import get_vdropbox
from utils import log
from utils import read_yaml
from utils import vtask

PATH_OPERATIONS = get_path("src/archive/operations.yaml")
OPERATIONS = read_yaml(PATH_OPERATIONS)

MONTHS = {
    "Enero": 1,
    "Febrero": 2,
    "Marzo": 3,
    "Abril": 4,
    "Mayo": 5,
    "Junio": 6,
    "Julio": 7,
    "Agosto": 8,
    "Setiembre": 9,
    "Octubre": 10,
    "Noviembre": 11,
    "Diciembre": 12,
}

MONTHS = {i: str(x).zfill(2) for i, x in MONTHS.items()}


def rename_files(vdp, path, regex, output):
    """ Rename files based on regexs """

    for path, file, kwargs in get_files_from_regex(vdp, path, regex):

        # Get month from month_text if needed
        month_text = kwargs.get("month_text")
        if month_text:
            kwargs["month"] = MONTHS.get(month_text, month_text)

        # Get month from quarter if needed
        quarter = kwargs.get("quarter")
        if quarter:
            kwargs["month"] = str(int(quarter) * 3).zfill(2)

        origin = f"{path}/{file}"
        dest = output.format(**kwargs)

        log.info(f"Archiving '{origin}'")
        vdp.mv(origin, dest)


def extract_files(vdp, path, regex, output, pwd, kwargs):
    """ Extract files based on regexs """

    # Evaluate as python expresions
    kwargs = {key: eval(val) for key, val in kwargs.items()}

    for path, file, _ in get_files_from_regex(vdp, path, regex):

        origin = f"{path}/{file}"
        dest = output.format(**kwargs)

        log.info(f"Extracting '{origin}'")

        # Extract from zip
        data = vdp.read_zip(origin, pwd=get_secret(pwd).encode())

        # Export as a file
        vdp.write_file(data, dest, as_binary=True)

        vdp.delete(origin)


@vtask
def archive():

    vdp = get_vdropbox()

    # Rename some files
    for name, kwargs in OPERATIONS["renames"].items():
        log.info(f"Archiving renames for '{name}'")
        rename_files(vdp, **kwargs)

    # Extract some files
    for name, kwargs in OPERATIONS["extractions"].items():
        log.info(f"Archiving extracts for '{name}'")
        extract_files(vdp, **kwargs)
