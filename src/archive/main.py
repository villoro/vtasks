from datetime import date
from datetime import timedelta

from prefect import flow, task

import utils as u

PATH_OPERATIONS = u.get_path("src/archive/operations.yaml")
OPERATIONS = u.read_yaml(PATH_OPERATIONS)

MONTHS = {
    "Enero": 1,
    "Febrero": 2,
    "Marzo": 3,
    "Abril": 4,
    "Mayo": 5,
    "Junio": 6,
    "Julio": 7,
    "Agosto": 8,
    "Septiembre": 9,
    "Octubre": 10,
    "Noviembre": 11,
    "Diciembre": 12,
}

MONTHS = {i: str(x).zfill(2) for i, x in MONTHS.items()}


@task(name="vtasks.archive.rename_files")
def rename_files(vdp, path, regex, output):
    """Rename files based on regexs"""

    log = u.get_log()

    for path, file, kwargs in u.get_files_from_regex(vdp, path, regex):
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


@task(name="vtasks.archive.extract_files")
def extract_files(vdp, path, regex, output, pwd, kwargs):
    """Extract files based on regexs"""

    log = u.get_log()

    # Evaluate as python expresions
    kwargs = {key: eval(val) for key, val in kwargs.items()}

    for path, file, _ in u.get_files_from_regex(vdp, path, regex):
        origin = f"{path}/{file}"
        dest = output.format(**kwargs)

        log.info(f"Extracting '{origin}'")

        # Extract from zip
        data = vdp.read_zip(origin, pwd=u.get_secret(pwd).encode())

        # Export as a file
        vdp.write_file(data, dest, as_binary=True)

        vdp.delete(origin)


@flow(**u.get_prefect_args("vtasks.archive"))
def archive():
    log = u.get_log()
    vdp = u.get_vdropbox()

    # Rename some files
    for name, kwargs in OPERATIONS["renames"].items():
        log.info(f"Archiving renames for '{name}'")
        rename_files(vdp, **kwargs)

    # Extract some files
    for name, kwargs in OPERATIONS["extractions"].items():
        log.info(f"Archiving extracts for '{name}'")
        extract_files(vdp, **kwargs)
