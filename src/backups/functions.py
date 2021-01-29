import re

from datetime import date

from utils import get_vdropbox

YEAR = f"{date.today():%Y}"
MONTH = f"{date.today():%Y_%m}"
DAY = f"{date.today():%Y_%m_%d}"


def get_files_to_backup(vdp, uri):

    # Extract path and regex
    path = uri.split("/")
    regex = path.pop()
    path = "/".join(path)

    filenames = [x for x in vdp.ls(path) if re.search(regex, x)]

    return path, filenames
