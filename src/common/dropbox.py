from vdropbox import Vdropbox

from common.secrets import read_secret
from common.logs import get_logger

VDROPBOX = None


def get_vdropbox():
    """Creates a vdropbox instance"""

    global VDROPBOX
    if VDROPBOX is None:
        VDROPBOX = Vdropbox(read_secret("DROPBOX_TOKEN"), log=get_logger())

    return VDROPBOX


def get_files_that_match(folder, regex, vdp=None):
    """Get all files in a folder that match a regex"""

    if vdp is None:
        vdp = get_vdropbox()

    out = []

    for file in vdp.ls(folder):
        match = re.match(regex, file)

        if match:
            out.append((folder, file, match.groupdict()))

    return out


def get_all_files_from_regex(path, regex, vdp=None):
    """Get all files based on a path and a regex for the filename"""

    if vdp is None:
        vdp = get_vdropbox()

    # No '*' return all files directly
    if not path.endswith("/*"):
        return get_files_that_match(vdp, path, regex)

    # Query all folders
    base_path = path.replace("/*", "")

    out = []
    for file in vdp.ls(base_path):
        if "." not in file:
            out += get_files_that_match(vdp, f"{base_path}/{file}", regex)

    return out
