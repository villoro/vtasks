from vdropbox import Vdropbox

from common.secrets import read_secret
from common.logs import get_logger

VDROPBOX = None


def get_vdropbox():
    """Creates a vdropbox instance"""

    logger = get_logger()

    global VDROPBOX
    if VDROPBOX is None:
        logger.info("Initializing Vdropbox connector")
        VDROPBOX = Vdropbox(read_secret("DROPBOX_TOKEN"), log=get_logger())

    return VDROPBOX


def _scan_folder_by_regex(folder, regex, vdp=None, silent=False):
    """Get all files in a folder that match a regex"""

    logger = get_logger()
    if vdp is None:
        vdp = get_vdropbox()

    if not silent:
        logger.info(f"Getting files in {folder=} that match {regex=}")

    out = []
    for file in vdp.ls(folder):
        if match := re.match(regex, file):
            out.append((folder, file, match.groupdict()))

    if not silent:
        logger.info(f"{len(out)} files found in {folder=} that match {regex=}")
    return out


def scan_folder_and_subfolders_by_regex(path, regex, vdp=None):
    """Get all files based on a path and a regex for the filename"""

    logger = get_logger()
    if vdp is None:
        vdp = get_vdropbox()

    logger.info(f"Getting all files in {path=} that match {regex=}")

    # No '*' return all files directly
    if not path.endswith("/*"):
        return _scan_folder_by_regex(path, regex, vdp=vdp, silent=True)

    # Query all folders
    base_path = path.replace("/*", "")

    out = []
    for file in vdp.ls(base_path):
        if "." not in file:
            sub_path = f"{base_path}/{file}"
            out += _scan_folder_by_regex(sub_path, regex, vdp=vdp, silent=True)

    logger.info(f"{len(out)} files found in {path=} that match {regex=}")
    return out