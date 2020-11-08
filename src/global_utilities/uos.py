from pathlib import Path

# Base path of the repo.
# It need to go 3 times up since this file has the following relative path:
# 	/src/global_utilities/uos.py
PATH_ROOT = Path(__file__).parent.parent.parent


def get_path(path_relative):
    """ Returns absolute path using PATH_ROOT """

    return str(PATH_ROOT / path_relative)
