import functools
import os
import re
import sys
import yaml

from argparse import ArgumentParser
from datetime import date
from datetime import timedelta
from os import path
from pathlib import Path
from time import time

import jinja2
import pandas as pd

from loguru import logger as log

from vcrypto import Cipher
from vdropbox import Vdropbox


# Base path of the repo.
# It need to go 2 times up since this file has the following relative path:
#   /src/utils.py
PATH_ROOT = Path(__file__).parent.parent

LOG_PATH = f"logs/{date.today():%Y_%m}/{date.today():%Y_%m_%d}.log"
LOG_PATH_DROPBOX = f"/Aplicaciones/vtasks/{LOG_PATH}"


def get_path(path_relative):
    """Returns absolute path using PATH_ROOT"""

    path_out = PATH_ROOT

    for x in path_relative.split("/"):
        path_out /= x

    return str(path_out)


CIPHER = None
CIPHER_KWARGS = {"secrets_file": get_path("secrets.yaml"), "environ_var_name": "VTASKS_TOKEN"}


CONFIG = {
    "handlers": [
        {"sink": sys.stdout, "level": "INFO"},
        {"sink": get_path(LOG_PATH), "level": "INFO"},
    ]
}


log.configure(**CONFIG)
log.enable("vtasks")


def is_pro():
    """Check if working in PRO"""

    return os.environ.get("VTASKS_ENV", "False") == "True"


def detect_env():
    """Detect if it is PRO environment"""

    parser = ArgumentParser()
    parser.add_argument("--pro", help="Wether it is PRO or not (DEV)", default=False, type=bool)

    args = parser.parse_args()

    os.environ["VTASKS_ENV"] = str(args.pro)

    if args.pro:
        log.info("Working on PRO")
    else:
        log.info("Working on DEV")


def get_secret(key, encoding="utf8"):
    """Retrives one encrypted secret"""

    global CIPHER
    if CIPHER is None:
        CIPHER = Cipher(**CIPHER_KWARGS)

    return CIPHER.get_secret(key, encoding=encoding)


def save_secret(key, value):
    """Retrives one encrypted secret"""

    global CIPHER
    if CIPHER is None:
        CIPHER = Cipher(**CIPHER_KWARGS)

    return CIPHER.save_secret(key, value)


def export_secret(uri, secret_name, binary=False):
    """Export a secret from secrets.yaml"""

    if binary:
        mode = "wb"
        encoding = None

    else:
        mode = "w"
        encoding = "utf8"

    if not path.exists(uri):

        log.info(f"Exporting '{uri}'")

        with open(uri, mode) as file:
            file.write(get_secret(secret_name, encoding=encoding))


VDROPBOX = None


def get_vdropbox():
    """Creates a vdropbox instance"""

    global VDROPBOX
    if VDROPBOX is None:
        VDROPBOX = Vdropbox(get_secret("DROPBOX_TOKEN"), log=log)

    return VDROPBOX


def get_files_that_match(vdp, folder, regex):
    """Get all files in a folder that match a regex"""

    out = []

    for file in vdp.ls(folder):
        match = re.search(regex, file)

        if match:
            out.append((folder, file, match.groupdict()))

    return out


def get_files_from_regex(vdp, path, regex):
    """Get all files based on a path and a regex for the filename"""

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


def timeit(func):
    """Timing decorator"""

    @functools.wraps(func)
    def timed_execution(*args, **kwa):
        """Outputs the execution time of a function"""
        t0 = time()
        result = func(*args, **kwa)

        total_time = time() - t0

        if total_time < 60:
            log.info(f"{func.__name__} done in {total_time:.2f} seconds")
        else:
            log.info(f"{func.__name__} done in {total_time/60:.2f} minutes")

        return result

    return timed_execution


def render_jinja_template(template_name, data):
    """Render a jinja2 template"""

    # Set up jinja to render parent templates and retrive template
    loader = jinja2.FileSystemLoader(get_path("templates"))
    template = jinja2.Environment(loader=loader).get_template(template_name)

    # Render the template
    return template.render(**data)


def read_yaml(filename, encoding="utf8"):
    """Read a yaml file"""

    with open(filename, "r", encoding=encoding) as file:
        return yaml.safe_load(file)
