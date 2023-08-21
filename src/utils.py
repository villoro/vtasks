import re
import sys
import yaml

from argparse import ArgumentParser
from collections import OrderedDict
from datetime import datetime
from os import path
from pathlib import Path

import jinja2
import numpy as np
import pandas as pd

from prefect import get_run_logger
from prefect.exceptions import MissingContextError
from scipy.signal import savgol_filter
from tsmoothie.smoother import ConvolutionSmoother
from vcrypto import Cipher
from vdropbox import Vdropbox


# Base path of the repo.
# It need to go 2 times up since this file has the following relative path:
#   /src/utils.py
PATH_ROOT = Path(__file__).parent.parent


def detect_env():
    """Detect if it is PROD environment"""

    parser = ArgumentParser()
    parser.add_argument("-f", help="Dummy argument not meant to be used")
    parser.add_argument("--env", help="Wether it is PRO or not (DEV)", default="dev", type=str)

    args = parser.parse_args()

    return args.env


def get_log():
    if detect_env() in ["pro", "prod"]:
        return get_run_logger()

    try:
        return get_run_logger()
    except MissingContextError:
        from loguru import logger

        logger.configure(handlers=[{"sink": sys.stdout, "level": "INFO"}])
        logger.enable("vtasks")

        logger.debug("Switching to loguru")
        return logger


def get_path(path_relative):
    """Returns absolute path using PATH_ROOT"""

    path_out = PATH_ROOT

    for x in path_relative.split("/"):
        path_out /= x

    return str(path_out)


CIPHER = None
CIPHER_KWARGS = {"secrets_file": get_path("secrets.yaml"), "environ_var_name": "VTASKS_TOKEN"}


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
        with open(uri, mode) as file:
            file.write(get_secret(secret_name, encoding=encoding))


VDROPBOX = None


def get_vdropbox():
    """Creates a vdropbox instance"""

    global VDROPBOX
    if VDROPBOX is None:
        VDROPBOX = Vdropbox(get_secret("DROPBOX_TOKEN"), log=get_log())

    return VDROPBOX


def get_files_that_match(vdp, folder, regex):
    """Get all files in a folder that match a regex"""

    out = []

    for file in vdp.ls(folder):
        match = re.match(regex, file)

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


def serie_to_dict(serie):
    """Transform a serie to a dict"""

    # If index is datetime transform to string
    if np.issubdtype(serie.index, np.datetime64):
        serie.index = serie.index.strftime("%Y-%m-%d")

    return serie.apply(lambda x: round(x, 2)).to_dict()


def series_to_dicts(series):
    """Transform a dict with series to a dict of dicts"""

    out = OrderedDict()

    for name, x in series.items():
        out[name] = serie_to_dict(x)

    return out


def convolution_smooth(serie, window):
    """Apply convolution smoother to a 1D serie"""
    smoother = ConvolutionSmoother(window_len=window, window_type="ones")
    smoother.smooth(serie)
    return smoother.smooth_data[0]


def smooth_serie(
    serie, savgol_window=35, savgol_polyorder=5, savgol_mode="nearest", convolution_window=3
):
    """Smooth a serie by doing a savgol filter followed by a convolution_smooth"""

    savgol = savgol_filter(serie, savgol_window, savgol_polyorder, mode=savgol_mode)
    return convolution_smooth(savgol, convolution_window)


def get_prefect_args(name):
    return dict(name=name, flow_run_name=f"{name}-{datetime.now():%Y-%m-%d %H:%M}")
