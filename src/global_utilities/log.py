"""
    Log utilities using loguru
"""

import sys
from datetime import date

from loguru import logger as log

from config import PATH_ROOT


CONFIG = {
    "handlers": [
        {"sink": sys.stdout, "level": "DEBUG"},
        {"sink": f"{PATH_ROOT}logs/{date.today():%Y_%m_%d}.log", "level": "INFO"},
    ]
}

log.configure(**CONFIG)
log.enable("vtasks")
