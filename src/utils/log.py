"""
    Log utilities using loguru
"""

import sys

from datetime import date

from loguru import logger as log

from .uos import get_path


log_path = f"logs/{date.today():%Y_%m}/{date.today():%Y_%m_%d}.log"

CONFIG = {
    "handlers": [
        {"sink": sys.stdout, "level": "INFO"},
        {"sink": get_path(log_path), "level": "INFO",},
    ]
}

log.configure(**CONFIG)
log.enable("vtasks")
