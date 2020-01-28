"""
    Log utilities using loguru
"""

from datetime import date

from loguru import logger as log

from config import PATH_ROOT

# fmt: off
CONFIG = {
    "handlers": [
    	{"sink": f"{PATH_ROOT}logs/{date.today():%Y_%m_%d}.log", "level": "INFO"}
    ]
}
# fmt: om

log.configure(**CONFIG)

log.enable("vtasks")
