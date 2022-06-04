import sys
from loguru import logger as log

CONFIG = {
    "handlers": [
        {"sink": sys.stdout, "level": "ERROR"},
        {"sink": "logs/info.log", "level": "INFO"},
        {"sink": "logs/debug.log", "level": "DEBUG"},
    ]
}

log.configure(**CONFIG)
log.enable("metadata")
