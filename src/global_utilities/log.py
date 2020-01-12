"""
    Log utilities using loguru
"""

from datetime import date

from loguru import logger as log

log.configure(handlers=[{"sink": f"logs/{date.today()}.log"}])
log.enable("vtasks")
