from os import path

from vcrypto import Cipher

from common.paths import get_path
from common.logs import get_logger

CIPHER = None
CIPHER_KWARGS = {
    "secrets_file": get_path("secrets.yaml"),
    "environ_var_name": "VTASKS_TOKEN",
}


def _get_cipher():
    logger = get_logger()

    global CIPHER
    if CIPHER is None:
        logger.info(f"Initializing Cipher with {CIPHER_KWARGS=}")
        return Cipher(**CIPHER_KWARGS)

    return CIPHER


def read_secret(key, encoding="utf8"):
    """Retrives one encrypted secret"""

    logger = get_logger()
    logger.info(f"Reading {secret=}")
    return _get_cipher().get_secret(key, encoding=encoding)


def write_secret(key, value):
    """Retrives one encrypted secret"""

    logger = get_logger()
    logger.info(f"Writing {secret=}")
    return _get_cipher().save_secret(key, value)


def export_secret(uri, secret_name, binary=False):
    """Export a secret from secrets.yaml"""

    logger = get_logger()
    logger.debug(f"Exporting {secret_name=} to {uri=}")

    if path.exists(uri):
        logger.info(f"Skipping secret export since {uri=} already exists")
        return False

    secret = get_secret(secret_name, encoding="utf8" if not binary else None)

    logger.info(f"Writing {secret_name=} to {uri=}")
    with open(uri, "wb" if binary else "w") as stream:
        stream.write(secret)
