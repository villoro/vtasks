from os import path

from common.logs import get_logger
from common.paths import get_path
from vcrypto import Cipher

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


def read_secret(secret, encoding="utf8"):
    """Retrives one encrypted secret"""
    logger = get_logger()
    cipher = _get_cipher()

    logger.info(f"Reading {secret=}")
    return cipher.get_secret(secret, encoding=encoding)


def write_secret(secret, value):
    """Retrives one encrypted secret"""
    logger = get_logger()
    cipher = _get_cipher()

    logger.info(f"Writing {secret=}")
    return cipher.save_secret(secret, value)


def export_secret(uri, secret_name, binary=False):
    """Export a secret from secrets.yaml"""

    logger = get_logger()
    logger.debug(f"Exporting {secret_name=} to {uri=}")

    if path.exists(uri):
        logger.info(f"Skipping secret export since {uri=} already exists")
        return False

    secret = read_secret(secret_name, encoding="utf8" if not binary else None)

    logger.info(f"Writing {secret_name=} to {uri=}")
    with open(uri, "wb" if binary else "w") as stream:
        stream.write(secret)
