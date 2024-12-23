from os import path

from vcrypto import Cipher

from common.paths import get_path

CIPHER = None
CIPHER_KWARGS = {"secrets_file": get_path("secrets.yaml"), "environ_var_name": "VTASKS_TOKEN"}


def read_secret(key, encoding="utf8"):
    """Retrives one encrypted secret"""

    global CIPHER
    if CIPHER is None:
        CIPHER = Cipher(**CIPHER_KWARGS)

    return CIPHER.get_secret(key, encoding=encoding)


def write_secret(key, value):
    """Retrives one encrypted secret"""

    global CIPHER
    if CIPHER is None:
        CIPHER = Cipher(**CIPHER_KWARGS)

    return CIPHER.save_secret(key, value)


def export_secret(uri, secret_name, binary=False):
    """Export a secret from secrets.yaml"""

    if not path.exists(uri):
        with open(uri, "wb" if binary else "w") as file:
            secret = get_secret(secret_name, encoding="utf8" if not binary else None)
            file.write(secret)
