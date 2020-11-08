"""
    Global utilities
"""

from vcrypto import Cipher

from .uos import get_path

FILE_SECRETS = get_path("secrets.yaml")
FILE_MASTER_PASSWORD = get_path("master.password")


cipher = Cipher(secrets_file=FILE_SECRETS, filename_master_password=FILE_MASTER_PASSWORD)


def get_secret(key):
    """ Retrives one encrypted secret """
    return cipher.get_secret(key)
