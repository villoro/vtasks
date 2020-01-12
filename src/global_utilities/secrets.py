"""
    Global utilities
"""

from v_crypt import Cipher

from config import PATH_ROOT

FILE_SECRETS = f"{PATH_ROOT}secrets.yaml"
FILE_MASTER_PASSWORD = f"{PATH_ROOT}master.password"


cipher = Cipher(secrets_file=FILE_SECRETS, filename_master_password=FILE_MASTER_PASSWORD)


def get_secret(key):
    """ Retrives one encrypted secret """
    return cipher.get_secret(key)
