"""
    Utilities for pandas dataframes
"""

from v_crypt import Cipher

from . import constants as c


cipher = Cipher(secrets_file=c.FILE_SECRETS, filename_master_password=c.FILE_MASTER_PASSWORD)


def get_secret(key):
    """ Retrives one encrypted secret """
    return cipher.get_secret(key)
