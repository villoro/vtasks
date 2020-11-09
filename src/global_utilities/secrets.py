"""
    Global utilities
"""

from vcrypto import Cipher

from .uos import get_path

cipher = Cipher(secrets_file=get_path("secrets.yaml"), environ_var_name="VTASKS_TOKEN")


def get_secret(key):
    """ Retrives one encrypted secret """
    return cipher.get_secret(key)
