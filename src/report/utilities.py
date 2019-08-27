"""
    Utilities
"""

import numpy as np

from v_palette import get_colors
from v_crypt import Cipher

from . import constants as c


cipher = Cipher(secrets_file=c.FILE_SECRETS, filename_master_password=c.FILE_MASTER_PASSWORD)


def get_secret(key):
    """ Retrives one encrypted secret """
    return cipher.get_secret(key)


def normalize_index(df1, df2):
    """ Force two dataframes to have the same indexs """

    index = df2.index if df2.shape[0] > df1.shape[0] else df1.index
    df1 = df1.reindex(index).fillna(0)
    df2 = df2.reindex(index).fillna(0)

    return df1, df2


def serie_to_dict(serie):
    """ Transform a serie to a dict """

    # If index is datetime transform to string
    if np.issubdtype(serie.index, np.datetime64):
        serie.index = serie.index.strftime("%Y-%m-%d")

    return serie.apply(lambda x: round(x, 2)).to_dict()


def series_to_dicts(series):
    """ Transform a dict with series to a dict of dicts """

    return {name: serie_to_dict(x) for name, x in series.items()}
