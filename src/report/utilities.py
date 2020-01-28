"""
    Utilities
"""

from collections import OrderedDict

import numpy as np

from v_palette import get_colors


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

    out = OrderedDict()

    for name, x in series.items():
        out[name] = serie_to_dict(x)

    return out
