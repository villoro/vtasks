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


def time_average(dfi, months=12, exponential=False):
    """
        Do some time average
        
        Args:
            dfi:            input dataframe (or series)
            months:         num of months for the average
            exponential:    whether to use EWM or simple rolling
    """

    # Exponential moving average
    if exponential:
        # No negative values
        months = max(0, months)

        df = dfi.ewm(span=months, min_periods=0, adjust=False, ignore_na=False)

    # Regular moving average
    else:
        # One month at least
        months = max(1, months)

        df = dfi.rolling(months, min_periods=1)

    return df.mean().apply(lambda x: round(x, 2))


def get_min_month_start(dfi):
    """ Extracts the min month_start of a dataframe """

    df = dfi.copy()

    if c.COL_DATE in df.columns:
        df = df.set_index(c.COL_DATE)

    return df.resample("MS").first().index.min()
