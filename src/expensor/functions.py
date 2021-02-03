"""
    Utilities
"""

from collections import OrderedDict

import numpy as np
import pandas as pd

from . import constants as c


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


def time_average(dfi, months=12, exponential=False, center=False):
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

        df = dfi.rolling(months, min_periods=1, center=center)

    return df.mean().apply(lambda x: round(x, 2))


def smooth_serie(dfi,):
    """ Smooth a serie by doing a time_average 2 times """

    df = time_average(dfi, months=12, center=True)
    return time_average(df, months=6, center=True)


# TODO: delete this function
def _get_min_month_start(dfi):
    """ Extracts the min month_start of a dataframe """

    df = dfi.copy()

    if isinstance(df, pd.DataFrame) and (c.COL_DATE in df.columns):
        df = df.set_index(c.COL_DATE)

    return df.resample("MS").first().index.min()


# TODO: delete this function
def add_missing_months(df, mdate):
    """
        Adds missing months from the min month to mdate
        
        Args:
            df:     dataframe with date as index
            mdate:  date for the max month
    """

    min_date = _get_min_month_start(df)
    max_date = mdate.replace(day=1)

    return df.reindex(pd.date_range(min_date, max_date, freq="MS"))


def filter_by_date(dfs, mdate):
    """
        No data greater than mdate and complete missing months

        Args:
            dfs:    dict with dataframes
            mdate:  date of the report
    """

    # Get last date of month
    mdate = pd.to_datetime(mdate) + pd.tseries.offsets.MonthEnd(1)

    for name, df in dfs.items():

        # Filter out future data
        if df.index.name == c.COL_DATE:
            df = df[df.index <= mdate]

        dfs[name] = df

    return dfs
