from collections import OrderedDict

import numpy as np
import pandas as pd

from . import constants as c


def resample(df, period, mdate):
    """Resample and fill missing periods"""

    index = pd.date_range(df.index.min(), mdate, freq=period)
    df = df.resample(period).sum().reindex(index).fillna(0)

    # If working with years, cast the index to integer
    if period == "YS":
        df.index = df.index.year

    return df


def filter_by_date(dfs_in, mdate):
    """
    No data greater than mdate and complete missing months

    Args:
        dfs_in: dict with dataframes
        mdate:  date of the report
    """

    dfs = dfs_in.copy()

    # Get last date of month
    mdate = pd.to_datetime(mdate) + pd.tseries.offsets.MonthEnd(1)

    for name, df in dfs.items():

        # Filter out future data
        if df.index.name == c.COL_DATE:
            df = df[df.index <= mdate]

        dfs[name] = df

    return dfs
