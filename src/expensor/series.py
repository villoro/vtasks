import pandas as pd

from scipy.signal import savgol_filter


def resample(df, period, mdate):
    """Resample and fill missing periods"""

    index = pd.date_range(df.index.min(), mdate, freq=period)
    df = df.resample(period).sum().reindex(index).fillna(0)

    # If working with years, cast the index to integer
    if period == "YS":
        df.index = df.index.year

    return df
