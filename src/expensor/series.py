import pandas as pd

from scipy.signal import savgol_filter
from tsmoothie.smoother import ConvolutionSmoother


def resample(df, period, mdate):
    """Resample and fill missing periods"""

    index = pd.date_range(df.index.min(), mdate, freq=period)
    df = df.resample(period).sum().reindex(index).fillna(0)

    # If working with years, cast the index to integer
    if period == "YS":
        df.index = df.index.year

    return df


def tsmooth(serie, window):
    """Apply convolution smoother to a 1D serie"""
    smoother = ConvolutionSmoother(window_len=window, window_type="ones")
    smoother.smooth(serie)
    return smoother.smooth_data[0]


def smooth_serie(
    serie, savgol_window=35, savgol_polyorder=5, savgol_mode="nearest", tsmoothie_window=3
):
    """Smooth a serie by doing a savgol filter followed by a tsmooth"""

    savgol = savgol_filter(serie, savgol_window, savgol_polyorder, mode=savgol_mode)
    return tsmooth(savgol, tsmoothie_window)
