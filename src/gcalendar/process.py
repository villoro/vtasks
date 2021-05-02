from utils import get_vdropbox
from utils import log

from expensor.functions import serie_to_dict
from expensor.functions import smooth_serie

from .gcal import PATH_GCAL_DROPBOX
from .gcal import read_calendars


def get_daily_data(vdp):
    """ Gets duration by calendar and day """

    df = vdp.read_parquet(PATH_GCAL_DROPBOX)

    # Filter out daily events
    df = df[df["duration"] < 24]

    df = df.pivot_table(index="start_day", columns="calendar", values="duration", aggfunc="sum")

    # Make sure all days are present
    return df.fillna(0).resample("D").sum()


def to_percentages(df):
    """ Divide times by totals """

    return df.apply(lambda x: x / df.sum(axis=1))


def get_pies(df_m, df_m_trend):
    """ Get pies info """

    return {
        "all": serie_to_dict(df_m.mean()),
        "1m": serie_to_dict(df_m.tail(1).T.iloc[:, 0]),
        "12m": serie_to_dict(df_m.iloc[-12:].mean()),
    }


def extract_data(export=False):

    vdp = get_vdropbox()

    df = get_daily_data(vdp)

    df_w_trend = df.resample("W").sum().apply(smooth_serie)
    df_m = df.resample("MS").sum()
    df_m_trend = df_m.apply(smooth_serie)

    out = {
        "week_trend": serie_to_dict(df_w_trend),
        "month": serie_to_dict(df_m),
        "month_percent": serie_to_dict(to_percentages(df_m)),
        "month_trend": serie_to_dict(df_m_trend),
        "month_trend_percent": serie_to_dict(to_percentages(df_m_trend)),
        "pies": get_pies(df_m, df_m_trend),
        "colors": {name: data["color"] for name, data in read_calendars().items()},
    }

    if export:
        vdp.write_yaml(out, f"I will fail")

    return out
