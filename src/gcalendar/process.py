from utils import get_vdropbox
from utils import log

from expensor.functions import serie_to_dict
from expensor.functions import smooth_serie

from .gcal import PATH_GCAL_DROPBOX


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


def extract_data(export=False):

    vdp = get_vdropbox()

    df = get_daily_data(vdp)

    # out = {
    #     "dashboard": get_dashboard(df),
    #     "year_by_category": get_year_data(df),
    #     "month_by_category": get_month_data(df),
    #     "colors": {name: get_colors(data) for name, data in c.COLORS.items()},
    # }

    if export:
        vdp.write_yaml(out, f"I will fail")

    return out
