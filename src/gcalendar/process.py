import pandas as pd

from utils import get_vdropbox
from utils import log
from utils import render_jinja_template

from expensor.functions import serie_to_dict
from expensor.functions import smooth_serie

from .gcal import PATH_GCAL
from .gcal import PATH_GCAL_DATA
from .gcal import read_calendars


def get_daily_data(vdp, mdate):
    """ Gets duration by calendar and day """

    df = vdp.read_parquet(PATH_GCAL_DATA)

    # Filter out daily events
    df = df[df["duration"] < 24]

    df = df.pivot_table(index="start_day", columns="calendar", values="duration", aggfunc="sum")

    # Make sure all days are present
    df = df.fillna(0).resample("D").sum()
    # First filter to start of the month and then one day less to avoid incomplete months
    # Also change the columns order
    return df.loc[:mdate, reversed(df.columns)].iloc[:-1]


def to_percentages(df):
    """ Divide times by totals """

    # If it's a dataframe get total by row
    if isinstance(df, pd.DataFrame):
        total = df.sum(axis=1)
    else:
        total = df.sum()

    return 100 * df.apply(lambda x: x / total)


def get_pies(df_m, df_m_trend):
    """ Get pies info """

    return {
        "all": serie_to_dict(to_percentages(df_m.mean())),
        "month": serie_to_dict(to_percentages(df_m.tail(1).T.iloc[:, 0])),
        "year": serie_to_dict(to_percentages(df_m.iloc[-12:].mean())),
    }


def get_cards(data, calendars):
    """ Get last month for main calendars """

    out = {}

    for i, mdict in data["month"].items():

        # If it's one of the main calendars
        if calendars[i].get("main", False):
            # Add value for last month
            out[i] = mdict[[*mdict][-1]]

    return out


def extract_data(vdp, df, export=False):
    """ Extract data from the dataframe """

    calendars = read_calendars()

    df_w_trend = df.resample("W").sum().apply(smooth_serie)
    df_m = df.resample("MS").sum()
    df_m_trend = df_m.apply(smooth_serie)

    to_dict_reversed = lambda df: serie_to_dict()

    out = {
        "week_trend": serie_to_dict(df_w_trend),
        "month": serie_to_dict(df_m),
        "month_percent": serie_to_dict(to_percentages(df_m)),
        "month_trend": serie_to_dict(df_m_trend),
        "month_trend_percent": serie_to_dict(to_percentages(df_m_trend)),
        "pies": get_pies(df_m, df_m_trend),
        "colors": {name: data["color"] for name, data in calendars.items()},
    }

    out["cards"] = get_cards(out, calendars)

    if export:
        vdp.write_yaml(out, f"I will fail")

    return out


@task
@timeit
def gcal_report(mdate):
    """ Creates the report """

    # Start of last month
    mdate = mdate.replace(day=1)

    vdp = get_vdropbox()

    df = get_daily_data(vdp, mdate)
    data = extract_data(vdp, df)

    # Add title
    data["title"] = "Calendar"
    data["sections"] = {
        "evolution": "fa-chart-line",
        "pies": "fa-chart-pie",
    }

    # Create report
    report = render_jinja_template("gcalendar.html", data)
    vdp.write_file(report, f"{PATH_GCAL}/gcalendar.html")
