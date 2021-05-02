from utils import get_vdropbox
from utils import log
from utils import render_jinja_template

from expensor.functions import serie_to_dict
from expensor.functions import smooth_serie

from .gcal import PATH_GCAL
from .gcal import PATH_GCAL_DATA
from .gcal import read_calendars


def get_daily_data(vdp):
    """ Gets duration by calendar and day """

    df = vdp.read_parquet(PATH_GCAL_DATA)

    # Filter out daily events
    df = df[df["duration"] < 24]

    df = df.pivot_table(index="start_day", columns="calendar", values="duration", aggfunc="sum")

    # Make sure all days are present
    return df.fillna(0).resample("D").sum()


def to_percentages(df):
    """ Divide times by totals """

    return 100 * df.apply(lambda x: x / df.sum(axis=1))


def get_pies(df_m, df_m_trend):
    """ Get pies info """

    return {
        "all": serie_to_dict(df_m.mean()),
        "1m": serie_to_dict(df_m.tail(1).T.iloc[:, 0]),
        "12m": serie_to_dict(df_m.iloc[-12:].mean()),
    }


def extract_data(vdp, export=False):
    """ Extract data from the dataframe """

    df = get_daily_data(vdp)

    df_w_trend = df.resample("W").sum().apply(smooth_serie)
    df_m = df.resample("MS").sum()
    df_m_trend = df_m.apply(smooth_serie)

    to_dict_reversed = lambda df: serie_to_dict(df[reversed(df.columns)])

    out = {
        "week_trend": to_dict_reversed(df_w_trend),
        "month": to_dict_reversed(df_m),
        "month_percent": to_dict_reversed(to_percentages(df_m)),
        "month_trend": to_dict_reversed(df_m_trend),
        "month_trend_percent": to_dict_reversed(to_percentages(df_m_trend)),
        "pies": get_pies(df_m, df_m_trend),
        "colors": {name: data["color"] for name, data in read_calendars().items()},
    }

    if export:
        vdp.write_yaml(out, f"I will fail")

    return out


# @task
# @timeit
def gcal_report():
    """ Creates the report """
    vdp = get_vdropbox()

    data = extract_data(vdp)

    # Add title
    data["title"] = "Calendar"
    data["sections"] = {
        "evolution": "fa-chart-line",
        "pies": "fa-chart-pie",
    }

    # Create report
    report = render_jinja_template("gcalendar.html", data)
    vdp.write_file(report, f"{PATH_GCAL}/gcalendar.html")
