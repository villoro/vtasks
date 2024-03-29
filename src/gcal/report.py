import pandas as pd

from prefect import task

import utils as u

from .export import PATH_GCAL
from .export import PATH_GCAL_DATA
from .export import read_calendars


def get_daily_data(vdp, mdate):
    """Gets duration by calendar and day"""

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
    """Divide times by totals"""

    # If it's a dataframe get total by row
    if isinstance(df, pd.DataFrame):
        total = df.sum(axis=1)
    else:
        total = df.sum()

    return 100 * df.apply(lambda x: x / total)


def get_pies(df_m, df_m_trend):
    """Get pies info"""

    return {
        "all": u.serie_to_dict(to_percentages(df_m.mean())),
        "month": u.serie_to_dict(to_percentages(df_m.tail(1).T.iloc[:, 0])),
        "year": u.serie_to_dict(to_percentages(df_m.iloc[-12:].mean())),
    }


def get_cards(data, calendars):
    """Get last month for main calendars"""

    out = {}

    # Cast to list to so that keys can be reversed
    for x in reversed([*data["month"]]):

        # If it's one of the main calendars
        if calendars[x].get("main", False):

            mdict = data["month"][x]

            # Add value for last month
            out[x] = mdict[[*mdict][-1]]

    return out


def extract_data(vdp, df, export=False):
    """Extract data from the dataframe"""

    calendars = read_calendars()

    df_w_trend = df.resample("W-MON").sum().apply(u.smooth_serie)
    df_m = df.resample("MS").sum()
    df_m_trend = df_m.apply(u.smooth_serie)
    df_y = df.resample("YS").sum()

    # Filter out incomplete year
    df_y = df_y[df_y.index.year > 2011]

    to_dict_reversed = lambda df: u.serie_to_dict()

    out = {
        "week_trend": u.serie_to_dict(df_w_trend),
        "month": u.serie_to_dict(df_m),
        "month_percent": u.serie_to_dict(to_percentages(df_m)),
        "month_trend": u.serie_to_dict(df_m_trend),
        "month_trend_percent": u.serie_to_dict(to_percentages(df_m_trend)),
        "pies": get_pies(df_m, df_m_trend),
        "colors": {name: data["color"] for name, data in calendars.items()},
        "year": u.serie_to_dict(df_y),
        "year_percent": u.serie_to_dict(to_percentages(df_y)),
    }

    out["cards"] = get_cards(out, calendars)

    if export:
        vdp.write_yaml(out, f"I will fail")

    return out


@task(name="vtasks.gcal.report")
def gcal_report(mdate):
    """Creates the report"""

    # Start of last month
    mdate = mdate.replace(day=1)

    vdp = u.get_vdropbox()

    df = get_daily_data(vdp, mdate)
    data = extract_data(vdp, df)

    # Add title
    data["title"] = "Calendar"
    data["sections"] = {
        "evolution": "fa-chart-line",
        "pies": "fa-chart-pie",
    }

    # Create report
    report = u.render_jinja_template("gcalendar.html", data)
    vdp.write_file(report, f"{PATH_GCAL}/gcalendar.html")
