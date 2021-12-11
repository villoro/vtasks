import jinja2
import pandas as pd

from vpalette import get_colors

import utils as u

from . import constants as c
from expensor.functions import serie_to_dict
from expensor.functions import smooth_serie
from gspreadsheets import read_df_gdrive
from prefect_task import vtask


def get_books():
    df = read_df_gdrive(c.SPREADSHEET, c.SHEET_BOOKS).reset_index()
    df[c.COL_DATE] = pd.to_datetime(df[c.COL_DATE])

    return df


def get_dashboard(dfi):

    out = serie_to_dict(dfi.groupby("Language")["Pages"].sum())
    out["Total"] = int(dfi["Pages"].sum())
    out["Years"] = int(dfi[c.COL_DATE].dt.year.nunique())

    return out


def to_year_start(dfi):
    """Get yearly data so that there are no missing years"""

    df = dfi.copy()

    if c.COL_DATE in df.columns:
        df = df.set_index(c.COL_DATE)

    # Transform to year start
    df = df.resample("YS").sum().reset_index()

    # Cast date to year
    df[c.COL_DATE] = df[c.COL_DATE].dt.year

    return df.set_index(c.COL_DATE)


def get_year_data(dfi):

    df = dfi.pivot_table(values="Pages", index=c.COL_DATE, columns="Language", aggfunc="sum")
    df = to_year_start(df)

    out = {x: serie_to_dict(df[x]) for x in df.columns}

    out["Total"] = serie_to_dict(to_year_start(dfi)["Pages"])

    return out


def get_month_data(dfi):

    out = {
        i: serie_to_dict(dfa.resample("MS")["Pages"].sum())
        for i, dfa in dfi.set_index(c.COL_DATE).groupby("Language")
    }
    out["Total"] = serie_to_dict(dfi.set_index(c.COL_DATE).resample("MS")["Pages"].sum())

    for name, data in {**out}.items():  # The ** is to avoid problems while mutating the dict
        serie = smooth_serie(pd.Series(data))
        out[f"{name}_trend"] = serie_to_dict(serie[6:])

    return out


def get_year_percent(data, cumsum=True):

    df = pd.DataFrame(data)

    if cumsum:
        df = df.cumsum()

    # Get percentatges
    df = 100 * df.div(df["Total"], axis=0).fillna(0)

    return {x: serie_to_dict(df[x]) for x in df.columns if x != "Total"}


def get_top_authors(dfi, top_n=20):

    df = dfi.groupby("Author").agg({"Pages": "sum"}).sort_values("Pages", ascending=False)[:top_n]

    return serie_to_dict(df["Pages"])


def extract_data(export=False):

    df = get_books()

    out = {
        "dashboard": get_dashboard(df),
        "year_by_category": get_year_data(df),
        "month_by_category": get_month_data(df),
        "colors": {name: get_colors(data) for name, data in c.COLORS.items()},
    }

    # Add percents
    data = out["year_by_category"]
    out["year_percent"] = get_year_percent(data, cumsum=False)
    out["year_percent_cumsum"] = get_year_percent(data, cumsum=True)

    # Extract totals
    out["year"] = out["year_by_category"].pop("Total")
    out["month"] = out["month_by_category"].pop("Total")

    # Top Authors
    out["top_authors"] = get_top_authors(df)

    if export:
        u.get_vdropbox().write_yaml(out, f"{c.PATH_VBOOKS}/report_data.yaml")

    return out


@vtask
def vbooks():
    """Creates the report"""

    data = extract_data()

    # Add title
    data["title"] = "VBooks"
    data["sections"] = {
        "evolution": "fa-chart-line",
        "percent": "fa-percent",
        "authors": "fa-user",
    }

    # Create report
    report = u.render_jinja_template("vbooks.html", data)
    u.get_vdropbox().write_file(report, f"{c.PATH_VBOOKS}/vbooks.html")
