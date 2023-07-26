import jinja2
import pandas as pd

from prefect import flow
from prefect import task
from vpalette import get_colors

import utils as u

from . import constants as c
from gspreadsheets import read_df_gdrive


def get_books():
    df = read_df_gdrive(c.SPREADSHEET, c.SHEET_BOOKS).reset_index()
    df[c.COL_DATE] = pd.to_datetime(df[c.COL_DATE])

    return df


def get_todo():
    df = read_df_gdrive(c.SPREADSHEET, c.SHEET_TODO).reset_index()
    df[c.COL_PAGES] = df[c.COL_PAGES].replace("", 0)
    # Replace owning status for nice names
    df[c.COL_OWNED] = (
        df[c.COL_OWNED]
        .map({1: c.STATUS_OWNED, "b": c.STATUS_IN_LIBRARY})
        .fillna(c.STATUS_NOT_OWNED)
    )

    return df


def get_dashboard(dfi):

    out = u.serie_to_dict(dfi.groupby(c.COL_LANGUAGE)[c.COL_PAGES].sum())
    out["Total"] = int(dfi[c.COL_PAGES].sum())
    out["Years"] = int(dfi[c.COL_DATE].dt.year.nunique())

    return out


def to_year_start(dfi):
    """Get yearly data so that there are no missing years"""

    df = dfi.copy()

    if c.COL_DATE in df.columns:
        df = df.set_index(c.COL_DATE)

    # Transform to year start
    df = df.resample("YS").sum(numeric_only=True).reset_index()

    # Cast date to year
    df[c.COL_DATE] = df[c.COL_DATE].dt.year

    return df.set_index(c.COL_DATE)


def get_year_data(dfi):

    df = dfi.pivot_table(
        values=c.COL_PAGES, index=c.COL_DATE, columns=c.COL_LANGUAGE, aggfunc="sum"
    )
    df = to_year_start(df)

    out = {x: u.serie_to_dict(df[x]) for x in df.columns}

    out["Total"] = u.serie_to_dict(to_year_start(dfi)[c.COL_PAGES])

    return out


def get_month_data(dfi):

    out = {
        i: u.serie_to_dict(dfa.resample("MS")[c.COL_PAGES].sum())
        for i, dfa in dfi.set_index(c.COL_DATE).groupby(c.COL_LANGUAGE)
    }
    out["Total"] = u.serie_to_dict(dfi.set_index(c.COL_DATE).resample("MS")[c.COL_PAGES].sum())

    for name, data in {**out}.items():  # The ** is to avoid problems while mutating the dict
        serie = u.smooth_serie(pd.Series(data))
        out[f"{name}_trend"] = u.serie_to_dict(serie[6:])

    return out


def get_year_percent(data, cumsum=True):

    df = pd.DataFrame(data)

    if cumsum:
        df = df.cumsum()

    # Get percentatges
    df = 100 * df.div(df["Total"], axis=0).fillna(0)

    return {x: u.serie_to_dict(df[x]) for x in df.columns if x != "Total"}


def get_year_by_type(dfi):
    df = dfi.pivot_table(values=c.COL_PAGES, index=c.COL_DATE, columns=c.COL_TYPE, aggfunc="sum")
    df = to_year_start(df)

    out = {x: u.serie_to_dict(df[x]) for x in df.columns}

    return out


def get_top(dfi, groupby, top_n=20):

    df = (
        dfi.groupby(groupby)
        .agg({c.COL_PAGES: "sum"})
        .sort_values(c.COL_PAGES, ascending=False)[:top_n]
    )

    return u.serie_to_dict(df[c.COL_PAGES])


@task(name="vtasks.vbooks.extract")
def extract_data(export=False):

    df = get_books()
    df_todo = get_todo()

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

    # Add type
    out["year_by_type"] = get_year_by_type(df)

    # Extract totals
    out["year"] = out["year_by_category"].pop("Total")
    out["month"] = out["month_by_category"].pop("Total")

    # Top Authors
    out["top_authors"] = get_top(df, groupby=c.COL_AUTHOR)

    # TO DO section
    out["todo_by_author"] = get_top(df_todo, c.COL_AUTHOR)
    out["todo_by_source"] = get_top(df_todo, c.COL_SOURCE)
    out["todo_by_ownership"] = get_top(df_todo, c.COL_OWNED)

    if export:
        u.get_vdropbox().write_yaml(out, f"{c.PATH_VBOOKS}/report_data.yaml")

    return out


@task(name="vtasks.vbooks.report")
def create_report(data):

    # Add title
    data["title"] = "VBooks"
    data["sections"] = {
        "evolution": "fa-chart-line",
        "percent": "fa-percent",
        "authors": "fa-user",
        "todo": "fa-list",
    }

    # Create report
    report = u.render_jinja_template("vbooks.html", data)
    u.get_vdropbox().write_file(report, f"{c.PATH_VBOOKS}/vbooks.html")


@flow(**u.get_prefect_args("vtasks.vbooks"))
def vbooks():
    data = extract_data()
    create_report(data)
