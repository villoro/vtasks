import jinja2
import oyaml as yaml
import pandas as pd

from vpalette import get_colors

import utils as u

from . import constants as c
from expensor.functions import serie_to_dict


def get_books():
    df = u.read_df_gdrive(c.SPREADSHEET, c.SHEET_BOOKS).reset_index()
    df[c.COL_DATE] = pd.to_datetime(df[c.COL_DATE])
    df["Year"] = df[c.COL_DATE].dt.year

    return df


def get_dashboard(dfi):

    out = serie_to_dict(dfi.groupby("Language")["Pages"].sum())
    out["Total"] = int(dfi["Pages"].sum())
    out["Years"] = int(dfi["Year"].nunique())

    return out


def get_year_data(dfi):

    df = dfi.pivot_table(values="Pages", index="Year", columns="Language", aggfunc="sum").fillna(0)

    out = {x: serie_to_dict(df[x]) for x in df.columns}
    out["Total"] = serie_to_dict(df.sum(axis=1))

    return out


def get_month_data(dfi):

    out = {
        i: serie_to_dict(dfa.resample("MS")["Pages"].sum())
        for i, dfa in dfi.set_index(c.COL_DATE).groupby("Language")
    }
    out["Total"] = serie_to_dict(dfi.set_index(c.COL_DATE).resample("MS")["Pages"].sum())

    return out


def extract_data(export=False):

    df = get_books()

    out = {
        "dashboard": get_dashboard(df),
        "year_by_category": get_year_data(df),
        "month_by_category": get_month_data(df),
        "colors": {name: get_colors(data) for name, data in c.COLORS.items()},
    }

    out["year"] = out["year_by_category"].pop("Total")
    out["month"] = out["month_by_category"].pop("Total")

    if export:
        u.get_vdropbox().write_yaml(out, f"{c.PATH_VBOOKS}/report_data.yaml")

    return out


def vbooks():
    """ Creates the report """

    data = extract_data()

    # Add title
    data["title"] = "VBooks"
    data["sections"] = {
        "evolution": "fa-chart-line",
        "comparison": "fa-poll",
        "pies": "fa-chart-pie",
    }

    # Create report
    report = u.render_jinja_template("vbooks.html", data)
    u.get_vdropbox().write_file(report, f"{c.PATH_VBOOKS}/vbooks.html")
