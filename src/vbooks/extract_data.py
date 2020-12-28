import pandas as pd

import utils as u

from . import constants as c
from expensor.functions import serie_to_dict
from expensor.functions import series_to_dicts


def get_books():
    df = u.read_df_gdrive(c.SPREADSHEET, c.SHEET_BOOKS).reset_index()
    df[c.COL_DATE] = pd.to_datetime(df[c.COL_DATE])
    df["Year"] = df[c.COL_DATE].dt.year

    return df


def get_dashboard(dfi):

    return {
        "Total": dfi["Pages"].sum(),
        "Languages": serie_to_dict(dfi.groupby("Language")["Pages"].sum()),
    }


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


def main(export=False):

    df = get_books()

    out = {
        "dashboard": get_dashboard(df),
        "year_by_category": get_year_data(df),
        "month_by_category": get_month_data(df),
    }

    out["year"] = out["year_by_category"].pop("Total")
    out["month"] = out["month_by_category"].pop("Total")

    if export:
        u.get_vdropbox().write_yaml(out, f"{c.PATH_VBOOKS}/report_data.yaml")
