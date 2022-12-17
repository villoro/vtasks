from prefect import task
from vpalette import get_colors

import pandas as pd

import utils as u

from . import constants as c


def read_flow_runs(vdp):
    df = vdp.read_parquet(c.PATH_FLOW_RUNS)
    df[c.COL_DAY] = df[c.COL_START].dt.date.apply(str)

    return df[df[c.COL_ENV] == "prod"]


def get_average_times(df_in):
    df = df_in.copy()
    mask = df[c.COL_STATE] == c.STATE_COMPLETED

    df = df[mask].pivot_table(
        index="day", columns="flow_name", values="total_run_time", aggfunc="mean"
    )

    out = {}
    out["times"] = {x: u.serie_to_dict(df[x].dropna()) for x in df.columns}

    df = df.apply(u.smooth_serie)
    out["times_trend"] = {x: u.serie_to_dict(df[x].dropna()) for x in df.columns}

    return out


def extract_colors():
    return {name: get_colors(data) for name, data in c.COLORS.items()}


def aggregate_results(df_in):
    df = df_in.copy()

    df[c.COL_DAY] = pd.to_datetime(df[c.COL_DAY])
    df["result"] = df[c.COL_STATE].map(c.STATES_MAP)

    return (
        df.groupby([c.COL_DAY, c.COL_FLOW_NAME])
        .agg(
            **{
                "result_min": pd.NamedAgg(column="result", aggfunc="min"),
                "result_max": pd.NamedAgg(column="result", aggfunc="max"),
            }
        )
        .reset_index()
        .set_index(c.COL_DAY)
    )


def get_results_by_flow_name(df_in, flow_name):
    df = df_in.copy()
    df = df.loc[df[c.COL_FLOW_NAME] == flow_name, ["result_min", "result_max"]]
    df = df.reindex(pd.date_range(start=df.index.min(), end=df.index.max()))

    df["result"] = None
    df.loc[(df["result_max"] == 1) & (df["result_min"] == 1), "result"] = "success"
    df.loc[(df["result_max"] == 1) & (df["result_min"] == 0), "result"] = "recovered"
    df.loc[(df["result_max"] == 0) & (df["result_min"] == 0), "result"] = "failed"
    df.loc[df["result_max"].isna() & df["result_min"].isna(), "result"] = "missing"

    df["count"] = 1

    return df[["result", "count"]]


def extract_results(df_in):
    data = {}

    df_agg = aggregate_results(df_in)
    df_res = get_results_by_flow_name(df_agg, "vtasks")
    data["dashboard"] = df_res["result"].value_counts().to_dict()
    data["vtasks_results"] = {
        x: u.serie_to_dict(df_res.loc[df_res["result"] == x, "count"])
        for x in df_res["result"].unique()
    }

    return data


@task(name="vtasks.vprefect.report")
def create_report():

    vdp = u.get_vdropbox()
    df = read_flow_runs(vdp)

    data = get_average_times(df)
    data.update(extract_results(df))

    data["colors"] = extract_colors()

    data["title"] = "Vtasks"
    data["sections"] = {}

    # Create report
    report = u.render_jinja_template("vtasks.html", data)
    vdp.write_file(report, c.PATH_REPORT)
