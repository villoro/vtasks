from prefect import task
from vpalette import get_colors

import utils as u

from . import constants as c


def read_flow_runs(vdp):
    df = vdp.read_parquet(c.PATH_FLOW_RUNS)
    df[c.COL_DAY] = df[c.COL_START].dt.date.apply(str)

    return df


def get_average_times(df_in):
    df = df_in[df_in[c.COL_STATE] == c.STATE_COMPLETED].copy()

    df = df.pivot_table(index="day", columns="flow_name", values="total_run_time", aggfunc="mean")

    out = {}
    out["times"] = {x: u.serie_to_dict(df[x].dropna()) for x in df.columns}

    df = df.apply(u.smooth_serie)
    out["times_trend"] = {x: u.serie_to_dict(df[x].dropna()) for x in df.columns}

    return out


def extract_colors():
    return {name: get_colors(data) for name, data in c.COLORS.items()}


@task(name="vtasks.vprefect.report")
def create_report():

    vdp = u.get_vdropbox()
    df = read_flow_runs(vdp)

    data = get_average_times(df)
    data["colors"] = extract_colors()

    data["title"] = "Vtasks"
    data["sections"] = {}

    # Create report
    report = u.render_jinja_template("vtasks.html", data)
    vdp.write_file(report, c.PATH_REPORT)
