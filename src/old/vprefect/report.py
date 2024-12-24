import pandas as pd
import utils as u
from adtk.detector import OutlierDetector
from prefect import task
from sklearn.neighbors import LocalOutlierFactor
from vpalette import get_colors

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
    out["times_trend"] = {
        x: u.serie_to_dict(u.smooth_serie(df[x].dropna())) for x in df.columns
    }

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

    return df[["result"]]


def get_results_percent(df_in):
    df = df_in.copy()
    results = df["result"].unique()

    total = df["result"].notna().rolling("30D").sum()

    for x in results:
        counts = (df["result"] == x).rolling("30D").sum()
        df[x] = (100 * counts / total).apply(lambda x: round(x, 2))

    return df[results]


def extract_results(df_in):
    data = {}

    df_agg = aggregate_results(df_in)
    df_res = get_results_by_flow_name(df_agg, "vtasks")

    # Extract cards
    data["dashboard"] = df_res["result"].value_counts().to_dict()
    data["dashboard"]["total_runs"] = len(df_res[df_res["result"] != "missing"])
    data["dashboard"]["last_run"] = df_in[c.COL_START].max().strftime("%Y-%m-%d %H:%M")

    # Extract vtasks results
    data["vtasks_results"] = {
        x: u.serie_to_dict((df_res["result"] == x).apply(int))
        for x in df_res["result"].unique()
    }

    # Results percent
    df_res_percent = get_results_percent(df_res)
    data["results_percent"] = {
        x: u.serie_to_dict(df_res_percent[x]) for x in df_res_percent.columns
    }

    return data


def get_vtasks_times(df_in):
    df = df_in.copy()

    mask = (
        (df[c.COL_ENV] == "prod")
        & (df[c.COL_STATE] == c.STATE_COMPLETED)
        & (df[c.COL_FLOW_NAME] == "vtasks")
    )
    df = df.loc[mask]

    times = df.set_index(c.COL_START)[c.COL_TIME].resample("D").mean()

    # Fill missing with 30d MA
    mask = times.isna()
    times.loc[mask] = times.rolling("30D").mean().loc[mask]

    return times


def datetime_index_to_date(serie_in):
    serie = serie_in.copy()
    ranges = pd.date_range(start=serie.index.min().date(), end=serie.index.max().date())
    serie.index = ranges
    return serie


def extract_anomalies(df_in):
    # Backfill to handle possible NaNs
    times = get_vtasks_times(df_in).backfill()

    outlier_detector = OutlierDetector(LocalOutlierFactor())
    anomalies = outlier_detector.fit_detect(times.to_frame())

    # For plotting, transform datetime index to date
    times = datetime_index_to_date(times)
    anomalies = datetime_index_to_date(anomalies)

    return {
        "times": u.serie_to_dict(times),
        "anomalies": u.serie_to_dict(times[anomalies]),
    }


@task(name="vtasks.vprefect.report")
def create_report():
    vdp = u.get_vdropbox()
    df = read_flow_runs(vdp)

    data = get_average_times(df)
    data.update(extract_results(df))
    data["anomalies"] = extract_anomalies(df)

    data["colors"] = extract_colors()

    data["title"] = "Vtasks"
    data["sections"] = {"evolution": "fa-chart-line"}

    # Create report
    report = u.render_jinja_template("vtasks.html", data)
    vdp.write_file(report, c.PATH_REPORT)
