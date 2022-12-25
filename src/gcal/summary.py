import asyncio

from datetime import date

import plotly.graph_objects as go

from prefect import task, get_run_logger
from prefect.context import get_run_context

import mailjet as mj
import utils as u

from .report import get_daily_data
from .export import read_calendars
from vprefect.query import query_task_runs


def get_n_week(dfi, n=1):
    """Get data for the week -N"""

    df = dfi.resample("W-MON", closed="left").sum().iloc[-n].T
    return df.sort_index()


def create_plot(dfi, calendars, mdate):
    data = []
    for name, value in dfi.items():
        data.append(
            go.Bar(
                x=[name],
                y=[value],
                text=f"{value:.2f}",
                textposition="outside",
                name=name,
                marker_color=calendars[name]["color"],
            )
        )
    return go.Figure(data=data, layout=go.Layout(title=f"Week {mdate}"))


def send_summary(mdate):
    """Send gcalendar report"""

    log = get_run_logger()

    vdp = u.get_vdropbox()
    df = get_daily_data(vdp, mdate)
    calendars = read_calendars()

    # Prepare slack message
    log.info("Preparing summary")
    df = get_n_week(df)
    fig = create_plot(df, calendars, mdate)

    # Send email
    log.info("Sending summary")
    mailjet = mj.get_mailjet_client()
    attachments = [mj.plotly_to_attachment(fig, f"plot_{mdate}.jpg")]
    data = mj.create_email(f"Gcal summary [{mdate}]", f"<h1>Report {mdate}</h1>", attachments)
    mailjet.send.create(data=data)


@task(name="vtasks.gcal.summary")
def do_summary(mdate: date):
    """Creates the report"""

    log = get_run_logger()

    # This extract the name of this same task (no hardcoding)
    task_name = get_run_context().task_run.name.split("-")[0]

    if mdate.isoweekday() != 1:
        log.info(f"Skipping '{task_name}' since it only runs on Mondays")
        return None

    env = u.detect_env()
    if env != "prod":
        log.info(f"Skipping summary since {env=}")
        return None

    log.info(f"Checking if '{task_name}' has already run today")
    task_runs = asyncio.run(query_task_runs(name_like=task_name, env=env))

    if task_runs:
        log.warning("Summary already send")
        return None

    log.info("Sending gcalendar weekly report since it's the first run of the week")
    send_summary(mdate)
