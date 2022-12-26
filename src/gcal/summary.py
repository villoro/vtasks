import asyncio

from datetime import date

import plotly.graph_objects as go

from prefect import task, get_run_logger
from prefect.context import get_run_context

import utils as u

from mailjet import Attachment, InlineAttachment, Email
from .report import get_daily_data
from .export import read_calendars
from vprefect.query import query_task_runs

MAIN_CALS = [
    "05_Sport",
    "11_Paid work",
    "12_Work",
    "23_Study",
    "31_Leisure",
    "32_Videogames",
    "33_TV",
    "34_Books",
]


def get_n_week(dfi, n=1):
    """Get data for the week -N"""

    df = dfi.resample("W-MON", closed="left").sum().iloc[-n].T
    return df.sort_index()


def create_plot(df, calendars, mdate):
    data = []
    for name, value in df.items():
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
    return go.Figure(data=data)


def create_main_list(df, calendars):
    out = "<ul>"

    for name in MAIN_CALS:
        value = round(df[name], 2)
        color = calendars[name]["color"]

        out += f'<li style="color:{color}";><b>{name}</b>: {value} h</li>'

    out += "</ul>"

    return out


def prepare_email(mdate, fig, main_list):

    filename = f"plot_{mdate}.jpg"
    cid = "plot_gcal"

    fig_content = fig.to_image(format="jpg", width=1280, height=720, scale=2)

    html = f"""
    <h3>Report {mdate}</h3>
    {main_list}
    <img src=\"cid:{cid}\" max-width="1200" width="100%">
    """

    inline_attachments = [
        InlineAttachment(
            cid=cid,
            filename=filename,
            content=Attachment.to_b64(fig_content),
        )
    ]
    return Email(
        subject=f"Gcal summary [{mdate}]", html=html, inline_attachments=inline_attachments
    )


def process_summary(mdate):
    """Send gcalendar report"""

    log = get_run_logger()

    vdp = u.get_vdropbox()
    df = get_daily_data(vdp, mdate)
    calendars = read_calendars()

    # Prepare email
    df = get_n_week(df)
    fig = create_plot(df, calendars, mdate)
    main_list = create_main_list(df, calendars)

    # Send email
    email = prepare_email(mdate, fig, main_list)
    email.send()


@task(name="vtasks.gcal.summary")
def summary(mdate: date):
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
    process_summary(mdate)
