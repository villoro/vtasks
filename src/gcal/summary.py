from datetime import date

from prefect import task, get_run_logger

from slack import send_slack
from utils import get_vdropbox, is_pro

from .report import get_daily_data


def get_n_week(dfi, n=1):
    """Get data for the week -N"""

    df = dfi.resample("W-MON", closed="left").sum().iloc[-n].T
    return df[df > 0].sort_values(ascending=False).to_dict()


def create_slack_block(data):

    options = []
    for i, (name, value) in enumerate(data.items()):
        options.append(
            {
                "text": {"type": "mrkdwn", "text": f"*{name}:* {round(value, 2)} h"},
                "value": f"value-{i}",
            }
        )

    return {
        "type": "section",
        "text": {"type": "mrkdwn", "text": "Summary of the week"},
        "accessory": {"type": "checkboxes", "options": options, "action_id": "checkboxes-action"},
    }


def send_summary(mdate, channel):
    """Send gcalendar report"""

    vdp = get_vdropbox()
    df = get_daily_data(vdp, mdate)

    # Prepare slack message
    data = get_n_week(df)
    block = create_slack_block(data)

    # Send slack
    send_slack(channel=channel, blocks=[block])


@task(name="vtasks.gcal.summary")
def do_summary(mdate: date):
    """Creates the report"""

    log = get_run_logger()

    if mdate.isoweekday() == 1:

        log.info("Sending gcalendar weekly report")
        send_summary(mdate, "general")

    elif not is_pro():
        log.info("Sending gcalendar weekly report for testing")
        send_summary(mdate, "test")

    else:
        log.info("Gcalendar weekly report skipped")
