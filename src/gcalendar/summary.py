from datetime import date

from slack import send_slack
from utils import get_vdropbox
from utils import is_pro
from utils import log

from .process import get_daily_data


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


@vtask
def do_summary(mdate):
    """Creates the report"""

    if mdate.isoweekday() == 1:

        log.info("Send gcalendar weekly report")
        send_summary(mdate, "events")

    elif not is_pro():
        log.info("Send gcalendar weekly report for testing")
        send_summary(mdate, "test")

    else:
        log.info("Gcalendar weekly report skipped")
