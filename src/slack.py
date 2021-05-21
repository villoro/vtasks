import json
import requests

from utils import get_secret
from utils import is_pro


def send_slack(text="", channel="test", blocks=None):

    assert channel in ["test", "events", "general"]

    webhook = get_secret(f"SLACK_WEBHOOK_{channel.upper()}")

    data = {"text": text}

    if blocks:
        data["blocks"] = blocks

    res = requests.post(
        webhook, data=json.dumps(data), headers={"Content-Type": "application/json"}
    )

    res.raise_for_status()


def slack_state_handler(task, old_state, new_state):

    if not new_state.is_finished():
        return new_state

    failure = new_state.is_failed()

    # Prepare message
    if failure:
        msg = f"*{task.name}:* :x:"
    else:
        msg = f"*{task.name}:* {task.duration} :heavy_check_mark:"

    # Notify result
    send_slack(msg, channel="events" if is_pro() else "test")

    # In pro notify about failures in general
    if failure and is_pro():
        send_slack(msg, channel="general")

    return new_state
