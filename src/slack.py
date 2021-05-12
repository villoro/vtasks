import json
import requests

from utils import ENV_PRO
from utils import get_secret


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

    if new_state.is_finished():

        emoji = ":x:" if new_state.is_failed() else ":heavy_check_mark:"
        msg = f"*{task.name.title()}:* {emoji}"
        send_slack(msg, channel="events" if ENV_PRO else "test")

    return new_state
