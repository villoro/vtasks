import json
import requests

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
