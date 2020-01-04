import slack

from config import PRO
from global_utilities import get_secret

CLIENT = slack.WebClient(token=get_secret("SLACK_LUIGI_TOKEN"))


def send_message(name, success, duration_human=None, exception=None, **kwa):
    """
        Send notification to slack.

        Args:
            name:           name of the tast
            success:        whether it ended with error or not [bool]
            duration_human: time as human readable
            exception:      details about the error
            kwa:            extra keyworded arguments
    """

    # Drop the 'Task' part of the task name
    name = name.replace("Task", "")

    if success:
        msg = f"*{name}*: {duration_human} :heavy_check_mark:"
    else:
        msg = f"*{name}*: {exception} :x:"

    response = CLIENT.chat_postMessage(channel="#events" if PRO else "#test", text=msg)

    # Check result
    assert response["ok"]
