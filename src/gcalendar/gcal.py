from datetime import date
from datetime import timedelta
from pathlib import Path

import oyaml as yaml
import pandas as pd

from gcsa.google_calendar import GoogleCalendar

from utils import export_secret
from utils import get_path

PATH_GCAL_JSON = get_path("auth/gcal.json")
PATH_TOKEN = get_path("auth/token.pickle")

PATH_CALENDARS = str(Path(__file__).parent / "calendars.yaml")

MIN_DATE = date(2011, 11, 5)

export_secret(PATH_GCAL_JSON, "GCAL_JSON")


def read_calendars():
    """ Read calendars info """

    with open(PATH_CALENDARS, "r") as stream:
        return yaml.safe_load(stream)


def get_calendar(name):
    """ Wrapper for GoogleCalendar """

    return GoogleCalendar(
        name, credentials_path=PATH_GCAL_JSON, token_path=PATH_TOKEN, read_only=True,
    )


def query_events(calendar, start=MIN_DATE, end=date.today()):
    """ Get events from one calendar """

    events = []

    # Retrive all events between start and end
    for event in calendar.get_events(start, end, order_by="updated", single_events=True):
        events.append(
            {"id": event.id, "summary": event.summary, "start": event.start, "end": event.end,}
        )

    # Return them as a nice pandas dataframe
    return pd.DataFrame(events).dropna(subset=["start"])
