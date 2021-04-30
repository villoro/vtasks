from datetime import date
from pathlib import Path

import oyaml as yaml
import pandas as pd

from gcsa.google_calendar import GoogleCalendar
from gcsa.serializers.event_serializer import EventSerializer
from prefect import task

from utils import export_secret
from utils import get_path
from utils import get_vdropbox
from utils import log
from utils import save_secret
from utils import timeit

PATH_GCAL_JSON = get_path("auth/gcal.json")
PATH_TOKEN = get_path("auth/token.pickle")

PATH_GCAL_DROPBOX = "/Aplicaciones/gcalendar/calendar.parquet"

PATH_CALENDARS = str(Path(__file__).parent / "calendars.yaml")

MIN_DATE = date(2011, 11, 5)

export_secret(PATH_GCAL_JSON, "GCAL_JSON")
export_secret(PATH_TOKEN, "GCAL_TOKEN", binary=True)


def load_token():
    """ Load token as a secret """

    with open(PATH_TOKEN, "rb") as stream:
        data = stream.read()

    save_secret("GCAL_TOKEN", data)


def read_calendars():
    """ Read calendars info """

    with open(PATH_CALENDARS, "r") as stream:
        return yaml.safe_load(stream)


def get_calendar(name):
    """ Wrapper for GoogleCalendar """

    return GoogleCalendar(
        name, credentials_path=PATH_GCAL_JSON, token_path=PATH_TOKEN, read_only=True,
    )


def query_events(calendar, end, start=MIN_DATE, drop_invalid=True):
    """ Get events from one calendar """

    data = []

    # Retrive all events between start and end
    events = calendar.get_events(start, end, order_by="updated", single_events=True)

    for event in events:

        data.append(
            {
                # Serialize the event
                **EventSerializer.to_json(event),
                # And replace 'start' and 'end' with the easier to use values
                # Defaults from 'to_json' are dict that include timezone and we don't want that
                "start": event.start,
                "end": event.end,
            }
        )

    # Return them as a nice pandas dataframe
    df = pd.DataFrame(data)

    if drop_invalid:
        df = df.dropna(subset=["start"])

    return df


def get_all_events(calendars, exec_date):
    """ Get all events from all calendars """

    log.info("Querying all calendars")

    dfs = []

    for name, data in calendars.items():

        log.debug(f"Querying calendar '{name}'")

        calendar = get_calendar(data["url"])

        df = query_events(calendar, end=exec_date)
        df["calendar"] = name

        dfs.append(df)

    log.info("Concatenating events from all calendars")
    df = pd.concat(dfs).reset_index(drop=True)

    # Cast datetime columns
    df["start"] = pd.to_datetime(df["start"], utc=True)
    df["end"] = pd.to_datetime(df["end"], utc=True)

    # Add duration and start_day
    df["duration"] = (df["end"] - df["start"]) / pd.Timedelta("1h")
    df["start_day"] = pd.to_datetime(df["start"].dt.date)

    return df


@task
@timeit
def export_calendar_events(exec_date):
    """ Export all events as a parquet """

    vdp = get_vdropbox()

    # Get events
    calendars = read_calendars()
    df = get_all_events(calendars, exec_date)

    # Export events
    vdp.write_parquet(df, PATH_GCAL_DROPBOX)
