from datetime import date
from pathlib import Path

import oyaml as yaml
import pandas as pd

from gcsa.google_calendar import GoogleCalendar
from gcsa.serializers.event_serializer import EventSerializer
from prefect import task

import utils as u

TOKEN_FILENAME = "token.pickle"

PATH_GCAL_JSON = u.get_path("auth/gcal.json")
PATH_TOKEN_LOCAL = u.get_path(f"auth/{TOKEN_FILENAME}")

PATH_GCAL = "/Aplicaciones/gcalendar"
PATH_GCAL_DATA = f"{PATH_GCAL}/calendar.parquet"

PATH_CALENDARS = str(Path(__file__).parent / "calendars.yaml")

MIN_DATE = date(2011, 11, 5)

u.export_secret(PATH_GCAL_JSON, "GCAL_JSON")


def download_token(vdp):
    """Download token from dropbox"""

    log = u.get_log()

    if not TOKEN_FILENAME in vdp.ls(PATH_GCAL):
        log.warning("GCAL token not found in dropbox")
        return False

    log.info("Downloading GCAL token from dropbox")

    token = vdp.read_file(f"{PATH_GCAL}/{TOKEN_FILENAME}", as_binary=True)

    with open(PATH_TOKEN_LOCAL, "wb") as stream:
        stream.write(token)

    return True


def upload_token(vdp):
    """Upload token to dropbox"""

    with open(PATH_TOKEN_LOCAL, "rb") as stream:
        data = stream.read()

    vdp.write_file(data, f"{PATH_GCAL}/{TOKEN_FILENAME}", as_binary=True)


def read_calendars():
    """Read calendars info"""

    with open(PATH_CALENDARS, "r") as stream:
        return yaml.safe_load(stream)


def get_calendar(name):
    """Wrapper for GoogleCalendar"""

    return GoogleCalendar(
        name,
        credentials_path=PATH_GCAL_JSON,
        token_path=PATH_TOKEN_LOCAL,
        read_only=True,
    )


def query_events(calendar, end, start=MIN_DATE, drop_invalid=True):
    """Get events from one calendar"""

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


def get_all_events(calendars, mdate):
    """Get all events from all calendars"""

    log = u.get_log()
    log.info("Querying all calendars")

    dfs = []

    for name, data in calendars.items():
        log.info(f"Querying calendar '{name}'")

        calendar = get_calendar(data["url"])

        df = query_events(calendar, end=mdate)
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


@task(name="vtasks.gcal.export")
def export_calendar_events(mdate):
    """Export all events as a parquet"""

    vdp = u.get_vdropbox()

    download_token(vdp)

    # Get events
    calendars = read_calendars()
    df = get_all_events(calendars, mdate)

    # Export events
    vdp.write_parquet(df, PATH_GCAL_DATA)

    upload_token(vdp)
