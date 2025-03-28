from datetime import date
from datetime import timedelta
from pathlib import Path

import pandas as pd
import yaml
from gcsa.google_calendar import GoogleCalendar
from gcsa.serializers.event_serializer import EventSerializer
from prefect import flow
from prefect import task

from vtasks.common.dropbox import get_vdropbox
from vtasks.common.duck import write_df
from vtasks.common.logs import get_logger
from vtasks.common.paths import get_path
from vtasks.common.secrets import export_secret


TOKEN_FILENAME = "token.pickle"

PATH_GCAL_JSON = get_path(".auth/gcal.json")
PATH_TOKEN_LOCAL = get_path(f".auth/{TOKEN_FILENAME}")

PATH_GCAL = "/Aplicaciones/gcalendar"
SCHEMA_OUT = "raw__gcal"
TABLE_OUT = "events"

BASE_PATH = str(Path(__file__).parent)
PATH_CALENDARS = f"{BASE_PATH}/calendars.yaml"

FLOW_NAME = "gcal"

MIN_DATE = date(2011, 11, 5)
MAX_DATE_DEFAULT = date.today() + timedelta(days=1)


@task(name=f"{FLOW_NAME}.download_token")
def download_token(vdp):
    """Download token from dropbox"""

    logger = get_logger()
    export_secret(PATH_GCAL_JSON, "GCAL_JSON")

    if TOKEN_FILENAME not in vdp.ls(PATH_GCAL):
        logger.warning("GCAL token not found in dropbox")
        return False

    logger.info("Downloading GCAL token from dropbox")
    token = vdp.read_file(f"{PATH_GCAL}/{TOKEN_FILENAME}", as_binary=True)

    with open(PATH_TOKEN_LOCAL, "wb") as stream:
        stream.write(token)

    return True


@task(name=f"{FLOW_NAME}.upload_token")
def upload_token(vdp):
    """Upload token to dropbox"""

    with open(PATH_TOKEN_LOCAL, "rb") as stream:
        data = stream.read()

    vdp.write_file(data, f"{PATH_GCAL}/{TOKEN_FILENAME}", as_binary=True)


@task(name=f"{FLOW_NAME}.read_calendars")
def read_calendars():
    """Read calendars info"""

    with open(PATH_CALENDARS, "r") as stream:
        return yaml.safe_load(stream)


def get_calendar(url):
    """Wrapper for GoogleCalendar"""

    return GoogleCalendar(
        url,
        credentials_path=PATH_GCAL_JSON,
        token_path=PATH_TOKEN_LOCAL,
        read_only=True,
    )


def serialize_event(name, event):
    return {
        # Serialize the event
        **EventSerializer.to_json(event),
        # And replace 'start' and 'end' with the easier to use values
        # Defaults from 'to_json' are dict that include timezone and we don't want that
        "start": event.start,
        "end": event.end,
        # Add fancy calendar name
        "calendar": name,
    }


def query_calendar(name, url, start=MIN_DATE, end=MAX_DATE_DEFAULT):
    """Get events from one calendar"""

    logger = get_logger()
    logger.info(f"Querying calender '{name}'")
    calendar = get_calendar(url)

    # Retrive all events between start and end
    data = []
    events = calendar.get_events(start, end, order_by="updated", single_events=True)
    for event in events:
        data.append(serialize_event(name, event))

    return data


def query_all_calendars(calendars):
    """Get all events from all calendars"""

    logger = get_logger()
    total = len(calendars)
    logger.info(f"Querying all {total} calendars")

    events = []

    for i, (name, url) in enumerate(calendars.items()):
        task_name = f"{FLOW_NAME}.query__{name}__{i + 1}/{total}".replace(" ", "_")
        events += task(name=task_name)(query_calendar)(name, url)

    logger.info("Tansforming to pandas")
    return pd.DataFrame(events)


@flow(name=FLOW_NAME)
def export_all_gcal():
    """Export all events as a parquet"""

    vdp = get_vdropbox()

    download_token(vdp)

    # Get events
    calendars = read_calendars()
    df = query_all_calendars(calendars)

    # Export events
    write_df(df, SCHEMA_OUT, TABLE_OUT, as_str=True)

    upload_token(vdp)


if __name__ == "__main__":
    export_all_gcal()
