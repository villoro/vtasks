import re

from datetime import date
from datetime import datetime
from datetime import timedelta

import pandas as pd

from utils import get_vdropbox
from utils import log

URIS = [
    "/Aplicaciones/KeePass/(.).kdbx",
    "/Aplicaciones/expensor/(.).(yaml|yml)",
    "/Aplicaciones/FreeFileSync/(.).ffs_gui",
]

YEAR = f"{date.today():%Y}"
DAY = f"{date.today():%Y_%m_%d}"


def get_update_at(vdp, filename):
    """ Get the date when a file was updated """

    metadata = vdp.dbx.files_get_metadata(filename)
    return metadata.client_modified.date()


def updated_yesterday(vdp, filename):
    """ True if the file has been updated after the start of yesterday """

    updated_at = get_update_at(vdp, filename)

    return updated_at > date.today() - timedelta(1)


def get_files_to_backup(vdp, uri):
    """ Get a path and a list of files form a regex """

    # Extract path and regex
    path = uri.split("/")
    regex = path.pop()
    path = "/".join(path)

    filenames = [x for x in vdp.ls(path) if re.search(regex, x)]

    return path, filenames


def one_backup(vdp, path, filenames):
    """ Back up a list of files from a folder """

    # Backup all files
    for filename in filenames:
        origin = f"{path}/{filename}"
        dest = f"{path}/Backups/{YEAR}/{DAY} {filename}"

        if updated_yesterday(vdp, origin):

            if not vdp.file_exists(dest):
                log.info(f"Copying '{origin}' to '{dest}'")

                vdp.dbx.files_copy(origin, dest)

            else:
                log.debug(f"File '{origin}' has already been backed up")

        else:
            log.debug(f"Skipping '{origin}' since has not been updated")


def get_backup_data(vdp, path):

    base_path = f"{path}/Backups"

    dfs = []

    for year in vdp.ls(base_path):

        uri = f"{base_path}/{year}"
        backups = vdp.ls(uri)

        # Create a dataframe with relevant data of each backup
        df = pd.DataFrame(backups, columns=["filename"])
        df["entity"] = df["filename"].str[11:]
        df["date"] = pd.to_datetime(df["filename"].str[:10], format="%Y_%m_%d")
        df["path"] = uri + df["filename"]

        dfs.append(df)

    return pd.concat(dfs)


def tag_duplicates(df_in):
    """
        Tag entries to delete. Old files (>30d) keep the latest for each month.
        New files (<30) keep them all.
    """

    df = df_in.copy()

    # Use the month as general filter
    df["date_filter"] = df["date"].dt.strftime("%Y-%m")

    # For files newer than 30d, keep them all (use the day for the filter)
    is_newer_30d = df["date"] > datetime.now() - timedelta(30)
    df.loc[is_newer_30d, "date_filter"] = df.loc[is_newer_30d, "date"].dt.strftime("%Y-%m-%d")

    # Mark the first duplicated for deletion
    df["delete"] = df.duplicated(["entity", "date_filter"], keep="last")

    return df


def backup_files():
    """ Back up all files from URIS """

    vdp = get_vdropbox()

    for uri in URIS:
        log.info(f"Backing up '{uri}'")

        path, filenames = get_files_to_backup(vdp, uri)

        one_backup(vdp, uri)
