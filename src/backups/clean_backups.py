import re

from datetime import datetime
from datetime import timedelta

import pandas as pd

from .backup_files import PATH_FILES
from prefect_task import vtask
from utils import get_vdropbox
from utils import log
from utils import read_yaml


def get_backup_data(vdp, path, regex):
    """ Get info about the backups """

    base_path = f"{path}/Backups"

    dfs = []

    # Check if there are backups
    if not vdp.file_exists(base_path):
        return None

    # Retrive all backups
    for year in vdp.ls(base_path):

        log.debug(f"Exploring '{path}/{year}'")

        # Skip iteration if it's not a year
        if not re.search(r"^\d{4}$", year):
            log.debug("Skipping")
            continue

        uri = f"{base_path}/{year}"
        backups = vdp.ls(uri)

        # Create a dataframe with relevant data of each backup
        df = pd.DataFrame(backups, columns=["filename"])
        df["entity"] = df["filename"].str[11:]
        df["date"] = pd.to_datetime(df["filename"].str[:10], format="%Y_%m_%d")
        df["uri"] = uri + "/" + df["filename"]

        dfs.append(df)

    # No data nothing to concatenate
    if not dfs:
        return None

    df = pd.concat(dfs)
    df["base_path"] = path

    return df.set_index("uri")


def get_all_backups(vdp):
    """ Get all backups """

    dfs = []

    for kwargs in read_yaml(PATH_FILES):

        log.info("Scanning '{path}'".format(**kwargs))

        df = get_backup_data(vdp, **kwargs)

        if df is not None:
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
    df["delete"] = df.duplicated(["entity", "base_path", "date_filter"], keep="last")

    return df


@vtask
def clean_backups():
    """ Delete backups so that only one per month remain (except if newer than 30d) """

    vdp = get_vdropbox()

    df = get_all_backups(vdp)
    df = tag_duplicates(df)

    # Delete files tagged as 'delete'
    for uri in df[df["delete"]].index:
        vdp.delete(uri)
