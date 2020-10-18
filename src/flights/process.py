import regex as re

import pandas as pd

from prefect import task

import global_utilities as gu

from . import constants as c
from .rapidapi import query_pair
from global_utilities import log


def get_airports_pairs():
    """ Get a set of all airports combinations """

    dbx = gu.dropbox.get_dbx_connector(c.VAR_DROPBOX_TOKEN)
    df_airports = gu.dropbox.read_excel(dbx, c.FILE_AIRPORTS)

    out = set()
    for _, row in df_airports.iterrows():
        out.add((row[c.COL_ORIGIN], row[c.COL_DESTINATION]))
        out.add((row[c.COL_DESTINATION], row[c.COL_ORIGIN]))

    log.info("Airports retrived from dropbox")

    return out


def retrive_all_flights():
    """ Get a dataframe with all flights """

    dfs = []
    airports_pairs = get_airports_pairs()
    total_pairs = len(airports_pairs)

    for i, (origin, dest) in enumerate(airports_pairs):

        log.info(f"Quering flights from '{origin}' to '{dest}' ({i + 1}/{total_pairs})")
        df = query_pair(origin, dest)

        if df is not None:
            dfs.append(df)

    if dfs:
        # drop_duplicates to make the concat easier
        return pd.concat(dfs).reset_index(drop=True).drop_duplicates(c.COLS_INDEX)
    else:
        log.error(f"There are no flights")


@task
def flights(mdate):

    filename = c.FILE_FLIGHTS_DAY.format(date=mdate)
    dbx = gu.dropbox.get_dbx_connector(c.VAR_DROPBOX_TOKEN)

    if gu.dropbox.file_exists(dbx, filename):
        log.warning(f"File '{filename}' already exists, skipping flights task")

    # Only query if the file does not exist
    else:
        df = retrive_all_flights()
        gu.dropbox.write_parquet(dbx, df, filename)


@task
def merge_flights_history(mdate):

    dbx = gu.dropbox.get_dbx_connector(c.VAR_DROPBOX_TOKEN)

    # Check for monthly folders and get all parquets inside
    for folder in sorted(gu.dropbox.ls(dbx, c.PATH_HISTORY)):

        is_date_folder = re.search(r"\d{4}_\d{2}", folder)
        if is_date_folder and ("." not in folder) and (folder < f"{mdate:%Y_%m}"):

            log.info(f"Merging '{folder}' vflights history")

            sub_folder = f"{c.PATH_HISTORY}/{folder}"

            # Read all daily parquets
            dfs = []
            for file in gu.dropbox.ls(dbx, sub_folder):
                if file.endswith(".parquet"):
                    dfs.append(gu.dropbox.read_parquet(dbx, f"{sub_folder}/{file}"))

            # Export it as only one parquet file
            df = pd.concat(dfs)
            gu.dropbox.write_parquet(dbx, df, f"{sub_folder}.parquet")
            log.success(f"Successfuly merged '{folder}' vflights history")

            # Delete original folder
            gu.dropbox.delete(dbx, sub_folder)
