import pandas as pd

from prefect import task

import global_utilities as gu

from global_utilities import log

from . import constants as c
from .rapidapi import query_pair


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

    # Get history
    dbx = gu.dropbox.get_dbx_connector(c.VAR_DROPBOX_TOKEN)

    # Get new data
    df = retrive_all_flights()

    # Store data from today
    gu.dropbox.write_parquet(dbx, df, c.FILE_FLIGHTS_DAY.format(date=mdate))
