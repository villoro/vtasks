import regex as re

import pandas as pd

from . import constants as c
from .rapidapi import query_pair
from utils import get_vdropbox
from utils import log
from utils import vtask


def get_airports_pairs():
    """ Get a set of all airports combinations """

    vdp = get_vdropbox()
    df_airports = vdp.read_excel(c.FILE_AIRPORTS)

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


@vtask
def flights(mdate):

    filename = c.FILE_FLIGHTS_DAY.format(date=mdate)

    vdp = get_vdropbox()

    if vdp.file_exists(filename):
        log.warning(f"File '{filename}' already exists, skipping flights task")

    # Only query if the file does not exist
    else:
        df = retrive_all_flights()
        vdp.write_parquet(df, filename)


@vtask
def merge_flights_history(mdate):

    vdp = get_vdropbox()

    # Check for monthly folders and get all parquets inside
    for folder in vdp.ls(c.PATH_HISTORY):

        is_date_folder = re.search(r"\d{4}_\d{2}", folder)
        if is_date_folder and ("." not in folder) and (folder < f"{mdate:%Y_%m}"):

            log.info(f"Merging '{folder}' vflights history")

            sub_folder = f"{c.PATH_HISTORY}/{folder}"

            # Read all daily parquets
            dfs = []
            for file in vdp.ls(sub_folder):
                if file.endswith(".parquet"):
                    dfs.append(vdp.read_parquet(f"{sub_folder}/{file}"))

            # Export it as only one parquet file
            df = pd.concat(dfs)
            vdp.write_parquet(df, f"{sub_folder}.parquet")
            log.success(f"Successfuly merged '{folder}' vflights history")

            # Delete original folder
            vdp.delete(sub_folder)
