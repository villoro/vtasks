"""
    Extract transactions from money lover file
"""

import pandas as pd

from prefect import task

import global_utilities as gu

from . import constants as c
from . import utilities as u
from global_utilities import log


def get_money_lover_filename(dbx):
    """ gets the name of the money lover excel file """

    names = []

    # Explore all files and save all that are valid
    for name in gu.dropbox.ls(dbx, c.PATH_MONEY_LOVER):
        try:
            # Try to parse date, if possible is a money lover file
            pd.to_datetime(name.split(".")[0])
            names.append(name)

        except (TypeError, ValueError):
            pass

    return c.PATH_MONEY_LOVER + max(names)


@task
def money_lover(mdate):
    """ Retrives all dataframes and update DFS global var """

    # Get data
    dbx = gu.dropbox.get_dbx_connector(c.VAR_DROPBOX_TOKEN)
    filename = get_money_lover_filename(dbx)
    df = gu.dropbox.read_excel(dbx, filename, index_col=0)
    log.info(f"File '{filename}' read from dropbox")

    # Process dataframe
    df = u.fix_df_trans(df)

    # Store data
    gu.dropbox.write_excel(dbx, df, c.FILE_TRANSACTIONS)
