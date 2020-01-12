"""
    Extract transactions from money lover file
"""

import os
import io

import dropbox
import pandas as pd

from global_utilities.gu_dropbox import get_dbx_connector, read_excel, write_excel
from . import constants as c
from . import utilities as u


def get_money_lover_filename(dbx):
    """ gets the name of the money lover excel file """

    names = []

    # Explore all files and save all that are valid
    for x in dbx.files_list_folder(c.PATH_MONEY_LOVER).entries:
        try:
            # Try to parse date, if possible if a money lover file
            pd.to_datetime(x.name.split(".")[0])
            names.append(x.name)

        except (TypeError, ValueError):
            pass

    return max(names)


def get_df_transactions(dbx):
    """
        Retrives the df with transactions. It will read the newest money lover excel file

        Args:
            dbx:    dropbox connector needed to call the dropbox api

        Returns:
            raw dataframe with transactions
    """
    filename = get_money_lover_filename(dbx)

    df = read_excel(dbx, c.PATH_MONEY_LOVER + filename, index_col=0)
    return u.fix_df_trans(df)


def main(*args, **kwa):
    """ Retrives all dataframes and update DFS global var """

    dbx = get_dbx_connector(c.VAR_DROPBOX_TOKEN)

    df = get_df_transactions(dbx)
    write_excel(dbx, df, c.FILE_TRANSACTIONS)

    return "Transactions processed"


if __name__ == "__main__":
    main()
