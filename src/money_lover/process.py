"""
    Extract transactions from money lover file
"""

import pandas as pd

import global_utilities as gu
from global_utilities import log
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

    return c.PATH_MONEY_LOVER + max(names)


def main(*args, **kwa):
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


if __name__ == "__main__":
    main()
