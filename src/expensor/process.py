"""
    Extract transactions from money lover file
"""

import os
import io

import dropbox
import pandas as pd

from global_utilities import get_secret
from . import constants as c
from . import utilities as u

import os


DBX = dropbox.Dropbox(get_secret(c.VAR_DROPBOX_TOKEN))


def get_money_lover_filename():
    """ gets the name of the money lover excel file """

    names = []

    # Explore all files and save all that are valid
    for x in DBX.files_list_folder(c.PATH_MONEY_LOVER).entries:
        try:
            # Try to parse date, if possible if a money lover file
            pd.to_datetime(x.name.split(".")[0])
            names.append(x.name)

        except (TypeError, ValueError):
            pass

    return max(names)


def get_df_transactions():
    """
        Retrives the df with transactions. It will read the newest money lover excel file

        Args:
            dbx:    dropbox connector needed to call the dropbox api

        Returns:
            raw dataframe with transactions
    """
    filename = get_money_lover_filename()

    _, res = DBX.files_download(c.PATH_MONEY_LOVER + filename)

    return u.fix_df_trans(pd.read_excel(io.BytesIO(res.content), index_col=0))


def upload_excel(df, filename):
    """ Uploads a pandas dataframe as an excel to dropbox """

    output = io.BytesIO()

    writer = pd.ExcelWriter(output)
    df.to_excel(writer)

    writer.save()
    output.seek(0)

    DBX.files_upload(output.getvalue(), filename, mode=dropbox.files.WriteMode.overwrite)


def main(*args, **kwa):
    """ Retrives all dataframes and update DFS global var """

    df = get_df_transactions()
    upload_excel(df, c.FILE_TRANSACTIONS)

    return "Transactions processed"


if __name__ == "__main__":
    main()
