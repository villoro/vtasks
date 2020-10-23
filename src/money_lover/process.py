"""
    Extract transactions from money lover file
"""

import pandas as pd

from prefect import task

import global_utilities as gu

from . import constants as c
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


def fix_df_trans(df_in):
    """
        It does all required transformations in order to use the transaction dataframe

        Args:
            df_in:  raw dataframe with transactions
    """

    df = df_in.rename(c.REPLACES_DF_TRANS, axis="columns").copy()
    df = df[~df[c.COL_CATEGORY].isin(c.FORBIDDEN_CATEGORIES)]

    # Add time filter columns (store everything as string to ensure JSON compatibility)
    df[c.COL_DATE] = pd.to_datetime(df[c.COL_DATE])
    df[c.COL_MONTH_DATE] = pd.to_datetime(df[c.COL_DATE].dt.strftime("%Y-%m-01"))
    df[c.COL_MONTH] = df[c.COL_DATE].dt.month
    df[c.COL_YEAR] = df[c.COL_DATE].dt.year

    # Tag expenses/incomes
    df.loc[df[c.COL_AMOUNT] > 0, c.COL_TYPE] = c.INCOMES
    df[c.COL_TYPE].fillna(c.EXPENSES, inplace=True)

    # Amount as positve number
    df[c.COL_AMOUNT] = df[c.COL_AMOUNT].apply(abs)

    return df[c.COLS_DF_TRANS]


@task
def money_lover(mdate):
    """ Retrives all dataframes and update DFS global var """

    # Get data
    dbx = gu.dropbox.get_dbx_connector(c.VAR_DROPBOX_TOKEN)
    filename = get_money_lover_filename(dbx)
    df = gu.dropbox.read_excel(dbx, filename, index_col=0)
    log.info(f"File '{filename}' read from dropbox")

    # Process dataframe
    df = fix_df_trans(df)

    # Store data
    gu.dropbox.write_excel(dbx, df, c.FILE_TRANSACTIONS)
