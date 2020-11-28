"""
    Extract transactions from money lover file
"""

import pandas as pd
import re

from prefect import task
from vdropbox import Vdropbox

from utils import get_secret

from . import constants as c
from utils import log
from utils import timeit

MONEY_LOVER_REGEX = r"\d{4}-\d{2}-\d{2}(.xls)"


def get_money_lover_df(vdp):
    """ gets the name of the money lover excel file """

    # Get all money_lover files in a list
    files = []
    for file in vdp.ls(c.PATH_MONEY_LOVER):
        if re.search(MONEY_LOVER_REGEX, file):
            files.append(file)

    # Iterate all files and transform all to parquet except the last one
    for file in files:
        name, extension = file.split(".")

        uri_in = f"{c.PATH_MONEY_LOVER}/{file}"
        uri_out = f"{c.PATH_MONEY_LOVER}/{name[:4]}/{name}.parquet"

        log.info(f"Reading '{uri_in}' from dropbox")
        df = vdp.read_excel(uri_in, index_col=0)

        # Return the list file
        if file == files[-1]:
            return df

        log.info(f"Exporting '{uri_out}' to dropbox")
        vdp.write_parquet(df, uri_out)
        vdp.delete(uri_in)


def transform_transactions(df_in):
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
@timeit
def money_lover(mdate, export_data=True):
    """ Retrives all dataframes and update DFS global var """

    vdp = Vdropbox(get_secret(c.VAR_DROPBOX_TOKEN))

    # Read
    df = get_money_lover_df(vdp)

    # Transform
    df = transform_transactions(df)

    # Export
    if export_data:
        vdp.write_excel(df, c.FILE_TRANSACTIONS)

    return df
