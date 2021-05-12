"""
    Extract transactions from money lover file
"""

import pandas as pd
import re

from . import constants as c
from prefect_task import vtask
from utils import get_vdropbox
from utils import log

REGEX_MONEY_LOVER = r"^(MoneyLover-)?\d{4}-\d{2}-\d{2}(.xls|.csv)$"
REGEX_DATE = r"(\d{4}-\d{2}-\d{2})"


def get_money_lover_df(vdp):
    """ gets the name of the money lover excel file """

    # Get all money_lover files in a list
    files = {}
    for file in vdp.ls(c.PATH_MONEY_LOVER):
        if re.search(REGEX_MONEY_LOVER, file):

            # Extract the date as string from the name
            date_str = re.search(REGEX_DATE, file).group()

            files[date_str] = file

    # Get the name of the file with the greatest date
    last_file = sorted(files.items())[-1]
    last_filename = last_file[1]

    # Iterate all files and transform all to parquet except the last one
    for date, filename in sorted(files.items()):

        uri_in = f"{c.PATH_MONEY_LOVER}/{filename}"
        uri_out = f"{c.PATH_MONEY_LOVER}/{date[:4]}/{date}.parquet"

        log.info(f"Reading '{uri_in}' from dropbox")

        extension = file.split(".")[-1]
        if extension == "csv":
            df = vdp.read_csv(uri_in, index_col=0, sep=";")
        else:
            df = vdp.read_excel(uri_in, index_col=0)

        # Return the list file
        if filename == last_filename:
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
    df[c.COL_DATE] = pd.to_datetime(df[c.COL_DATE], dayfirst=True)

    # Tag expenses/incomes
    df.loc[df[c.COL_AMOUNT] > 0, c.COL_TYPE] = c.INCOMES
    df[c.COL_TYPE].fillna(c.EXPENSES, inplace=True)

    # Amount as positve number
    df[c.COL_AMOUNT] = df[c.COL_AMOUNT].apply(abs)

    return df[c.COLS_DF_TRANS]


@vtask
def money_lover():
    """ Retrives all dataframes and update DFS global var """

    vdp = get_vdropbox()

    # Read
    df = get_money_lover_df(vdp)

    # Transform
    df = transform_transactions(df)

    # Export
    vdp.write_excel(df, c.FILE_TRANSACTIONS)
