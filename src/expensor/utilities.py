"""
    Utilities for pandas dataframes
"""

import pandas as pd
from v_crypt import Cipher

from . import constants as c
from . import config


cipher = Cipher(secrets_file=config.FILE_SECRETS, filename_master_password=config.FILE_MASTER_PASSWORD)


def get_secret(key):
    """ Retrives one encrypted secret """
    return cipher.get_secret(key)


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
