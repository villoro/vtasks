"""
    Utilities for input/output operations.
"""

import io

import dropbox
import pandas as pd
import oyaml as yaml


from . import constants as c
from . import utilities as u

DBX = dropbox.Dropbox(u.get_secret(c.VAR_DROPBOX_TOKEN))


def get_config():
    """ retrives config yaml as ordered dict """

    _, res = DBX.files_download(c.FILE_CONFIG)
    return yaml.load(io.BytesIO(res.content), Loader=yaml.SafeLoader)


def get_dfs():
    """ Retrives all dataframes """

    # Dataframes from data.xlsx
    _, res = DBX.files_download(c.FILE_DATA)
    dfs = {x: pd.read_excel(io.BytesIO(res.content), sheet_name=x) for x in c.DFS_ALL_FROM_DATA}

    # Transactions
    _, res = DBX.files_download(c.FILE_TRANSACTIONS)
    dfs[c.DF_TRANS] = pd.read_excel(io.BytesIO(res.content), index_col=0)

    return dfs
