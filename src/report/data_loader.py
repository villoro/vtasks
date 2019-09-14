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
    with io.BytesIO(res.content) as stream:
        return yaml.safe_load(stream)


def get_dfs():
    """ Retrives all dataframes """

    # Dataframes from data.xlsx
    _, res = DBX.files_download(c.FILE_DATA)
    with io.BytesIO(res.content) as stream:
        dfs = {x: pd.read_excel(stream, sheet_name=x) for x in c.DFS_ALL_FROM_DATA}

    # Transactions
    _, res = DBX.files_download(c.FILE_TRANSACTIONS)
    with io.BytesIO(res.content) as stream:
        dfs[c.DF_TRANS] = pd.read_excel(stream, index_col=0)

    return dfs
