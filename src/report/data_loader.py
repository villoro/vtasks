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


def upload_text_file(text, filename):
    """ Uploads a text file in dropbox. """

    with io.BytesIO(text.encode()) as stream:
        stream.seek(0)

        # Write a text file
        DBX.files_upload(stream.read(), filename, mode=dropbox.files.WriteMode.overwrite)


def upload_yaml(data, filename):
    """
        Uploads a dict/ordered dict as yaml in dropbox.

        Args:
            data:       dict or dict-like info
            filename:   info to read

    """

    with io.StringIO() as file:
        yaml.dump(data, file, default_flow_style=False)
        file.seek(0)

        DBX.files_upload(file.read().encode(), filename, mode=dropbox.files.WriteMode.overwrite)


def read_yaml(filename):
    """ Read a yaml as an ordered dict from dropbox """

    _, res = DBX.files_download(filename)

    with io.BytesIO(res.content) as stream:
        return yaml.safe_load(stream)
