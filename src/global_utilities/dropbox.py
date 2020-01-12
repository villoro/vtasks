"""
    Global utilities
"""

import io

import dropbox
import pandas as pd

from .secrets import get_secret


def get_dbx_connector(key):
    """
        Retrive a dropbox connector.

        Args:
            key:    name of the secret with the dropbox token
    """

    return dropbox.Dropbox(get_secret(key))


def read_excel(dbx, filename, **kwa):
    """
        Read an excel from dropbox as a pandas dataframe

        Args:
            dbx:
            filename:   name of the excel file
            **kwa:      keyworded arguments for the pd.read_excel inner function
    """

    _, res = dbx.files_download(filename)
    return pd.read_excel(io.BytesIO(res.content), **kwa)


def write_excel(dbx, df, filename, **kwa):
    """
        Write an excel to dropbox from a pandas dataframe

        Args:
            dbx:
            filename:   name of the excel file
            **kwa:      keyworded arguments for the df.to_excel inner function
    """

    output = io.BytesIO()

    writer = pd.ExcelWriter(output)
    df.to_excel(writer, **kwa)

    writer.save()
    output.seek(0)

    dbx.files_upload(output.getvalue(), filename, mode=dropbox.files.WriteMode.overwrite)
