"""
    Global utilities
"""

import io

import dropbox
import oyaml as yaml
import pandas as pd

from .log import log
from .secrets import get_secret


def get_dbx_connector(key):
    """
        Retrive a dropbox connector.

        Args:
            key:    name of the secret with the dropbox token
    """

    return dropbox.Dropbox(get_secret(key))


def read_yaml(dbx, filename):
    """
        Read a yaml from dropbox as an ordered dict

        Args:
            dbx:        dropbox connector
            filename:   name of the yaml file
    """

    _, res = dbx.files_download(filename)

    res.raise_for_status()

    with io.BytesIO(res.content) as stream:
        return yaml.safe_load(stream)


def write_yaml(dbx, data, filename):
    """
        Uploads a dict/ordered dict as yaml in dropbox.

        Args:
            dbx:        dropbox connector
            data:       dict or dict-like info
            filename:   name of the yaml file
    """

    with io.StringIO() as stream:
        yaml.dump(data, stream, default_flow_style=False, indent=4)
        stream.seek(0)

        dbx.files_upload(stream.read().encode(), filename, mode=dropbox.files.WriteMode.overwrite)

    log.info(f"File '{filename}' exported to dropbox")


def read_parquet(dbx, filename):
    """
        Read a parquet from dropbox as a pandas dataframe

        Args:
            dbx:        dropbox connector
            filename:   name of the parquet file
    """

    _, res = dbx.files_download(filename)

    with io.BytesIO(res.content) as stream:
        return pd.read_parquet(stream)


def write_parquet(dbx, df, filename):
    """
        Write a parquet to dropbox from a pandas dataframe.

        Args:
            dbx:        dropbox connector
            df:         pandas dataframe
            filename:   name of the yaml file
    """

    with io.BytesIO() as stream:
        df.to_parquet(stream)
        stream.seek(0)

        dbx.files_upload(stream.getvalue(), filename, mode=dropbox.files.WriteMode.overwrite)


def write_textfile(dbx, text, filename):
    """
        Uploads a text file in dropbox.

        Args:
            dbx:        dropbox connector
            text:       text to write
            filename:   name of the file
    """

    with io.BytesIO(text.encode()) as stream:
        stream.seek(0)

        # Write a text file
        dbx.files_upload(stream.read(), filename, mode=dropbox.files.WriteMode.overwrite)

    log.info(f"File '{filename}' exported to dropbox")


def read_excel(dbx, filename, sheet_names=None, **kwa):
    """
        Read an excel from dropbox as a pandas dataframe

        Args:
            dbx:            dropbox connector
            filename:       name of the excel file
            sheet_names:    names of the sheets to read (if None read the only sheet)
            **kwa:          keyworded arguments for the pd.read_excel inner function
    """

    _, res = dbx.files_download(filename)

    res.raise_for_status()

    # Read one dataframe
    if sheet_names is None:
        with io.BytesIO(res.content) as stream:
            return pd.read_excel(stream, **kwa)

    # Read multiple dataframes
    with io.BytesIO(res.content) as stream:
        return {x: pd.read_excel(stream, sheet_name=x, **kwa) for x in sheet_names}


def write_excel(dbx, df, filename, **kwa):
    """
        Write an excel to dropbox from a pandas dataframe

        Args:
            dbx:        dropbox connector
            filename:   name of the excel file
            **kwa:      keyworded arguments for the df.to_excel inner function
    """

    with io.BytesIO() as stream:
        writer = pd.ExcelWriter(stream)
        df.to_excel(writer, **kwa)

        writer.save()
        stream.seek(0)

        dbx.files_upload(stream.getvalue(), filename, mode=dropbox.files.WriteMode.overwrite)

    log.info(f"File '{filename}' exported to dropbox")
