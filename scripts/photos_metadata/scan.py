import os
import re

import pandas as pd

from tqdm.notebook import tqdm

from utils import log
from docs import Doc

REGEX = re.compile(r"(?P<year>\d{4})(_(?P<month>\d{2}))?(_(?P<day>\d{2}))?(.*)")


def get_folder_date(path):

    log.info(f"Extracting date from {path=}")

    # Find the first match going from the deepest folder
    for level, folder in enumerate(path.split("/")[::-1]):
        data = REGEX.match(folder)

        if data:
            data = data.groupdict()
            break

    else:
        log.error(f"Unable to extract data for {folder=}")
        return -1, False

    log.debug(f"Result: {data=}")

    out = str(data["year"])

    for key in ["month", "day"]:
        value = data[key]

        if (value is not None) and (int(value) > 0):
            out += f":{value}"

    return level, out


def read_everything(base_path):

    log.debug(f"Starting process for {base_path=}")

    # Get total number of steps to do
    count = len([0 for _ in os.walk(base_path)])

    out = []

    for path, _, files in tqdm(os.walk(base_path), total=count):

        path = path.replace("\\", "/")
        log.debug(f"Scanning {path=} with {len(files)} files")

        level, folder_date = get_folder_date(path)

        for file in files:
            log.debug(f"Loading {path=} {file=}")

            data = Doc(folder=path, name=file, folder_date=folder_date, level=level).load()
            out.append(data)

    return pd.DataFrame(out)


def summarize(df_in):

    df = df_in.copy()

    for col in ["is_image", "error_dt", "error_dt_original", "missing_meta"]:
        df.loc[df[col] == False, col] = None

    aggs = {
        "dt_min": pd.NamedAgg(column="datetime", aggfunc="min"),
        "dt_max": pd.NamedAgg(column="datetime", aggfunc="max"),
        "dt_original_min": pd.NamedAgg(column="datetime_original", aggfunc="min"),
        "dt_original_max": pd.NamedAgg(column="datetime_original", aggfunc="max"),
        "extensions": pd.NamedAgg(column="extension", aggfunc="unique"),
        "images": pd.NamedAgg(column="is_image", aggfunc="count"),
        "files": pd.NamedAgg(column="name", aggfunc="count"),
        "level": pd.NamedAgg(column="level", aggfunc="max"),
        "error_dt": pd.NamedAgg(column="error_dt", aggfunc="count"),
        "error_dt_original": pd.NamedAgg(column="error_dt_original", aggfunc="count"),
        "missing_meta": pd.NamedAgg(column="missing_meta", aggfunc="count"),
    }

    return df.groupby("folder").agg(**aggs).reset_index()
