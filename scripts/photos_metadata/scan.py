import os
import re

import pandas as pd

from tqdm.notebook import tqdm
from loguru import logger as log

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

    for root, _, files in tqdm(os.walk(base_path), total=count):

        root = root.replace("\\", "/")
        log.debug(f"Scanning {root=} with {len(files)} files")

        level, folder_date = get_folder_date(root)

        for file in files:
            data = Doc(folder=root, name=file, folder_date=folder_date, level=level).load()
            out.append(data)

    return pd.DataFrame(out)
