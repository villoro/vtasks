import glob
import os
import re

import pandas as pd
from loguru import logger as log
from mappings import MAPPINGS

PATH_IN = r"C:/Program Files (x86)/OpenHardwareMonitor"
PATH_OUT = r"C:/Users/Villoro/Dropbox/Aplicaciones/OpenHardwareMonitor"

REGEX_DATE = re.compile("OpenHardwareMonitorLog-(?P<date>[\w-]+)\.csv")


def get_pc_name_and_columns(columns):
    name = None
    max_intersection = []

    for x, maps in MAPPINGS.items():
        intersection = list(set(maps).intersection(set(columns)))

        if len(intersection) > len(max_intersection):
            name = x
            max_intersection = intersection

    return name, max_intersection


def parse_data(filename):
    df = pd.read_csv(filename, header=[0, 1], index_col=0).droplevel(1, axis=1)
    df.index.name = "ts"

    cols_to_keep = list(set(MAPPINGS).intersection(set(df.columns)))

    pc_name, cols = get_pc_name_and_columns(df.columns)
    df = df[cols].rename(columns=MAPPINGS[pc_name]).sort_index(axis=1, ascending=False)

    df["PC"] = pc_name

    return df.reset_index()


def process_files():
    for filename in glob.glob(f"{PATH_IN}/OpenHardwareMonitorLog-*.csv")[
        :-1
    ]:  # Skip today file
        log.info(f"Processing {filename=}")

        match = REGEX_DATE.match(filename.split("\\")[-1])

        if not match:
            log.warning(f"No matches for {filename=}")
            continue

        date = match.groupdict()["date"]

        df = parse_data(filename)
        pc_name = df.at[0, "PC"]  # Extract from the just created 'PC' column

        df.to_parquet(f"{PATH_OUT}/{pc_name}_{date}.parquet")
        log.success(f"File '{pc_name}_{date}.parquet' extracted")

        log.info(f"Removing {filename=}")
        os.remove(filename)


if __name__ == "__main__":
    process_files()
