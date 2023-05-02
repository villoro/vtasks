import glob
import re
import os

import pandas as pd

from loguru import logger as log

PATH_IN = r"C:/Program Files (x86)/OpenHardwareMonitor"
PATH_OUT = r"C:/Users/Villoro/Dropbox/Aplicaciones/OpenHardwareMonitor"

MAPPINGS = {
    "/lpc/nct6793d/temperature/0": "temp/CPU",
    "/nvidiagpu/0/temperature/0": "temp/GPU",
    "/hdd/0/temperature/0": "temp/HD/D",
    "/hdd/1/temperature/0": "temp/HD/E",
    "/hdd/2/temperature/0": "temp/HD/G",
    "/nvidiagpu/0/fan/0": "fan/GPU",
    "/lpc/nct6793d/fan/0": "fan/front/phanteks",
    "/lpc/nct6793d/fan/1": "fan/CPU",
    "/lpc/nct6793d/fan/2": "fan/front/arctic",
    "/lpc/nct6793d/fan/3": "fan/top/arctic)",
    "/intelcpu/0/load/0": "load/CPU",
    "/nvidiagpu/0/load/0": "load/GPU",
    "/intelcpu/0/power/0": "W/CPU",
    "/nvidiagpu/0/power/0": "W/GPU",
}

REGEX_DATE = re.compile("OpenHardwareMonitorLog-(?P<date>[\w-]+)\.csv")


def parse_data(filename):
    df = pd.read_csv(filename, header=[0, 1]).droplevel(1, axis=1)

    cols_to_keep = list(set(MAPPINGS).intersection(set(df.columns)))

    return df[cols_to_keep].rename(columns=MAPPINGS).sort_index(axis=1, ascending=False)


def process_files():
    for filename in glob.glob(f"{PATH_IN}/OpenHardwareMonitorLog-*.csv")[:-1]:  # Skip today file
        log.info(f"Processing {filename=}")

        match = REGEX_DATE.match(filename.split("\\")[-1])

        if not match:
            log.warning(f"No matches for {filename=}")
            continue

        date = match.groupdict()["date"]

        df = parse_data(filename)
        df.to_parquet(f"{PATH_OUT}/{date}.parquet")
        log.success(f"File '{date}.parquet' extracted")

        log.info(f"Removing {filename=}")
        os.remove(filename)


if __name__ == "__main__":
    process_files()
