import re
import os

from datetime import timedelta, datetime

import pandas as pd

from tqdm import tqdm

PATH_LOGS = "C:/GIT/vtasks/logs"
REGEX_CLEAN_TIME = re.compile(
    r"(?P<task>[\w_]*)\s(ended|done)\s(in)\s(?P<time>\d*\.\d*)\s(?P<unit>(min|s))"
)
REGEX_LUIGI_START = re.compile(r"-\sStarting\s(?P<task>\w*)")
REGEX_LUIGI_END = re.compile(
    r"-\s(?P<task>[\w_]*)\s(ended)\s(in)\s(?P<time>\d*\.\d*)\s(?P<unit>(min|s))"
)
REGEX_LUIGI_END_VTASKS = re.compile(r"- End of (?P<task>vtasks)")
REGEX_PREFECT_START = re.compile(r"- Task '(?P<task>[\w_]*)': Starting task run")
REGEX_PREFECT_END = re.compile(r"- Task '(?P<task>[\w_]*)': finished task run")


def get_log_paths():
    paths = []
    for root, _, files in os.walk(PATH_LOGS):
        for file in files:
            path = f"{root}/{file}".replace("\\", "/")
            paths.append(path)
    return paths


def read_lines(path):
    with open(path, "r", encoding="latin1") as stream:
        data = stream.read()
    return data.split("\n")


def extract_times(lines):
    data = []
    for x in lines:
        out = REGEX_CLEAN_TIME.search(x)
        if not out:
            continue

        mdict = out.groupdict()
        mdict["timestamp"] = x[:23]

        data.append(mdict)

    return data


def parse_all_logs(tqdm_f=tqdm):
    data = []
    for path in tqdm_f(get_log_paths()):
        data += extract_times(read_lines(path))

    return pd.DataFrame(data)


def cast_columns(df_in):
    df = df_in.copy()

    df["time"] = df["time"].apply(float)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    mask = df["unit"] == "min"
    df.loc[mask, "time"] = 60 * df.loc[mask, "time"]

    return df[["timestamp", "task", "time"]]


def to_prefect_data(df_in):
    df = df_in.copy()

    df["state_name"] = "Completed"
    df["created"] = df["timestamp"]
    df["exported_at"] = datetime.now()
    df["start_time"] = df["timestamp"] - df["time"].apply(lambda x: timedelta(seconds=x))
    df["end_time"] = df["timestamp"]
    df["total_run_time"] = df["time"]
    df["flow_name"] = df["task"]

    cols = [x for x in df.columns if x not in df_in.columns]

    return df[cols]
