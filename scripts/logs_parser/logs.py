import re
import os

from datetime import timedelta, datetime, date, timezone
from pathlib import Path

import yaml
import pandas as pd

from tqdm import tqdm
from pydantic import BaseModel

PATH_LOGS = "C:/GIT/vtasks/logs"
REGEX_CLEAN_TIME = re.compile(
    r"(?P<task>[\w_]*)\s(ended|done)\s(in)\s(?P<time>\d*\.\d*)\s(?P<unit>(min|s))"
)
REGEX_LUIGI_START = re.compile(r"-\sStarting\s(?P<task>\w*)")
REGEX_LUIGI_END = re.compile(
    r"-\s(?P<task>[\w_]*)\s(ended)\s(in)\s(?P<time>\d*\.\d*)\s(?P<unit>(min|s))"
)
REGEX_LUIGI_END_VTASKS = re.compile(r"- End of (?P<task>vtasks)")
REGEX_PREFECT_START = re.compile(r"- Task '(?P<task>[\w_]*)': [Ss]tarting task run")
REGEX_PREFECT_END = re.compile(
    r"- Task '(?P<task>[\w_]*)': [Ff]inished task run for task with final state: '(?P<state>\w*)'"
)

LAST_LUIGI_AT = datetime(2020, 10, 18, 11, 57, 0)

PATH_MAPPINGS = Path(__file__).parent / "mappings.yaml"


class Task(BaseModel):
    name: str
    start: datetime
    end: datetime = None
    state: str = "Unknown"
    run: int


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


# deprecated
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


def extract_tasks_smart(lines):
    terminal = []
    started = {}

    run = 0

    for x in lines:

        # Starts
        for regex in REGEX_LUIGI_START, REGEX_PREFECT_START:
            out = regex.search(x)
            if out:
                name = out.groupdict()["task"]

                if name in ["vtasks", "run"]:
                    run += 1

                if name in started:
                    failed = started[name]
                    failed.state = "Failed"

                    terminal.append(failed)

                started[name] = Task(name=name, start=x[:23], run=run)

        for regex in REGEX_LUIGI_END, REGEX_LUIGI_END_VTASKS, REGEX_PREFECT_END:
            out = regex.search(x)
            if out:
                data = out.groupdict()
                name = data["task"]

                if not name in started:
                    continue

                task = started.pop(name)
                task.end = x[:23]
                if "state" in data:
                    task.state = data["state"]

                terminal.append(Task(**task.dict()))

    terminal += list(started.values())
    return [x.dict() for x in terminal]


def parse_all_logs(extract_func):
    data = []
    for path in tqdm(get_log_paths(), desc="Parsing logs"):
        data += extract_func(read_lines(path))

    return pd.DataFrame(data).sort_values("start")


# deprecated
def cast_columns(df_in):
    df = df_in.copy()

    df["time"] = df["time"].apply(float)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    mask = df["unit"] == "min"
    df.loc[mask, "time"] = 60 * df.loc[mask, "time"]

    return df[["timestamp", "task", "time"]]


def mark_failed_runs(df_in):
    df = df_in.copy()

    mask = (
        df["state"].isin(["Failed", "TriggerFailed"])
        & (df["name"] != "vtasks")
        & (df["name"] != "vtasks")
    )
    df_failed_runs = df[mask].groupby(["day", "run"])["name"].count().to_frame().reset_index()

    rows = [row for _, row in df_failed_runs.iterrows()]
    for row in tqdm(rows, desc="Marking failed"):
        mask = (df["day"] == row["day"]) & (df["run"] == row["run"]) & (df["name"] == "vtasks")
        df.loc[mask, "state"] = "Failed"

    return df


def get_maps():
    with open(PATH_MAPPINGS, "r") as file:
        return yaml.safe_load(file)


def clean_results(df_in):
    df = df_in.copy()

    # Remove prefect params since they are not real tasks
    df = df[~df["name"].isin(["pro", "mdate"])]

    df["day"] = df["start"].dt.date

    # Fix Luigi States
    mask = (df["start"] < LAST_LUIGI_AT) & df["end"].notna() & (df["state"] == "Unknown")
    df.loc[mask, "state"] = "Success"

    # Mark failed
    df = mark_failed_runs(df)

    # Mark prefect correct runs
    mask = (df["start"] > LAST_LUIGI_AT) & df["end"].notna() & (df["state"] == "Unknown")
    df.loc[mask, "state"] = "Success"

    # Mark stopped runs
    mask = df["end"].isna() & (df["state"] == "Unknown")
    df.loc[mask, "state"] = "Stopped"

    # Add time spent
    df["time"] = None
    mask = df["end"].notna()
    df.loc[mask, "time"] = (df.loc[mask, "end"] - df.loc[mask, "start"]) / timedelta(seconds=1)

    # Map task names
    df["name"] = df["name"].map(get_maps())

    # Drop unwanted states
    df = df[df["state"].isin(["Failed", "Success", "Stopped", "TriggerFailed"])]

    return df


def merge_task_into_flows(df_in):
    df = df_in.copy()

    df.loc[:, "state_code"] = 0
    df.loc[df["state"] == "Success", "state_code"] = 1

    df = (
        df.groupby(["name", "day", "run"])
        .agg(
            **{
                "start": pd.NamedAgg(column="start", aggfunc="min"),
                "end": pd.NamedAgg(column="end", aggfunc="max"),
                "time": pd.NamedAgg(column="time", aggfunc="sum"),
                "state": pd.NamedAgg(column="state_code", aggfunc="prod"),
            }
        )
        .reset_index()
        .sort_values("start")
        .reset_index(drop=True)
    )
    df.loc[df["end"].isna(), "time"] = None
    mask = (df["end"] - df["start"]) / timedelta(seconds=1) / df["time"] > 1.0
    df.loc[mask, "start"] = df.loc[mask, "end"] - df.loc[mask, "time"].apply(
        lambda x: timedelta(seconds=x)
    )
    return df


def proper_index(df_in):
    df = df_in.copy()

    df["id"] = ""
    df.loc[df["start"] < LAST_LUIGI_AT, "id"] = "luigi-"
    df.loc[df["start"] > LAST_LUIGI_AT, "id"] = "prefect-"

    df["id"] = df["id"] + df.index.astype(str)

    df = df.set_index("id", drop=True)
    df.index.name = ""

    return df


def to_prefect_data(df_in):
    df = df_in.copy()

    df["state_name"] = df["state"].map({0: "Failed", 1: "Completed"})
    df["created"] = df["start"].dt.tz_localize(timezone.utc)
    df["exported_at"] = datetime.now(timezone.utc)
    df["start_time"] = df["start"].dt.tz_localize(timezone.utc)
    df["end_time"] = df["end"].dt.tz_localize(timezone.utc)
    df["total_run_time"] = df["time"]
    df["flow_name"] = df["name"]
    df["run_count"] = df["run"]

    cols = [x for x in df.columns if x not in df_in.columns]
    return df[cols]


def main():
    dfg = parse_all_logs(extract_tasks_smart)
    dfg = clean_results(dfg)
    dfg.to_parquet("old_tasks.parquet")

    df = merge_task_into_flows(dfg)
    df = proper_index(df)
    df = to_prefect_data(df)
    df.to_parquet("flows.parquet")
    return dfg, df
