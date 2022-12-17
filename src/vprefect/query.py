from datetime import datetime
from datetime import timezone

import asyncio
import pandas as pd

from pandas.api.types import is_datetime64_any_dtype
from prefect import get_run_logger
from prefect import task
from prefect.client import get_client
from prefect.orion.schemas import filters

import utils as u

from . import constants as c
from .models import Flow
from .models import FlowRun
from .models import TaskRun


async def read_flows():
    """extract task runs"""
    client = get_client()
    return await client.read_flows()


async def read_flow_runs():
    """extract flow runs"""
    client = get_client()
    return await client.read_flow_runs()


async def read_task_runs(task_run_filter=None):
    """extract task runs"""
    client = get_client()
    return await client.read_task_runs(task_run_filter=task_run_filter)


async def query_task_runs(
    name_like,
    state_names=["Completed"],
    start_time_min=datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
):
    client = get_client()
    filter_params = {}

    if name_like:
        filter_params["name"] = filters.TaskRunFilterName(like_=name_like)

    if state_names:
        filter_params["state"] = filters.TaskRunFilterState(
            name=filters.TaskRunFilterStateName(any_=state_names)
        )

    if start_time_min:
        filter_params["start_time"] = filters.TaskRunFilterStartTime(after_=start_time_min)

    task_run_filter = filters.TaskRunFilter(**filter_params)
    return await read_task_runs(task_run_filter)


def handle_localization(df_in):
    df = df_in.copy()

    # There has been some problems with timezones,
    # make sure to remove them everywhere
    for col in [c.COL_CREATED, c.COL_EXPORTED_AT, c.COL_START, c.COL_END]:

        # Skip columns that are not present in some models
        if col not in df.columns:
            continue

        # Cast to datetime when needed
        if not is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], utc=True)

        # If missing timezone tag it as UTC
        if df[col].dt.tz is None:
            df[col] = df[col].dt.tz_localize(timezone.utc)

    return df


def parse_prefect(prefect_list, Model):
    data = [Model(**x.dict()).dict() for x in prefect_list]
    df = pd.DataFrame(data).set_index("id")
    return handle_localization(df)


def _extract_env(tags):
    for x in tags:
        k, v = x.split(":")
        if k == "env":
            return v


def extract_tags(df_in):
    df = df_in.copy()

    # Tags to proper list
    mask = df[c.COL_TAGS].notna()
    df.loc[mask, c.COL_TAGS] = df.loc[mask, c.COL_TAGS].apply(eval)

    # Extract env
    if c.COL_ENV not in df.columns:
        df[c.COL_ENV] = None
    df.loc[mask, c.COL_ENV] = df.loc[mask, c.COL_TAGS].apply(_extract_env)

    return df


def deduplicate(df_in):
    df = df_in.copy()
    df = df.sort_values([c.COL_CREATED, c.COL_EXPORTED_AT])
    return df[~df.index.duplicated(keep="first")]


def update_parquet(df_new, parquet_path):

    vdp = u.get_vdropbox()

    if vdp.file_exists(parquet_path):
        df_history = vdp.read_parquet(parquet_path)
        df_history = handle_localization(df_history)

        df = pd.concat([df_new, df_history])
        df = deduplicate(df)
    else:
        df = df_new.copy()

    vdp.write_parquet(df, parquet_path)


def add_flow_name(df_in, flows):

    df = df_in.copy()

    flow_map = parse_prefect(flows, Flow)[c.COL_NAME].to_dict()
    df[c.COL_FLOW_NAME] = df[c.COL_FLOW_ID].map(flow_map)

    return df


@task(name="vtasks.vprefect.flow_runs")
def process_flow_runs():
    flows = asyncio.run(read_flows())
    flow_runs = asyncio.run(read_flow_runs())

    df_new = parse_prefect(flow_runs, FlowRun)
    df_new = extract_tags(df_new)

    # Exclude running flows
    df_new = df_new[df_new[c.COL_STATE] != c.STATE_RUNNING]

    # Retrive flow_name from flows
    df_new = add_flow_name(df_new, flows)

    update_parquet(df_new, c.PATH_FLOW_RUNS)


@task(name="vtasks.vprefect.task_runs")
def process_task_runs():
    task_runs = asyncio.run(read_task_runs())

    df_new = parse_prefect(task_runs, TaskRun)
    df_new = extract_tags(df_new)

    update_parquet(df_new, c.PATH_TASK_RUNS)
