from datetime import datetime
from datetime import timedelta
from datetime import timezone

import asyncio
import pandas as pd

from pandas.api.types import is_datetime64_any_dtype
from prefect import task
from prefect.client import get_client
from prefect.orion.schemas import filters
from prefect.orion.schemas import sorting

import utils as u

from . import constants as c
from .models import Flow
from .models import FlowRun
from .models import TaskRun

MAX_RECORDS_PER_QUERY = 200
HOURS_TO_QUERY = 6


async def read_flows():
    """extract task runs"""
    client = get_client()
    return await client.read_flows()


async def _read_flow_runs(offset, flow_run_filter=None):
    """extract flow runs with query limits"""
    client = get_client()
    return await client.read_flow_runs(
        sort=sorting.FlowRunSort.START_TIME_DESC, flow_run_filter=flow_run_filter, offset=offset
    )


async def read_all_flow_runs(flow_run_filter=None, max_queries=100):
    """extract flow runs iterating to avoid query limits"""

    log = u.get_log()
    flow_runs = []

    for x in range(max_queries):
        log.info(f"    Starting iteration {x+1}/{max_queries}")
        offset = x * MAX_RECORDS_PER_QUERY
        response = await _read_flow_runs(offset=offset, flow_run_filter=flow_run_filter)
        if not response:
            break
        flow_runs += response

    log.info(f"All flow_runs ({len(flow_runs)}) extracted in {x} API calls ({max_queries=})")
    return flow_runs


async def query_all_flow_runs(
    start_time_min=datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
):
    client = get_client()
    filter_params = {}

    if start_time_min:
        filter_params["start_time"] = filters.FlowRunFilterStartTime(after_=start_time_min)

    flow_run_filter = filters.FlowRunFilter(**filter_params)
    return await read_all_flow_runs(flow_run_filter)


async def _read_task_runs(offset, task_run_filter=None):
    """extract task runs with query limits"""
    client = get_client()
    return await client.read_task_runs(
        sort=sorting.TaskRunSort.END_TIME_DESC, task_run_filter=task_run_filter, offset=offset
    )


async def read_all_task_runs(task_run_filter=None, max_queries=200):
    """extract task runs iterating to avoid query limits"""

    log = u.get_log()
    task_runs = []

    for x in range(max_queries):
        log.info(f"    Starting iteration {x+1}/{max_queries}")
        offset = x * MAX_RECORDS_PER_QUERY
        response = await _read_task_runs(offset=offset, task_run_filter=task_run_filter)
        if not response:
            break
        task_runs += response

    log.info(f"All task_runs ({len(task_runs)}) extracted in {x} API calls ({max_queries=})")
    return task_runs


async def query_all_task_runs(
    name_like=None,
    env=None,
    state_names=["Completed"],
    start_time_min=datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
):
    client = get_client()
    filter_params = {}

    if name_like:
        filter_params["name"] = filters.TaskRunFilterName(like_=name_like)

    if env:
        filter_params["tags"] = filters.TaskRunFilterTags(all_=[f"env:{env}"])

    if state_names:
        filter_params["state"] = filters.TaskRunFilterState(
            name=filters.TaskRunFilterStateName(any_=state_names)
        )

    if start_time_min:
        filter_params["start_time"] = filters.TaskRunFilterStartTime(after_=start_time_min)

    task_run_filter = filters.TaskRunFilter(**filter_params)
    return await read_all_task_runs(task_run_filter)


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


def get_history(vdp, parquet_path):
    log = u.get_log()

    if not vdp.file_exists(parquet_path):
        return None

    log.info(f"Reading {parquet_path=}")
    df_history = vdp.read_parquet(parquet_path)
    return handle_localization(df_history)


def get_last_update(df_history):
    if df_history is None:
        return None
    return df_history.loc[df_history[c.COL_ENV] == "prod", c.COL_START].max()


def update_parquet(vdp, df_new, df_history, parquet_path):
    log = u.get_log()

    vdp = u.get_vdropbox()

    if df_history is not None:
        log.info(f"Updating {parquet_path=}")
        df = pd.concat([df_new, df_history])
        df = deduplicate(df)
    else:
        log.info(f"Exporting {parquet_path=}")
        df = df_new.copy()

    vdp.write_parquet(df, parquet_path)
    log.info(f"{parquet_path=} exported")


def add_flow_name(df_in, flows):
    df = df_in.copy()

    flow_map = parse_prefect(flows, Flow)[c.COL_NAME].to_dict()
    df[c.COL_FLOW_NAME] = df[c.COL_FLOW_ID].map(flow_map)

    return df


@task(name="vtasks.vprefect.flow_runs", retries=3, retry_delay_seconds=5)
def process_flow_runs():
    vdp = u.get_vdropbox()
    log = u.get_log()

    log.info("Querying last_update")
    df_history = get_history(vdp, c.PATH_FLOW_RUNS)
    last_update = get_last_update(df_history)

    log.info("Querying flows")
    flows = asyncio.run(read_flows())
    log.info("Querying flow_runs")
    start_time_min = last_update - timedelta(hours=HOURS_TO_QUERY)
    flow_runs = asyncio.run(query_all_flow_runs(start_time_min=start_time_min))

    log.info("Removing invalid flow_runs")
    flow_runs = [x for x in flow_runs if x.start_time is not None]

    log.info("Processing flow_runs")
    df_new = parse_prefect(flow_runs, FlowRun)
    df_new = extract_tags(df_new)

    # Exclude running flows
    df_new = df_new[df_new[c.COL_STATE] != c.STATE_RUNNING]

    # Retrive flow_name from flows
    df_new = add_flow_name(df_new, flows)

    update_parquet(vdp, df_new, df_history, c.PATH_FLOW_RUNS)


@task(name="vtasks.vprefect.task_runs", retries=3, retry_delay_seconds=5)
def process_task_runs():
    vdp = u.get_vdropbox()
    log = u.get_log()

    log.info("Querying last_update")
    df_history = get_history(vdp, c.PATH_TASK_RUNS)
    last_update = get_last_update(df_history)

    log.info("Querying task_runs")
    start_time_min = last_update - timedelta(hours=HOURS_TO_QUERY)
    task_runs = asyncio.run(query_all_task_runs(start_time_min=start_time_min, state_names=None))

    log.info("Removing invalid task_runs")
    task_runs = [x for x in task_runs if x.start_time is not None]

    log.info("Processing task_runs")
    df_new = parse_prefect(task_runs, TaskRun)
    df_new = extract_tags(df_new)

    # Exclude running tags
    df_new = df_new[df_new[c.COL_STATE] != c.STATE_RUNNING]

    update_parquet(vdp, df_new, df_history, c.PATH_TASK_RUNS)
