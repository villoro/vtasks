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
DATETIME_A_WEEK_AGO = datetime.now(timezone.utc).replace(
    hour=0, minute=0, second=0, microsecond=0
) - timedelta(days=7)


def extract_async_tasks(tasks):
    """
    Extracts results from a list of asynchronous tasks into a single list.

    Args:
        tasks (list): List of asynchronous tasks.

    Returns:
        list: List containing the results of the asynchronous tasks.
    """
    out = []
    for task in tasks:
        out += task.result()
    return out


async def read_flows():
    """
    Asynchronously retrieves the list of flows using the Prefect client.

    Returns:
        list: List of flows.
    """
    client = get_client()
    return await client.read_flows()


async def _read_flow_runs(offset, flow_run_filter=None, sort=sorting.FlowRunSort.START_TIME_DESC):
    """
    Asynchronously retrieves flow runs with query limits.

    Args:
        offset (int): Offset for paginating the results.
        flow_run_filter (prefect.orion.schemas.filters.FlowRunFilter, optional): Filter for flow runs. Defaults to None.
        sort (prefect.orion.schemas.sorting.FlowRunSort, optional): Sorting options. Defaults to sorting.FlowRunSort.START_TIME_DESC.

    Returns:
        list: List of flow runs.
    """
    client = get_client()
    return await client.read_flow_runs(sort=sort, flow_run_filter=flow_run_filter, offset=offset)


async def read_all_flow_runs(flow_run_filter=None, queries_per_batch=4, max_queries=100):
    """
    Asynchronously extracts flow runs in concurrent batches to speed up retrieval.

    Args:
        flow_run_filter (prefect.orion.schemas.filters.FlowRunFilter, optional): Filter for flow runs. Defaults to None.
        queries_per_batch (int, optional): Number of queries per batch. Defaults to 4.
        max_queries (int, optional): Maximum number of queries. Defaults to 100.

    Returns:
        list: List of flow runs.
    """

    log = u.get_log()
    async_tasks = []
    max_batches = max_queries // queries_per_batch

    for batch in range(max_batches):
        log.info(f"    Starting batch {batch+1}/{max_batches}")

        # Do batch of concurrent tasks
        async with asyncio.TaskGroup() as tg:
            for task_num in range(queries_per_batch):
                offset = (task_num + queries_per_batch * batch) * MAX_RECORDS_PER_QUERY
                async_task = tg.create_task(
                    _read_flow_runs(offset=offset, flow_run_filter=flow_run_filter)
                )
                async_tasks.append(async_task)

        # If last query returned no results or less than max, stop iterations
        if len(async_tasks[-1].result()) < MAX_RECORDS_PER_QUERY:
            break

    flow_runs = extract_async_tasks(async_tasks)

    log.info(f"All flow_runs ({len(flow_runs)}) extracted in {batch+1} batches ({max_batches=})")
    return flow_runs


async def query_all_flow_runs(
    name_like=None,
    env=None,
    state_names=(),
    start_time_min=DATETIME_A_WEEK_AGO,
    queries_per_batch=4,
):
    """
    Asynchronously queries flow runs with some predefined filters.

    Args:
        name_like (str, optional): Partial name of the task. Defaults to None.
        env (str, optional): Environment tag. Defaults to None.
        state_names (list, optional): List of state names to include. Defaults to ().
        start_time_min (datetime, optional): Minimum start time for the query range. Defaults to the start of the current day.
        queries_per_batch (int, optional): Number of queries per batch. Defaults to 4.

    Returns:
        list: List of flow runs.
    """
    client = get_client()
    filter_params = {}

    if name_like:
        filter_params["name"] = filters.FlowRunFilterName(like_=name_like)

    if env:
        filter_params["tags"] = filters.FlowRunFilterTags(all_=[f"env:{env}"])

    if state_names:
        filter_params["state"] = filters.FlowRunFilterState(
            name=filters.FlowRunFilterStateName(any_=state_names)
        )

    if start_time_min:
        filter_params["start_time"] = filters.FlowRunFilterStartTime(after_=start_time_min)

    flow_run_filter = filters.FlowRunFilter(**filter_params)
    return await read_all_flow_runs(flow_run_filter, queries_per_batch=queries_per_batch)


async def _read_task_runs(offset, task_run_filter=None, sort=sorting.TaskRunSort.END_TIME_DESC):
    """
    Asynchronously retrieves task runs with query limits.

    Args:
        offset (int): Offset for paginating the results.
        task_run_filter (prefect.orion.schemas.filters.TaskRunFilter, optional): Filter for task runs. Defaults to None.
        sort (prefect.orion.schemas.sorting.TaskRunSort, optional): Sorting options. Defaults to sorting.TaskRunSort.END_TIME_DESC.

    Returns:
        list: List of task runs.
    """
    client = get_client()
    return await client.read_task_runs(sort=sort, task_run_filter=task_run_filter, offset=offset)


async def read_all_task_runs(task_run_filter=None, max_queries=500, queries_per_batch=10):
    """
    Asynchronously extracts task runs in concurrent batches to speed up retrieval.

    Args:
        task_run_filter (prefect.orion.schemas.filters.TaskRunFilter, optional): Filter for task runs. Defaults to None.
        max_queries (int, optional): Maximum number of queries. Defaults to 500.
        queries_per_batch (int, optional): Number of queries per batch. Defaults to 10.

    Returns:
        list: List of task runs.
    """

    log = u.get_log()
    async_tasks = []
    max_batches = max_queries // queries_per_batch

    for batch in range(max_batches):
        log.info(f"    Starting batch {batch+1}/{max_batches}")

        # Do batch of concurrent tasks
        async with asyncio.TaskGroup() as tg:
            for task_num in range(queries_per_batch):
                offset = (task_num + queries_per_batch * batch) * MAX_RECORDS_PER_QUERY
                async_task = tg.create_task(
                    _read_task_runs(offset=offset, task_run_filter=task_run_filter)
                )
                async_tasks.append(async_task)

        # If last query returned no results or less than max, stop iterations
        if len(async_tasks[-1].result()) < MAX_RECORDS_PER_QUERY:
            break

    task_runs = extract_async_tasks(async_tasks)

    log.info(f"All task_runs ({len(task_runs)}) extracted in {batch+1} batches ({max_batches=})")
    return task_runs


async def query_all_task_runs(
    name_like=None,
    env=None,
    state_names=["Completed"],
    start_time_min=DATETIME_A_WEEK_AGO,
    queries_per_batch=10,
):
    """
    Asynchronously queries task runs with some predefined filters.

    Args:
        name_like (str, optional): Partial name of the task. Defaults to None.
        env (str, optional): Environment tag. Defaults to None.
        state_names (list, optional): List of state names to include. Defaults to ["Completed"].
        start_time_min (datetime, optional): Minimum start time for the query range. Defaults to the start of the current day.
        queries_per_batch (int, optional): Number of queries per batch. Defaults to 10.

    Returns:
        list: List of task runs.
    """
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
    return await read_all_task_runs(task_run_filter, queries_per_batch=queries_per_batch)


def handle_localization(df_in):
    """
    Handles time localization and timezone issues in the input DataFrame.

    Args:
        df_in (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with proper time localization.
    """
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
    """
    Parses a list of Prefect objects into a DataFrame.

    Args:
        prefect_list (list): List of Prefect objects.
        Model (pydantic.BaseModel): Prefect model type.

    Returns:
        pd.DataFrame: Parsed DataFrame.
    """
    data = [Model(**x.dict()).dict() for x in prefect_list]
    df = pd.DataFrame(data).set_index("id")
    return handle_localization(df)


def _extract_env(tags):
    """
    Extracts the environment value from a list of tags.

    Args:
        tags (list): List of tags in the format ['key:value'].

    Returns:
        str: Extracted environment value.
    """
    for x in tags:
        k, v = x.split(":")
        if k == "env":
            return v


def extract_tags(df_in):
    """
    Extracts tags and environment information from the input DataFrame.

    Args:
        df_in (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with extracted tags and environment information.
    """
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
    """
    Removes duplicate rows from the input DataFrame.

    Args:
        df_in (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with duplicates removed, keeping the first occurrence.
    """
    df = df_in.copy()
    df = df.sort_values([c.COL_CREATED, c.COL_EXPORTED_AT])
    return df[~df.index.duplicated(keep="first")]


def get_history(vdp, parquet_path):
    """
    Retrieves historical data from a Parquet file using the specified VDropbox client.

    Args:
        vdp (VDropbox): VDropbox client instance.
        parquet_path (str): Path to the Parquet file.

    Returns:
        pd.DataFrame: DataFrame containing historical data, or None if the file does not exist.
    """
    log = u.get_log()

    if not vdp.file_exists(parquet_path):
        return None

    log.info(f"Reading {parquet_path=}")
    df_history = vdp.read_parquet(parquet_path)
    return handle_localization(df_history)


def get_last_update(df_history, env="prod"):
    """
    Retrieves the last update timestamp from the historical data DataFrame.

    Args:
        df_history (pd.DataFrame): DataFrame containing historical data.

    Returns:
        datetime: Last update timestamp for the specified environment.
    """
    if df_history is None:
        return None
    return df_history.loc[df_history[c.COL_ENV] == env, c.COL_START].max()


def update_parquet(vdp, df_new, df_history, parquet_path):
    """
    Updates a Parquet file with new data, handling duplicates and proper merging.

    Args:
        vdp (VDropbox): VDropbox client instance.
        df_new (pd.DataFrame): New data to be added.
        df_history (pd.DataFrame): Historical data.
        parquet_path (str): Path to the Parquet file.

    Returns:
        None
    """
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
    """
    Adds flow names to the DataFrame based on flow IDs.

    Args:
        df_in (pd.DataFrame): Input DataFrame.
        flows (list): List of flows.

    Returns:
        pd.DataFrame: DataFrame with added flow names.
    """
    df = df_in.copy()

    flow_map = parse_prefect(flows, Flow)[c.COL_NAME].to_dict()
    df[c.COL_FLOW_NAME] = df[c.COL_FLOW_ID].map(flow_map)

    return df


@task(name="vtasks.vprefect.flow_runs", retries=3, retry_delay_seconds=5)
def process_flow_runs():
    """
    Prefect task that queries flow runs, processes the data, and updates a Parquet file.

    Retrieves historical flow run data, queries new flow runs, processes the data by adding tags and flow names,
    and then updates the Parquet file with the new data.

    Returns:
        None
    """
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

    # Retrieve flow_name from flows
    df_new = add_flow_name(df_new, flows)

    update_parquet(vdp, df_new, df_history, c.PATH_FLOW_RUNS)


@task(name="vtasks.vprefect.task_runs", retries=3, retry_delay_seconds=5)
def process_task_runs():
    """
    Prefect task that queries task runs, processes the data, and updates a Parquet file.

    Retrieves historical task run data, queries new task runs, processes the data by adding tags,
    and then updates the Parquet file with the new data.

    Returns:
        None
    """
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
