from datetime import datetime

import asyncio
import pandas as pd

from prefect.client import get_client
from prefect import flow, task, get_run_logger

import utils as u

from .models import FlowRun, TaskRun, Flow

COL_EXPORTED_AT = "exported_at"
COL_CREATED = "created"
COL_NAME = "name"
COL_FLOW_ID = "flow_id"
COL_FLOW_NAME = "flow_name"

PATH_VTASKS = "/Aplicaciones/vtasks"
PATH_FLOW_RUNS = f"{PATH_VTASKS}/flows.parquet"
PATH_TASK_RUNS = f"{PATH_VTASKS}/tasks.parquet"


async def read_flows():
    """extract task runs"""
    client = get_client()
    return await client.read_flows()


async def read_flow_runs():
    """extract flow runs"""
    client = get_client()
    return await client.read_flow_runs()


async def read_task_runs():
    """extract task runs"""
    client = get_client()
    return await client.read_task_runs()


def parse_prefect(prefect_list, Model):
    data = [Model(**x.dict()).dict() for x in prefect_list]
    return pd.DataFrame(data).set_index("id")


def deduplicate(df_in):
    df = df_in.sort_values([COL_EXPORTED_AT, COL_CREATED]).copy()
    return df[~df.index.duplicated(keep="first")]


def update_parquet(df_new, parquet_path):

    vdp = u.get_vdropbox()

    if vdp.file_exists(parquet_path):
        df_history = vdp.read_parquet(parquet_path)

        df = pd.concat([df_new, df_history])
        df = deduplicate(df)
    else:
        df = df_new.copy()

    vdp.write_parquet(df, parquet_path)


def add_flow_name(df_in, flows):

    df = df_in.copy()

    flow_map = parse_prefect(flows, Flow)[COL_NAME].to_dict()
    df[COL_FLOW_NAME] = df[COL_FLOW_ID].map(flow_map)

    return df


@task(name="vtasks.vprefect.flow_runs")
def process_flow_runs():
    flows = asyncio.run(read_flows())
    flow_runs = asyncio.run(read_flow_runs())

    df_new = parse_prefect(flow_runs, FlowRun)

    # Retrive flow_name from flows
    df_new = add_flow_name(df_new, flows)

    update_parquet(df_new, PATH_FLOW_RUNS)


@task(name="vtasks.vprefect.task_runs")
def process_task_runs():
    task_runs = asyncio.run(read_task_runs())

    df_new = parse_prefect(task_runs, TaskRun)
    update_parquet(df_new, PATH_TASK_RUNS)


@flow(name="vtasks.vprefect")
def vprefect():
    process_flow_runs()
    process_task_runs()
