from datetime import datetime

import asyncio
import pandas as pd

from prefect.client import get_client
from prefect import flow, task, get_run_logger

import utils as u

from .models import Flow, Task

COL_EXPORTED_AT = "exported_at"
COL_CREATED = "created"

PATH_VTASKS = "/Aplicaciones/vtasks"
PATH_FLOWS = f"{PATH_VTASKS}/flows.parquet"
PATH_TASKS = f"{PATH_VTASKS}/tasks.parquet"


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
    df = df.sort_values([COL_EXPORTED_AT, COL_CREATED]).copy()
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


@task(name="vtasks.vprefect.process_flows")
def process_flows():
    flows = asyncio.run(read_flow_runs())

    df_new = parse_prefect(flows, Flow)
    update_parquet(df_new, PATH_FLOWS)


@task(name="vtasks.vprefect.process_tasks")
def process_tasks():
    tasks = asyncio.run(read_task_runs())

    df_new = parse_prefect(tasks, Task)
    update_parquet(df_new, PATH_TASKS)


@flow(name="vtasks.vprefect")
def vprefect():
    process_flows()
    process_tasks()
