import asyncio

from datetime import datetime
from datetime import timedelta
from datetime import timezone

from prefect import flow
from prefect import states
from prefect import task
from prefect.client import get_client

import utils as u

from .models import FlowRun
from .query import parse_prefect
from .query import query_all_flow_runs

DATETIME_10_MIN_AGO = datetime.now(timezone.utc) - timedelta(minutes=10)


@task(name="aux.fix_status.get_uncompleted")
def get_uncompleted_flow_runs(
    env=u.detect_env(), state_names=["Running"], start_time_min=DATETIME_10_MIN_AGO
):

    log = u.get_log()

    log.info("Querying flow runs that are 'running'")
    flow_runs = asyncio.run(
        query_all_flow_runs(
            env=env, state_names=state_names, start_time_min=start_time_min, queries_per_batch=1
        )
    )

    if len(flow_runs) == 0:
        log.info(f"There are no uncompleted flow runs")
        return []

    df = parse_prefect(flow_runs, FlowRun)
    flow_runs_id = df[df["name"].str.startswith("vtasks")].index

    log.info(f"{len(flow_runs_id)} flow runs found")

    return flow_runs_id


@task(name="aux.fix_status.complete_uncompleted")
def update_flow_runs(flow_runs_id, state=states.Completed()):
    async def _update_flow_runs(flow_runs_id, state=states.Completed()):
        client = get_client()
        async with asyncio.TaskGroup() as tg:
            for flow_run_id in flow_runs_id:
                tg.create_task(client.set_flow_run_state(flow_run_id, state=state))

    asyncio.run(_update_flow_runs(flow_runs_id, state=state))


@flow(**u.get_prefect_args("aux.fix_status"))
def complete_uncompleted_flow_runs():
    update_flow_runs(get_uncompleted_flow_runs())
