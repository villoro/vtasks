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
    """
    Prefect task that queries uncompleted flow runs.

    Args:
        env (str, optional): Environment tag. Defaults to auto-detected environment.
        state_names (list, optional): List of state names to include. Defaults to ["Running"].
        start_time_min (datetime, optional): Minimum start time for the query range. Defaults to 10 minutes ago.

    Returns:
        list: List of uncompleted flow run IDs.
    """
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

    log.info(f"Updating {len(flow_runs_id)}: {flow_runs_id=}")
    return flow_runs_id


# This should be a task but I haven't managed to make it work since I'm getting:
#    prefect.exceptions.MissingResult: State data is missing.
#    Typically, this occurs when result persistence is disabled and the state has been retrieved from the API.
# @task(name="aux.fix_status.complete_uncompleted")
def update_flow_runs(flow_runs_id, state=states.Completed()):
    """
    Asynchronously updates the state of flow runs.

    Args:
        flow_runs_id (list): List of flow run IDs to update.
        state (str, optional): New state for the flow runs. Defaults to 'Completed'.

    Returns:
        None
    """

    async def _update_flow_runs(flow_runs_id, state=states.Completed()):
        client = get_client()
        async with asyncio.TaskGroup() as tg:
            for flow_run_id in flow_runs_id:
                tg.create_task(client.set_flow_run_state(flow_run_id, state=state))

    asyncio.run(_update_flow_runs(flow_runs_id, state=state))


@flow(**u.get_prefect_args("aux.fix_status"))
def complete_uncompleted_flow_runs():
    """
    Prefect flow that completes uncompleted flow runs.

    Queries uncompleted flow runs and updates their state to 'Completed'

    Returns:
        None
    """
    log = u.get_log()

    flow_runs = get_uncompleted_flow_runs()
    update_flow_runs(flow_runs)

    log.info(f"All {len(flow_runs)} flows updated")
