from prefect import flow
from prefect import states
from prefect import tags

from src.common.logs import get_logger
from src.jobs.backups.main import backup_all
from src.jobs.crypto.main import crypto
from src.jobs.dropbox.export_tables import export_dropbox_tables
from src.jobs.dropbox.money_lover import export_money_lover
from src.jobs.gcal.export import export_all_gcal
from src.jobs.gsheets.export_tables import export_gsheets_tables
from src.jobs.indexa.main import indexa_all
from src.vdbt.python.run import run_dbt

FLOW_NAME = "vtasks.hourly"

JOBS = {
    "maintain": [backup_all],
    "updates": [crypto, indexa_all],
    "export": [
        export_dropbox_tables,
        export_money_lover,
        export_all_gcal,
        export_gsheets_tables,
    ],
}


def is_failure(state):
    """Checks if it's a failed state"""
    return isinstance(state, states.State) and state.is_failed()


def run_flows(flows, flow_name):
    """Execute a list of flows, collect states, and fail if any flow fails."""

    logger = get_logger()

    results = {mflow.__name__: mflow(return_state=True) for mflow in flows}

    # Detect failed flows
    failures = [name for name, state in results.items() if is_failure(state)]

    if failures:
        msg = f"{flow_name} failed ({len(failures)} {failures=})."
        logger.error(msg)
        return states.Failed(message=msg)

    logger.info(f"{flow_name} completed successfully!")
    return states


@flow(name=f"{FLOW_NAME}.maintain")
def maintain():
    name = "maintain"
    return run_flows(JOBS[name], name)


@flow(name=f"{FLOW_NAME}.updates")
def updates():
    name = "updates"
    return run_flows(JOBS[name], name)


@flow(name=f"{FLOW_NAME}.export")
def export():
    name = "export"
    return run_flows(JOBS[name], name)


@flow(name=FLOW_NAME)
def hourly():
    with tags("env:pro"):
        flows = [maintain, updates, export, run_dbt]
        return run_flows(flows, "hourly")


if __name__ == "__main__":
    hourly()
