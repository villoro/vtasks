from prefect import flow
from prefect import tags
from prefect.states import Failed

from src.common.logs import get_logger
from src.jobs.backups.main import backup_all
from src.jobs.crypto.main import crypto
from src.jobs.dropbox.export_tables import export_dropbox_tables
from src.jobs.dropbox.money_lover import export_money_lover
from src.jobs.gcal.export import export_all_gcal
from src.jobs.gsheets.export_tables import export_gsheets_tables
from src.jobs.indexa.main import indexa_all
from src.vdbt.python.run import run_dbt


def run_flows(flows, flow_name):
    """Execute a list of flows, collect states, and fail if any flow fails."""

    logger = get_logger()

    states = {flow.__name__: flow(return_state=True) for flow in flows}

    # Detect failed flows
    failures = [name for name, state in states.items() if isinstance(state, Failed)]

    if failures:
        msg = f"❌ {flow_name} failed ({len(failures)} flows failed, {failures=})."
        logger.error(msg)
        return Failed(message=msg)

    logger.info(f"✅ {flow_name} completed successfully!")
    return states


@flow(name="vtasks.hourly.maintain")
def maintain():
    flows = [backup_all]
    return run_flows(flows, "maintain")


@flow(name="vtasks.hourly.updates")
def updates():
    flows = [crypto, indexa_all]
    return run_flows(flows, "updates")


@flow(name="vtasks.hourly.export")
def export():
    flows = [
        export_dropbox_tables,
        export_money_lover,
        export_all_gcal,
        export_gsheets_tables,
    ]
    return run_flows(flows, "export")


@flow(name="vtasks.hourly")
def hourly():
    with tags("env:pro"):
        return run_flows([maintain, updates, export, run_dbt], "hourly")


if __name__ == "__main__":
    hourly()
