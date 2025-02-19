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


def run_tasks(tasks, flow_name):
    """Execute a list of tasks and determine if the flow should fail."""

    logger = get_logger()

    states = {task.__name__: task(return_state=True) for task in tasks}

    failed_tasks = {
        name: state for name, state in states.items() if isinstance(state, Failed)
    }

    if failed_tasks:
        logger.info(f"❌ {flow_name} failed ({len(failed_tasks)} tasks failed).")
        for name, state in failed_tasks.items():
            logger.info(f"  - Task failed: {name}")

        return Failed(
            message=f"{flow_name} failed due to {len(failed_tasks)} failed tasks."
        )

    logger.info(f"✅ {flow_name} completed successfully!")
    return states  # Return states in case we want to use them later


@flow(name="vtasks.hourly.maintain")
def maintain():
    tasks = [backup_all]
    return run_tasks(tasks, "maintain")


@flow(name="vtasks.hourly.updates")
def updates():
    tasks = [crypto, indexa_all]
    return run_tasks(tasks, "updates")


@flow(name="vtasks.hourly.export")
def export():
    tasks = [
        export_dropbox_tables,
        export_money_lover,
        export_all_gcal,
        export_gsheets_tables,
    ]
    return run_tasks(tasks, "export")


@flow(name="vtasks.hourly")
def hourly():
    with tags("env:pro"):
        # Run all subflows and collect their states
        state_maintain = maintain()
        state_updates = updates()
        state_export = export()
        state_dbt = run_tasks(
            [run_dbt], "run_dbt"
        )  # Single function still works with run_tasks

        # Check failures at the parent flow level
        return run_tasks(
            [
                lambda: state_maintain,
                lambda: state_updates,
                lambda: state_export,
                lambda: state_dbt,
            ],
            "hourly",
        )


if __name__ == "__main__":
    hourly()
